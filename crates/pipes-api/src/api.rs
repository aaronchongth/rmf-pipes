//! The REST and SSE API under `/api/v1`, documented with utoipa. Every route
//! is registered through [`OpenApiRouter`], so a route cannot exist without
//! appearing in `/openapi.json`.

use std::{convert::Infallible, time::Duration};

use axum::{
    Json,
    extract::{Query, State},
    response::sse::{Event, KeepAlive, Sse},
};
use futures::Stream;
use pipes_core::{
    graph::{Node, ServiceSummary, TopicSummary},
    hub::{TopicEvent, TopicStatus, TopicView, Watch, validate_topic},
    sample::Sample,
    schema::Schema,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    AppState,
    error::{ApiError, Problem},
};

/// Longest `wait_ms` a request may ask for.
pub const MAX_WAIT: Duration = Duration::from_secs(10);
const DEFAULT_WAIT_MS: u64 = 2000;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "rmf-pipes",
        description = "Live, read-only view of a ROS 2 system over zenoh. Message payloads follow \
                       the JSON mapping in docs/json-mapping.md (64-bit integers are strings, \
                       byte arrays are base64)."
    ),
    tags(
        (name = "graph", description = "The ROS graph: nodes, topics, services, type mismatches"),
        (name = "topics", description = "Subscriptions, latest samples, schemas and live streams"),
        (name = "ops", description = "Health and metrics"),
    )
)]
#[openapi(components(schemas(Lagged, TopicStatus)))]
struct ApiDoc;

/// The documented routes and their OpenAPI description.
pub fn router() -> (axum::Router<AppState>, utoipa::openapi::OpenApi) {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(crate::health::healthz))
        .routes(routes!(crate::metrics::metrics))
        .routes(routes!(graph))
        .routes(routes!(topics))
        .routes(routes!(topic))
        .routes(routes!(latest))
        .routes(routes!(schema))
        .routes(routes!(stream))
        .split_for_parts()
}

/// The ROS graph as of the last hub tick.
#[derive(Debug, Serialize, ToSchema)]
pub struct GraphView {
    /// Nodes other than rmf-pipes itself.
    pub nodes: Vec<Node>,
    /// Every topic with at least one endpoint.
    pub topics: Vec<TopicSummary>,
    /// Every service with at least one server or client.
    pub services: Vec<ServiceSummary>,
    /// Names of topics whose endpoints disagree on the message type or hash.
    pub mismatches: Vec<String>,
}

/// Everything known about one topic.
#[derive(Debug, Serialize, ToSchema)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// The topic in the graph, if it has endpoints.
    pub graph: Option<TopicSummary>,
    /// rmf-pipes' subscription to it, if any.
    pub subscription: Option<TopicView>,
}

/// Selects a topic.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TopicQuery {
    /// Fully qualified topic name, e.g. `/fleet/robot1/state`.
    pub name: String,
}

/// Selects a topic and how long to wait for it.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct WaitQuery {
    /// Fully qualified topic name, e.g. `/fleet/robot1/state`.
    pub name: String,
    /// If there is no sample (or schema) yet, how long to wait for one, in
    /// milliseconds (default 2000, at most 10000).
    pub wait_ms: Option<u64>,
}

impl WaitQuery {
    fn wait(&self) -> Duration {
        Duration::from_millis(self.wait_ms.unwrap_or(DEFAULT_WAIT_MS)).min(MAX_WAIT)
    }
}

/// The ROS graph.
#[utoipa::path(get, path = "/api/v1/graph", tag = "graph",
    responses((status = 200, description = "The graph as of the last hub tick", body = GraphView)))]
pub async fn graph(State(state): State<AppState>) -> Json<GraphView> {
    let g = state.hub().graph();
    let topics = g.topics();
    let mismatches = topics
        .iter()
        .filter(|t| t.mismatch)
        .map(|t| t.name.clone())
        .collect();
    Json(GraphView {
        nodes: g.nodes.clone(),
        services: g.services(),
        topics,
        mismatches,
    })
}

/// Topics rmf-pipes is subscribed to (standing, watched, opening or failed).
#[utoipa::path(get, path = "/api/v1/topics", tag = "topics",
    responses((status = 200, description = "Subscriptions, sorted by topic name", body = Vec<TopicView>)))]
pub async fn topics(State(state): State<AppState>) -> Json<Vec<TopicView>> {
    Json(state.hub().topics())
}

/// One topic: its graph summary and subscription state.
#[utoipa::path(get, path = "/api/v1/topic", tag = "topics", params(TopicQuery),
    responses(
        (status = 200, description = "What is known about the topic", body = TopicInfo),
        (status = 400, description = "Invalid topic name or query (`invalid_topic`, `invalid_query`)", body = ApiError),
        (status = 404, description = "The topic has no endpoints and no subscription (`unknown_topic`)", body = ApiError),
    ))]
pub async fn topic(
    State(state): State<AppState>,
    ApiQuery(q): ApiQuery<TopicQuery>,
) -> Result<Json<TopicInfo>, Problem> {
    validate_topic(&q.name)?;
    let graph = state.hub().graph().topic(&q.name);
    let subscription = state.hub().topic(&q.name);
    if graph.is_none() && subscription.is_none() {
        return Err(Problem::not_found(
            "unknown_topic",
            format!("`{}` has no endpoints", q.name),
        ));
    }
    Ok(Json(TopicInfo {
        name: q.name,
        graph,
        subscription,
    }))
}

/// The latest sample on a topic, subscribing on demand.
///
/// If no sample has arrived yet, rmf-pipes subscribes (if needed) and waits up
/// to `wait_ms` for one. The subscription then stays warm for the idle
/// timeout, so polling this endpoint does not resubscribe every time.
#[utoipa::path(get, path = "/api/v1/topic/latest", tag = "topics", params(WaitQuery),
    responses(
        (status = 200, description = "The most recent sample", body = Sample),
        (status = 400, description = "Invalid topic name or query (`invalid_topic`, `invalid_query`)", body = ApiError),
        (status = 404, description = "Nobody publishes the topic (`no_publisher`)", body = ApiError),
        (status = 503, description = "The topic cannot be served: `topic_failed` (e.g. no type description), `too_many_topics`, `closed`, `shutting_down`", body = ApiError),
        (status = 504, description = "No sample arrived within `wait_ms` (`no_sample`)", body = ApiError),
    ))]
pub async fn latest(
    State(state): State<AppState>,
    ApiQuery(q): ApiQuery<WaitQuery>,
) -> Result<Json<Sample>, Problem> {
    validate_topic(&q.name)?;
    if let Some(sample) = state.hub().latest(&q.name) {
        return Ok(Json((*sample).clone()));
    }
    let mut watch = watch_published(&state, &q.name)?;
    if let Some(s) = watch.initial_latest.clone() {
        return Ok(Json((*s).clone()));
    }
    if let TopicStatus::Failed { error } = &watch.initial_status {
        return Err(failed_problem(&q.name, error));
    }
    let wait = q.wait();
    let sample = tokio::time::timeout(wait, async {
        loop {
            match watch.events.recv().await {
                Ok(TopicEvent::Sample(s)) => return Ok(s),
                Ok(TopicEvent::Status(TopicStatus::Failed { error })) => {
                    return Err(failed_problem(&q.name, &error));
                }
                Ok(TopicEvent::Status(_)) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return Err(closed_problem()),
            }
        }
    })
    .await
    .map_err(|_| {
        Problem::timeout(
            "no_sample",
            format!("no sample on `{}` within {} ms", q.name, wait.as_millis()),
        )
    })??;
    Ok(Json((*sample).clone()))
}

/// A topic's message schema, subscribing on demand.
#[utoipa::path(get, path = "/api/v1/topic/schema", tag = "topics", params(WaitQuery),
    responses(
        (status = 200, description = "The message definition, as reported by the publisher", body = Schema),
        (status = 400, description = "Invalid topic name or query (`invalid_topic`, `invalid_query`)", body = ApiError),
        (status = 404, description = "Nobody publishes the topic (`no_publisher`)", body = ApiError),
        (status = 503, description = "The schema could not be discovered: `topic_failed`, `too_many_topics`, `closed`, `shutting_down`", body = ApiError),
        (status = 504, description = "Discovery did not finish within `wait_ms` (`no_schema`)", body = ApiError),
    ))]
pub async fn schema(
    State(state): State<AppState>,
    ApiQuery(q): ApiQuery<WaitQuery>,
) -> Result<Json<Schema>, Problem> {
    validate_topic(&q.name)?;
    if let Some(s) = state.hub().schema(&q.name) {
        return Ok(Json(s));
    }
    let mut watch = watch_published(&state, &q.name)?;
    let wait = q.wait();
    let hub = state.hub().clone();
    let name = q.name.clone();
    let mut status = watch.initial_status.clone();
    tokio::time::timeout(wait, async {
        loop {
            if let Some(s) = hub.schema(&name) {
                return Ok(s);
            }
            if let TopicStatus::Failed { error } = &status {
                return Err(failed_problem(&name, error));
            }
            match watch.events.recv().await {
                Ok(TopicEvent::Status(st)) => status = st,
                Ok(TopicEvent::Sample(_)) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return Err(closed_problem()),
            }
        }
    })
    .await
    .map_err(|_| {
        Problem::timeout(
            "no_schema",
            format!(
                "schema of `{}` not discovered within {} ms",
                q.name,
                wait.as_millis()
            ),
        )
    })?
    .map(Json)
}

/// Payload of the SSE `lagged` event.
#[derive(Debug, Serialize, ToSchema)]
pub struct Lagged {
    /// How many events this client missed.
    pub skipped: u64,
}

/// Live samples as server-sent events, subscribing on demand.
///
/// Events, each with a JSON `data` line:
/// - `status`: a `TopicStatus`. Sent first, and on every change.
/// - `sample`: a `Sample`. The latest one is sent right after the first
///   `status` if there is one.
/// - `lagged`: a `Lagged`, when this client fell behind and missed events.
///
/// The stream ends after a final `status` of `{"state": "closed"}`, when the
/// topic is closed or the server shuts down. Watching a topic that nobody
/// publishes yet is allowed: the status reports why, and delivery starts once
/// a publisher appears (after the retry backoff, which grows up to a minute).
/// Concurrent streams are capped (`503 too_many_streams`).
#[utoipa::path(get, path = "/api/v1/topic/stream", tag = "topics", params(TopicQuery),
    responses(
        (status = 200, description = "An SSE stream of `status` (TopicStatus), `sample` (Sample) and `lagged` (Lagged) events", content_type = "text/event-stream", body = String),
        (status = 400, description = "Invalid topic name or query (`invalid_topic`, `invalid_query`)", body = ApiError),
        (status = 503, description = "`too_many_streams`, `too_many_topics` or `shutting_down`", body = ApiError),
    ))]
pub async fn stream(
    State(state): State<AppState>,
    ApiQuery(q): ApiQuery<TopicQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, Problem> {
    validate_topic(&q.name)?;
    let permit = state.stream_permit().ok_or_else(|| {
        Problem::unavailable(
            "too_many_streams",
            "too many live streams are open; try again later",
        )
    })?;
    let watch = state.hub().watch(&q.name)?;
    let shutdown = state.shutdown().clone();
    let mut initial = vec![json_event("status", &watch.initial_status)];
    if let Some(s) = &watch.initial_latest {
        initial.push(json_event("sample", &**s));
    }
    // State: the watch (None once the final event is sent), the permit, the
    // shutdown token.
    let live = futures::stream::unfold(
        (Some(watch), permit, shutdown),
        |(watch, permit, shutdown)| async move {
            let mut watch = watch?;
            let ev = tokio::select! {
                () = shutdown.cancelled() => {
                    return Some((Ok(json_event("status", &TopicStatus::Closed)), (None, permit, shutdown)));
                }
                ev = watch.events.recv() => ev,
            };
            let event = match ev {
                Ok(TopicEvent::Sample(s)) => json_event("sample", &*s),
                Ok(TopicEvent::Status(TopicStatus::Closed)) => {
                    return Some((
                        Ok(json_event("status", &TopicStatus::Closed)),
                        (None, permit, shutdown),
                    ));
                }
                Ok(TopicEvent::Status(st)) => json_event("status", &st),
                Err(RecvError::Lagged(n)) => json_event("lagged", &Lagged { skipped: n }),
                Err(RecvError::Closed) => {
                    return Some((
                        Ok(json_event("status", &TopicStatus::Closed)),
                        (None, permit, shutdown),
                    ));
                }
            };
            Some((Ok(event), (Some(watch), permit, shutdown)))
        },
    );
    let stream =
        futures::StreamExt::chain(futures::stream::iter(initial.into_iter().map(Ok)), live);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(KEEP_ALIVE)))
}

/// SSE keep-alive comment interval.
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);

fn json_event<T: Serialize + ?Sized>(name: &str, value: &T) -> Event {
    Event::default()
        .event(name)
        .json_data(value)
        .unwrap_or_else(|_| Event::default().event(name).data("null"))
}

/// Watch a topic that has publishers, or explain why not. Topics with no
/// publisher in the graph are 404 even if some client already watches them.
fn watch_published(state: &AppState, name: &str) -> Result<Watch, Problem> {
    if state.hub().graph().publishers_of(name).next().is_none() {
        return Err(Problem::not_found(
            "no_publisher",
            format!("nobody publishes `{name}`"),
        ));
    }
    Ok(state.hub().watch(name)?)
}

fn failed_problem(name: &str, error: &str) -> Problem {
    Problem::unavailable("topic_failed", format!("cannot serve `{name}`: {error}"))
}

fn closed_problem() -> Problem {
    Problem::unavailable("closed", "the topic was closed")
}

/// A query string extractor whose rejections are JSON [`Problem`]s
/// (`400 invalid_query`) like every other API error.
#[derive(Debug)]
pub struct ApiQuery<T>(pub T);

impl<S, T> axum::extract::FromRequestParts<S> for ApiQuery<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Problem;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(q)| Self(q))
            .map_err(|e| Problem::bad_request("invalid_query", e.body_text()))
    }
}
