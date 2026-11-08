//! The introspection dashboard: server-rendered HTML (askama) refreshed with
//! vendored htmx, plus a few lines of inline JS for the live SSE view. No Node
//! toolchain and no external URLs, so it works air-gapped.
//!
//! It exists only to verify that the data layer works: no operational features.

use askama::Template;
use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use pipes_core::{
    graph::{Endpoint, TopicSummary},
    hub::{TopicStatus, TopicView},
};

use crate::{
    AppState,
    api::{ApiQuery, TopicQuery},
    auth::Caller,
    error::Problem,
};

/// Vendored htmx build (0BSD; see `assets/htmx-LICENSE`).
const HTMX: &[u8] = include_bytes!("../assets/htmx-2.0.11.min.js");
/// Path the templates load htmx from. The version is in the name, so it can
/// be cached forever.
pub const HTMX_PATH: &str = "/ui/assets/htmx-2.0.11.min.js";

/// Dashboard routes. Not part of the OpenAPI description.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/", get(index))
        .route("/login", get(login_page))
        .route("/ui/topic", get(topic_page))
        .route("/ui/fragments/overview", get(overview))
        .route("/ui/fragments/topic", get(topic_info))
        .route(HTMX_PATH, get(htmx))
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexPage {
    version: &'static str,
    ros_distro: &'static str,
    user: Option<String>,
}

#[derive(Template)]
#[template(path = "topic.html")]
struct TopicPage {
    version: &'static str,
    ros_distro: &'static str,
    user: Option<String>,
    name: String,
}

#[derive(Template)]
#[template(path = "login.html")]
struct LoginPage {
    version: &'static str,
    ros_distro: &'static str,
    user: Option<String>,
    next: String,
}

/// `?next=` on the login page.
#[derive(Debug, serde::Deserialize)]
struct LoginQuery {
    next: Option<String>,
}

#[derive(Template)]
#[template(path = "overview.html")]
struct Overview {
    nodes: usize,
    services: usize,
    active: usize,
    mismatches: usize,
    topics: Vec<Row>,
}

struct Row {
    name: String,
    /// Whether `name` is a valid topic name we can link to.
    linkable: bool,
    types: Vec<RowType>,
    mismatch: bool,
    publishers: usize,
    subscriptions: usize,
    state: String,
}

struct RowType {
    type_name: String,
    short_hash: String,
}

#[derive(Template)]
#[template(path = "topic_info.html")]
struct TopicInfoFragment {
    graph: Option<TopicSummary>,
    publishers: Vec<PublisherRow>,
    subscription: Option<TopicView>,
}

struct PublisherRow {
    node: String,
    qos: String,
}

fn render(t: &impl Template) -> Response {
    match t.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            tracing::error!(%err, "template rendering failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "template error").into_response()
        }
    }
}

async fn index(State(state): State<AppState>, Caller(caller): Caller) -> Response {
    let b = state.build();
    render(&IndexPage {
        version: b.version,
        ros_distro: b.ros_distro,
        user: caller.user().map(|u| u.username.clone()),
    })
}

async fn login_page(State(state): State<AppState>, ApiQuery(q): ApiQuery<LoginQuery>) -> Response {
    if state.auth().is_none() {
        // Nothing to log in to.
        return axum::response::Redirect::to("/").into_response();
    }
    let b = state.build();
    render(&LoginPage {
        version: b.version,
        ros_distro: b.ros_distro,
        user: None,
        next: safe_next(q.next.as_deref()),
    })
}

async fn topic_page(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiQuery(q): ApiQuery<TopicQuery>,
) -> Result<Response, Problem> {
    pipes_core::hub::validate_topic(&q.name)?;
    let b = state.build();
    Ok(render(&TopicPage {
        version: b.version,
        ros_distro: b.ros_distro,
        user: caller.user().map(|u| u.username.clone()),
        name: q.name,
    }))
}

async fn overview(State(state): State<AppState>) -> Response {
    let hub = state.hub();
    let graph = hub.graph();
    let subs = hub.topics();
    let summaries = graph.topics();
    let state_of = |name: &str| -> String {
        match subs.iter().find(|s| s.name == name) {
            None => "—".into(),
            Some(s) => {
                let base = match &s.status {
                    TopicStatus::Opening => "opening".to_owned(),
                    TopicStatus::Active => format!("active · {} received", s.stats.received),
                    TopicStatus::Failed { .. } => "failed".to_owned(),
                    TopicStatus::Closed => "closed".to_owned(),
                };
                if s.standing {
                    format!("{base} · standing")
                } else {
                    base
                }
            }
        }
    };
    let rows = summaries
        .iter()
        .map(|t| Row {
            name: t.name.clone(),
            linkable: pipes_core::hub::validate_topic(&t.name).is_ok(),
            types: t
                .types
                .iter()
                .map(|ty| RowType {
                    type_name: ty.type_name.clone(),
                    short_hash: ty.type_hash.chars().take(15).collect(),
                })
                .collect(),
            mismatch: t.mismatch,
            publishers: t.publishers,
            subscriptions: t.subscriptions,
            state: state_of(&t.name),
        })
        .collect();
    render(&Overview {
        nodes: graph.nodes.len(),
        services: graph.services().len(),
        active: subs
            .iter()
            .filter(|s| s.status == TopicStatus::Active)
            .count(),
        mismatches: summaries.iter().filter(|t| t.mismatch).count(),
        topics: rows,
    })
}

async fn topic_info(State(state): State<AppState>, ApiQuery(q): ApiQuery<TopicQuery>) -> Response {
    let hub = state.hub();
    let graph = hub.graph();
    let publishers = graph.publishers_of(&q.name).map(publisher_row).collect();
    render(&TopicInfoFragment {
        graph: graph.topic(&q.name),
        publishers,
        subscription: hub.topic(&q.name),
    })
}

/// Where to go after login: only a local absolute path. Anything that a
/// browser could resolve off-site (`//host`, `/\\host`, control characters it
/// strips, a scheme) becomes `/`.
pub fn safe_next(next: Option<&str>) -> String {
    let Some(n) = next else { return "/".into() };
    let local = n.starts_with('/')
        && !n.starts_with("//")
        && !n.contains('\\')
        && !n.chars().any(|c| c.is_control() || c.is_whitespace())
        && n.len() <= 2048;
    if local { n.to_owned() } else { "/".into() }
}

fn publisher_row(ep: &Endpoint) -> PublisherRow {
    let q = ep.qos;
    let depth = q
        .depth
        .map_or("keep all".to_owned(), |d| format!("depth {d}"));
    PublisherRow {
        node: ep.node.clone().unwrap_or_else(|| "(unknown node)".into()),
        qos: format!(
            "{}, {}, {depth}",
            if q.reliable {
                "reliable"
            } else {
                "best effort"
            },
            if q.transient_local {
                "transient local"
            } else {
                "volatile"
            },
        ),
    }
}

async fn htmx() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        HTMX,
    )
}
