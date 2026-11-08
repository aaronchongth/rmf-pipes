//! Liveness endpoint.
//!
//! Rule carried over from rmf-console: health responses never include raw error
//! text from dependencies. Details go to the log; the response stays generic.

use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;

use crate::AppState;

/// Body of `GET /healthz`.
#[derive(Debug, Serialize)]
pub struct Health {
    /// Always `"ok"` while the process can serve HTTP.
    pub status: &'static str,
    /// rmf-pipes version.
    pub version: &'static str,
    /// ROS 2 distro this build speaks.
    pub ros_distro: &'static str,
}

/// Routes for liveness checks.
pub fn routes() -> Router<AppState> {
    Router::new().route("/healthz", get(healthz))
}

async fn healthz(State(state): State<AppState>) -> Json<Health> {
    let build = state.build();
    Json(Health {
        status: "ok",
        version: build.version,
        ros_distro: build.ros_distro,
    })
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use pipes_core::build_info::BuildInfo;
    use tower::ServiceExt;

    use crate::{AppState, router};

    #[tokio::test]
    async fn healthz_reports_version_and_distro() {
        let app = router(AppState::new(BuildInfo::new("testdistro")));
        let res = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["ros_distro"], "testdistro");
        assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
    }
}
