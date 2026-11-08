//! Serving with graceful shutdown and a hard drain deadline.
//!
//! Graceful shutdown alone waits for every in-flight request. Long-lived
//! responses (SSE streams, a stalled client) can keep that wait going forever,
//! which is what hung rmf-console's F01. Here, once shutdown is requested, the
//! server gets `drain` to finish; after that `serve` returns regardless.
//!
//! "Abandoned" means `serve` stops waiting: axum's per-connection tasks keep
//! running until the tokio runtime is dropped, which in the binary happens right
//! after `serve` returns.

use std::{io, time::Duration};

use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// How `serve` ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeOutcome {
    /// All connections finished within the drain deadline.
    Drained,
    /// The drain deadline passed with connections still open; they were abandoned.
    DeadlineExceeded,
}

/// Serve `app` on `listener` until `shutdown` is cancelled, then drain for at
/// most `drain`.
///
/// Handlers that stream (SSE) should also watch `shutdown` and end their
/// streams, so that the common case is `Drained`.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
    drain: Duration,
) -> io::Result<ServeOutcome> {
    let graceful =
        axum::serve(listener, app).with_graceful_shutdown(shutdown.clone().cancelled_owned());
    let deadline = async {
        shutdown.cancelled().await;
        tokio::time::sleep(drain).await;
    };
    tokio::select! {
        res = graceful => res.map(|()| ServeOutcome::Drained),
        () = deadline => {
            tracing::warn!(drain_secs = drain.as_secs_f64(), "drain deadline exceeded; abandoning open connections");
            Ok(ServeOutcome::DeadlineExceeded)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{Router, routing::get};
    use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot};
    use tokio_util::sync::CancellationToken;

    use super::{ServeOutcome, serve};

    /// A handler that never completes, standing in for an SSE stream that
    /// ignores shutdown or a client that stops reading.
    fn stalling_app(entered: oneshot::Sender<()>) -> Router {
        let entered = std::sync::Arc::new(std::sync::Mutex::new(Some(entered)));
        Router::new().route(
            "/stall",
            get(move || {
                if let Some(tx) = entered.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                std::future::pending::<&'static str>()
            }),
        )
    }

    #[tokio::test]
    async fn stalled_request_is_abandoned_at_the_drain_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (entered_tx, entered_rx) = oneshot::channel();
        let shutdown = CancellationToken::new();
        let drain = Duration::from_secs(1);
        let server = tokio::spawn(serve(
            listener,
            stalling_app(entered_tx),
            shutdown.clone(),
            drain,
        ));

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /stall HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap();
        // Only shut down once the request is provably in flight; otherwise the
        // test could pass vacuously with an idle server.
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .expect("handler was never entered")
            .unwrap();
        // The deadline must count from shutdown, not from server start: with no
        // shutdown requested, the server keeps serving well past `drain`.
        tokio::time::sleep(drain * 2).await;
        assert!(
            !server.is_finished(),
            "serve returned without a shutdown request"
        );

        let started = tokio::time::Instant::now();
        shutdown.cancel();
        let outcome = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("serve hung past the drain deadline")
            .unwrap()
            .unwrap();
        let elapsed = started.elapsed();

        assert_eq!(outcome, ServeOutcome::DeadlineExceeded);
        assert!(
            elapsed >= drain,
            "returned before the deadline: {elapsed:?}"
        );
        assert!(
            elapsed < drain + Duration::from_millis(500),
            "took too long: {elapsed:?}"
        );
        drop(client);
    }

    #[tokio::test]
    async fn idle_server_drains_immediately() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(serve(
            listener,
            Router::new().route("/", get(|| async { "hi" })),
            shutdown.clone(),
            Duration::from_secs(30),
        ));
        shutdown.cancel();
        let outcome = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("idle server did not drain promptly")
            .unwrap()
            .unwrap();
        assert_eq!(outcome, ServeOutcome::Drained);
    }
}
