//! The rmf-pipes server binary: parses configuration, installs signal
//! handlers, and wires the bus, hub and HTTP layer together.

use anyhow::Context;
use clap::Parser;
use tokio_util::sync::CancellationToken;

mod config;

use config::{Config, LogFormat};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    init_logging(config.log_format);

    let build = pipes_bus::distro::BUILD_INFO;
    tracing::info!(
        version = build.version,
        ros_distro = build.ros_distro,
        "starting rmf-pipes"
    );

    // Install signal handlers before anything is announced, so a SIGTERM sent
    // as soon as `listening` appears is always handled gracefully.
    let shutdown = CancellationToken::new();
    let signals = Signals::install().context("installing signal handlers")?;
    tokio::spawn(signals.cancel_on_signal(shutdown.clone()));
    tracing::info!("signal handlers installed");

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    let addr = listener.local_addr()?;
    tracing::info!(addr = %addr, "listening");

    let app = pipes_api::router(pipes_api::AppState::new(build));
    let outcome = pipes_api::serve::serve(listener, app, shutdown, config.shutdown_drain).await?;
    tracing::info!(?outcome, "stopped");
    Ok(())
}

fn init_logging(format: LogFormat) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Pretty => builder.init(),
        LogFormat::Json => builder.json().flatten_event(true).init(),
    }
}

/// SIGINT/SIGTERM listeners, registered eagerly by [`Signals::install`].
struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl Signals {
    fn install() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    /// Cancel `token` on the first SIGINT or SIGTERM.
    async fn cancel_on_signal(mut self, token: CancellationToken) {
        tokio::select! {
            _ = self.interrupt.recv() => tracing::info!("SIGINT received"),
            _ = self.terminate.recv() => tracing::info!("SIGTERM received"),
        }
        token.cancel();
    }
}
