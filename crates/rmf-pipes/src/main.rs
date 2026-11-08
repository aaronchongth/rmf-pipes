//! The rmf-pipes server binary: parses configuration, installs signal
//! handlers, and wires the bus, hub and HTTP layer together.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio_util::sync::CancellationToken;

mod config;
mod users;

use config::{Command, Config, LogFormat};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    if let Some(Command::User { action }) = &config.command {
        // CLI output is for humans; keep logs to warnings.
        init_logging_level(config.log_format, "warn");
        return users::run(&config, action).await;
    }
    init_logging(config.log_format);

    let writes = config.writes().map_err(|e| anyhow::anyhow!(e))?;
    for rule in writes.publish.iter().chain(&writes.call) {
        if rule.matches_everything() {
            tracing::warn!(
                pattern = %rule.pattern,
                role = %rule.role,
                "a write rule matches every topic or service (including every node's parameter services)"
            );
        }
    }

    // Refuse an open, unauthenticated API on the network unless asked to.
    if config.database_url.is_none()
        && !config.listen.ip().is_loopback()
        && !config.insecure_no_auth
    {
        anyhow::bail!(
            "refusing to serve without authentication on {}: set --database-url, listen on a loopback \
             address, or pass --insecure-no-auth",
            config.listen
        );
    }

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

    let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .context("installing the metrics recorder")?;

    // Join zenoh before listening, so `listening` means ready. Opening the
    // session blocks, so keep it off the async workers. In client mode it fails
    // fast when no router is reachable.
    let bus_config = config.bus();
    tracing::info!(
        connect = ?bus_config.connect,
        mode = %bus_config.mode,
        domain_id = bus_config.domain_id,
        "joining zenoh"
    );
    let bus = tokio::task::spawn_blocking(move || pipes_bus::HirozBus::connect(bus_config))
        .await
        .context("joining zenoh")??;
    let auth = match &config.database_url {
        Some(url) => Some(connect_auth(&config, url).await?),
        None => {
            tracing::warn!(
                "no --database-url: authentication is disabled and the API is read-only"
            );
            None
        }
    };
    let hub = pipes_core::hub::Hub::new(Arc::new(bus), config.hub());
    let hub_task = hub.spawn(shutdown.clone());
    tracing::info!(
        patterns = ?config.ingest.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "hub running"
    );

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    let addr = listener.local_addr()?;
    tracing::info!(addr = %addr, "listening");

    let mut state = pipes_api::AppState::new(
        build,
        hub,
        Arc::new(move || metrics.render()),
        shutdown.clone(),
    )
    .with_max_streams(config.max_streams)
    .with_writes(writes);
    if config.database_url.is_none()
        && !(config.allow_publish.is_empty() && config.allow_call.is_empty())
    {
        tracing::warn!(
            "--allow-publish/--allow-call need --database-url: without authentication nothing can be written"
        );
    }
    if let Some(auth) = auth {
        tokio::spawn(purge_sessions(auth.db.clone(), shutdown.clone()));
        state = state.with_auth(auth);
    }
    let outcome = pipes_api::serve::serve(
        listener,
        pipes_api::router(state),
        shutdown.clone(),
        config.shutdown_drain,
    )
    .await?;
    shutdown.cancel();
    if tokio::time::timeout(config.shutdown_drain, hub_task)
        .await
        .is_err()
    {
        tracing::warn!("hub did not stop within the drain deadline");
    }
    tracing::info!(?outcome, "stopped");
    Ok(())
}

/// Connect to Postgres (applying migrations), bootstrap the first admin if
/// asked, and build the auth service.
async fn connect_auth(config: &Config, url: &str) -> anyhow::Result<pipes_api::auth::AuthService> {
    let db = pipes_db::Db::connect(url, 10, config.database_connect_timeout).await?;
    tracing::info!("database connected; authentication is enabled");
    let users = db.count_users().await?;
    match (&config.bootstrap_admin, users) {
        (Some(name), 0) => {
            let password = config.bootstrap_password.as_deref().context(
                "--bootstrap-admin needs --bootstrap-password (or RMF_PIPES_BOOTSTRAP_PASSWORD)",
            )?;
            db.create_user(name, password, pipes_db::Role::Admin)
                .await?;
            db.record_audit(&users::cli_audit(
                "user.create",
                name,
                serde_json::json!({"role": "admin", "bootstrap": true}),
            ))
            .await?;
            tracing::info!(username = %name, "created the bootstrap admin");
        }
        (Some(_), _) => tracing::debug!("users exist; --bootstrap-admin ignored"),
        (None, 0) => tracing::warn!(
            "there are no users: create one with `rmf-pipes user add <name> --role admin`, or start with --bootstrap-admin"
        ),
        (None, _) => {}
    }
    Ok(pipes_api::auth::AuthService::new(db, config.auth()))
}

/// Delete expired sessions hourly.
async fn purge_sessions(db: pipes_db::Db, shutdown: CancellationToken) {
    let mut every = tokio::time::interval(std::time::Duration::from_secs(3600));
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = every.tick() => match db.purge_expired_sessions().await {
                Ok(n) if n > 0 => tracing::debug!(n, "purged expired sessions"),
                Ok(_) => {}
                Err(err) => tracing::warn!(%err, "cannot purge expired sessions"),
            },
        }
    }
}

fn init_logging(format: LogFormat) {
    init_logging_level(format, "info");
}

fn init_logging_level(format: LogFormat, default: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
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
