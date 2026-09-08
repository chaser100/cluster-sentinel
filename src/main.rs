//! ClusterSentinel binary entrypoint.

use std::env;
use std::sync::Arc;

use anyhow::{Context, Result};
use clustersentinel::config::Config;
use clustersentinel::events::{
    EventRegistry, EventStoreHandle, StorageBackend, StorageHealthHandle, WatchStateHandle,
    run_event_pipeline, spawn_durable_writer,
};
use clustersentinel::http::{HttpState, router};
use clustersentinel::mcp::{AppState, SentinelMcp};
use clustersentinel::metrics::Metrics;
use rmcp::ServiceExt;
use rmcp::transport::stdio;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    install_rustls_crypto_provider()?;

    let config = Config::from_env().context("failed to load configuration")?;
    let metrics = Metrics::try_new().context("failed to initialize metrics")?;
    let registry = EventRegistry::new(config.registry_capacity, config.dedup_ttl);
    let watch_state = WatchStateHandle::new();
    let storage_backend = if config.storage_path.is_some() {
        StorageBackend::Sqlite
    } else {
        StorageBackend::Memory
    };
    let storage_health = StorageHealthHandle::new(storage_backend);
    let cancel = CancellationToken::new();

    let store = match &config.storage_path {
        Some(path) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create storage directory {}", parent.display())
                })?;
            }
            match EventStoreHandle::open(path, config.cluster_id.clone()) {
                Ok(store) => {
                    storage_health.mark_ready();
                    metrics.observe_storage_op("open", 0.0, true);
                    if let Ok(stats) = store.stats().await {
                        metrics.set_storage_stats(stats.rows, stats.bytes);
                    }
                    store
                }
                Err(err) => {
                    metrics.observe_storage_op("open", 0.0, false);
                    storage_health.note_error(&err.to_string());
                    return Err(err).with_context(|| {
                        format!("failed to open durable store at {}", path.display())
                    });
                }
            }
        }
        None => match EventStoreHandle::open_in_memory(config.cluster_id.clone()) {
            Ok(store) => {
                storage_health.mark_ready();
                metrics.observe_storage_op("open", 0.0, true);
                store
            }
            Err(err) => {
                metrics.observe_storage_op("open", 0.0, false);
                storage_health.note_error(&err.to_string());
                return Err(err).context("failed to open in-memory durable store");
            }
        },
    };
    let (writer, writer_task) = spawn_durable_writer(
        store.shared(),
        config.writer_queue_capacity,
        cancel.child_token(),
        metrics.clone(),
        storage_health.clone(),
    );

    if env::args().any(|arg| arg == "--mcp-stdio") {
        info!("starting MCP stdio transport (Bearer auth not required on stdio)");
        let server = SentinelMcp::new(AppState {
            registry,
            store,
            metrics,
            watch_state,
            storage_health,
            events_mode: config.events_mode,
            configured_namespaces: config.namespaces.clone(),
            build_version: env::var("CLUSTERSENTINEL_BUILD_VERSION")
                .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_owned()),
            git_sha: env::var("CLUSTERSENTINEL_GIT_SHA").unwrap_or_else(|_| "unknown".to_owned()),
        });
        let service = server
            .serve(stdio())
            .await
            .context("MCP stdio serve failed")?;
        service
            .waiting()
            .await
            .context("MCP stdio session failed")?;
        cancel.cancel();
        let _ = writer_task.await;
        return Ok(());
    }

    let mcp_auth_token = config
        .require_mcp_auth_token()
        .context("HTTP mode requires MCP Bearer token")?;
    let mcp_auth_token: Arc<str> = Arc::from(mcp_auth_token);

    let build_version = env::var("CLUSTERSENTINEL_BUILD_VERSION")
        .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_owned());
    let git_sha = env::var("CLUSTERSENTINEL_GIT_SHA").unwrap_or_else(|_| "unknown".to_owned());

    let pipeline_cancel = cancel.child_token();
    let pipeline_registry = registry.clone();
    let pipeline_metrics = metrics.clone();
    let pipeline_watch = watch_state.clone();
    let pipeline_config = config.clone();
    let pipeline_writer = writer.clone();
    let pipeline_task = tokio::spawn(async move {
        run_event_pipeline(
            pipeline_config,
            pipeline_registry,
            pipeline_writer,
            pipeline_metrics,
            pipeline_watch,
            pipeline_cancel,
        )
        .await;
    });

    let app = router(
        HttpState {
            registry,
            store,
            metrics,
            watch_state,
            storage_health,
            events_mode: config.events_mode,
            mcp_allowed_hosts: config.mcp_allowed_hosts.clone(),
            mcp_auth_token,
            metrics_event_limit: config.metrics_event_limit,
            configured_namespaces: config.namespaces.clone(),
            build_version,
            git_sha,
        },
        cancel.child_token(),
    );

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("failed to bind {}", config.bind_addr))?;
    info!(
        bind = %config.bind_addr,
        mode = config.events_mode.as_str(),
        "clustersentinel listening"
    );

    let shutdown_cancel = cancel.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            shutdown_cancel.cancel();
        })
        .await
        .context("HTTP server failed")?;

    if let Err(err) = pipeline_task.await {
        warn!(error = %err, "event pipeline task join failed");
    }
    if let Err(err) = writer_task.await {
        warn!(error = %err, "durable writer task join failed");
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

fn install_rustls_crypto_provider() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls CryptoProvider (ring)"))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(err) => warn!(error = %err, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    info!("shutdown signal received");
}
