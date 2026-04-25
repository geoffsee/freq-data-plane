mod error;
mod extractors;
mod routes;
mod state;

use axum::{http::Request, Router};
use data_sdk::{ControlPlane, NewTenantProfile};
use duckdb::Connection;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use state::AppState;
use std::{net::SocketAddr, path::Path, sync::Arc};
use std::{fs, sync::Mutex};
use time::Duration;
use tower_http::trace::TraceLayer;
use tower_sessions::{Expiry, MemoryStore, SessionManagerLayer};
use tracing::{info, info_span, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() {
    let otel_provider = init_tracing();

    let db_encryption_key = std::env::var("DB_ENCRYPTION_KEY")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    if db_encryption_key.is_none() {
        eprintln!("DB_ENCRYPTION_KEY not set — databases will not be encrypted at the application level.");
    }

    // User database (auth, tokens)
    let db_conn = open_user_database(db_encryption_key.as_deref());
    user_database::bootstrap_auth_schema(&db_conn).expect("failed to bootstrap auth schema");

    if user_database::list_users(&db_conn).unwrap().is_empty() {
        let initial_admin_username = std::env::var("INITIAL_ADMIN_USERNAME")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let initial_admin_password = std::env::var("INITIAL_ADMIN_PASSWORD")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        match (initial_admin_username, initial_admin_password) {
            (Some(username), Some(password)) => {
                user_database::create_user(
                    &db_conn,
                    &user_database::NewUser {
                        username: &username,
                        password: &password,
                        display_name: Some("Administrator"),
                    },
                )
                .expect("failed to seed admin user");
                info!(username = %username, "seeded initial admin user");
            }
            (Some(_), None) | (None, Some(_)) => {
                eprintln!(
                    "Initial admin credentials are incomplete. Set both INITIAL_ADMIN_USERNAME and INITIAL_ADMIN_PASSWORD."
                );
            }
            _ => {
                eprintln!(
                    "No user data bootstrapped. Set INITIAL_ADMIN_USERNAME and INITIAL_ADMIN_PASSWORD in production."
                );
            }
        }
    }

    // Control plane database
    let cp = open_control_plane(db_encryption_key.as_deref());

    let tenant_key =
        std::env::var("TENANT_KEY").unwrap_or_else(|_| "default".to_string());

    // Seed default tenant if it doesn't exist
    if cp.get_tenant(&tenant_key).unwrap().is_none() {
        cp.create_tenant(&NewTenantProfile {
            tenant_key: tenant_key.clone(),
            tenant_name: "Default Tenant".to_string(),
            control_plane_uri: format!("memory://{tenant_key}/control.duckdb"),
            default_region: None,
            default_bucket: None,
            default_prefix: None,
        })
        .expect("failed to seed default tenant");
        info!(tenant_key = %tenant_key, "seeded default tenant");
    }

    let data_dir =
        std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());

    let state = Arc::new(AppState {
        app_name: std::env::var("APP_NAME").unwrap_or_else(|_| "My Axum Server".to_string()),
        tenant_key,
        data_dir,
        db: Arc::new(Mutex::new(db_conn)),
        control_plane: Arc::new(Mutex::new(cp)),
    });

    let app = create_app(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let addr = format!("{host}:{port}")
        .parse::<SocketAddr>()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], port)));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    let addr = listener.local_addr().unwrap();
    info!(%addr, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server failed");

    info!("server shutdown complete — flushing remaining spans");
    let _ = otel_provider.shutdown();
}

fn create_app(state: Arc<AppState>) -> Router {
    let session_store = MemoryStore::default();
    let session_secure = parse_bool_env("SESSION_SECURE", true);
    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(session_secure)
        .with_expiry(Expiry::OnInactivity(Duration::hours(1)));

    routes::api_router()
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    let method = request.method();
                    let uri = request.uri();
                    let matched_path = request
                        .extensions()
                        .get::<axum::extract::MatchedPath>()
                        .map(|matched_path| matched_path.as_str());

                    info_span!(
                        "http_request",
                        otel.kind = "server",
                        http.request.method = %method,
                        url.path = %uri.path(),
                        url.query = uri.query().unwrap_or_default(),
                        http.route = matched_path.unwrap_or_default(),
                        http.response.status_code = tracing::field::Empty,
                    )
                })
                .on_response(
                    |response: &axum::response::Response,
                     latency: std::time::Duration,
                     span: &Span| {
                        span.record("http.response.status_code", response.status().as_u16());
                        info!(
                            parent: span,
                            latency_ms = latency.as_millis(),
                            status = response.status().as_u16(),
                            "request completed"
                        );
                    },
                ),
        )
        .layer(session_layer)
        .with_state(state)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("received Ctrl+C, initiating graceful shutdown");
        },
        _ = terminate => {
            info!("received SIGTERM, initiating graceful shutdown");
        },
    }
}

fn open_user_database(encryption_key: Option<&str>) -> Connection {
    match std::env::var("USER_DB_PATH").ok().map(|p| p.trim().to_string()).filter(|p| !p.is_empty()) {
        Some(path) => {
            if let Some(parent) = Path::new(&path).parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).expect("failed to create user database directory");
                }
            }
            match encryption_key {
                Some(key) => {
                    let conn = Connection::open_in_memory().expect("failed to open in-memory DuckDB");
                    conn.execute_batch("LOAD '/app/duckdb_extensions/httpfs.duckdb_extension';")
                        .expect("failed to load httpfs extension for encryption support");
                    let safe_path = path.replace('\'', "''");
                    let safe_key = key.replace('\'', "''");
                    conn.execute_batch(&format!(
                        "ATTACH '{safe_path}' AS user_db (ENCRYPTION_KEY '{safe_key}'); USE user_db;"
                    ))
                    .expect("failed to attach encrypted user database");
                    conn
                }
                None => Connection::open(&path).expect("failed to open configured user database path"),
            }
        }
        None => {
            eprintln!(
                "USER_DB_PATH not set. Falling back to ephemeral in-memory auth DB (state will be lost on restart)."
            );
            Connection::open_in_memory().expect("failed to open user DuckDB")
        }
    }
}

fn open_control_plane(encryption_key: Option<&str>) -> ControlPlane {
    match std::env::var("CONTROL_PLANE_DB_PATH").ok().map(|p| p.trim().to_string()).filter(|p| !p.is_empty()) {
        Some(path) => {
            if let Some(parent) = Path::new(&path).parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).expect("failed to create control plane directory");
                }
            }
            match encryption_key {
                Some(key) => ControlPlane::open_encrypted(&path, key).expect("failed to open encrypted control plane"),
                None => ControlPlane::open(&path).expect("failed to open control plane"),
            }
        }
        None => {
            eprintln!(
                "CONTROL_PLANE_DB_PATH not set. Falling back to ephemeral in-memory control plane DB."
            );
            ControlPlane::open_in_memory().expect("failed to open control plane")
        }
    }
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    std::env::var(key)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "t" | "yes" | "y" | "on"
            )
        })
        .unwrap_or(default)
}

fn init_tracing() -> SdkTracerProvider {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let otel_service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "axum-server".to_string());
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info"));
    let resource = Resource::builder().with_service_name(otel_service_name).build();

    if let Some(endpoint) =
        std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    {
        match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
        {
            Ok(exporter) => {
                let provider = SdkTracerProvider::builder()
                    .with_resource(resource.clone())
                    .with_sampler(Sampler::AlwaysOn)
                    .with_id_generator(RandomIdGenerator::default())
                    .with_batch_exporter(exporter)
                    .build();

                let tracer = provider.tracer("axum-server");
                let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

                tracing_subscriber::registry()
                    .with(filter)
                    .with(tracing_subscriber::fmt::layer().json())
                    .with(otel_layer)
                    .init();

                return provider;
            }
            Err(err) => {
                eprintln!("OTEL exporter not configured: {err}. Continuing without OTLP export.");
            }
        }
    } else {
        eprintln!("OTEL_EXPORTER_OTLP_ENDPOINT not set. Running without OTLP exporter.");
    }

    let provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(Sampler::AlwaysOn)
        .with_id_generator(RandomIdGenerator::default())
        .build();
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .init();
    provider
}
