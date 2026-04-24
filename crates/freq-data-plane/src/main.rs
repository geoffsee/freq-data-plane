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
use std::{net::SocketAddr, sync::Arc};
use std::sync::Mutex;
use time::Duration;
use tower_http::trace::TraceLayer;
use tower_sessions::{Expiry, MemoryStore, SessionManagerLayer};
use tracing::{info, info_span, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() {
    let otel_provider = init_tracing();

    // User database (auth, tokens)
    let db_conn = Connection::open_in_memory().expect("failed to open user DuckDB");
    user_database::bootstrap_auth_schema(&db_conn).expect("failed to bootstrap auth schema");

    if user_database::list_users(&db_conn).unwrap().is_empty() {
        user_database::create_user(
            &db_conn,
            &user_database::NewUser {
                username: "admin",
                password: "admin",
                display_name: Some("Administrator"),
            },
        )
        .expect("failed to seed admin user");
        info!("seeded default user: admin / admin");
    }

    // Control plane database
    let cp = ControlPlane::open_in_memory().expect("failed to open control plane");

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
        std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());

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
        .unwrap_or(0);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

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
    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
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

fn init_tracing() -> SdkTracerProvider {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(
            std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:4317".to_string()),
        )
        .build()
        .expect("failed to create OTLP span exporter");

    let provider = SdkTracerProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name(
                    std::env::var("OTEL_SERVICE_NAME")
                        .unwrap_or_else(|_| "axum-server".to_string()),
                )
                .build(),
        )
        .with_sampler(Sampler::AlwaysOn)
        .with_id_generator(RandomIdGenerator::default())
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer("axum-server");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .with(otel_layer)
        .init();

    provider
}
