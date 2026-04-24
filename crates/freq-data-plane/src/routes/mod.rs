pub mod health;
pub mod tenant;
pub mod principals;
pub mod roles;
pub mod secrets;
pub mod databases;
pub mod bindings;
pub mod assets;
pub mod mappings;
pub mod policies;
pub mod delivery_channels;
pub mod deliveries;
pub mod sessions;
pub mod audit;
pub mod tokens;
pub mod pages;

use crate::state::AppState;
use axum::Router;
use std::sync::Arc;

pub fn api_router() -> Router<Arc<AppState>> {
    Router::new()
        .merge(health::router())
        .merge(tenant::router())
        .merge(principals::router())
        .merge(roles::router())
        .merge(secrets::router())
        .merge(databases::router())
        .merge(bindings::router())
        .merge(assets::router())
        .merge(mappings::router())
        .merge(policies::router())
        .merge(delivery_channels::router())
        .merge(deliveries::router())
        .merge(sessions::router())
        .merge(audit::router())
        .merge(tokens::router())
        .merge(pages::router())
}
