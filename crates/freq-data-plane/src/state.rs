use data_sdk::ControlPlane;
use duckdb::Connection;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct AppState {
    pub app_name: String,
    pub tenant_key: String,
    pub data_dir: String,
    pub db: Arc<Mutex<Connection>>,
    pub control_plane: Arc<Mutex<ControlPlane>>,
}
