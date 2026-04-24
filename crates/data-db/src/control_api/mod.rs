use duckdb::{Connection, Result};

pub mod asset;
pub mod audit;
pub mod binding;
pub mod database;
pub mod delivery;
pub mod mapping;
pub mod migration;
pub mod policy;
pub mod principal;
pub mod secret;
pub mod session;
pub mod tenant;

#[cfg(test)]
mod tests;

pub use asset::*;
pub use audit::*;
pub use binding::*;
pub use database::*;
pub use delivery::*;
pub use mapping::*;
pub use migration::*;
pub use policy::*;
pub use principal::*;
pub use secret::*;
pub use session::*;
pub use tenant::*;

pub const CONTROL_SCHEMA_SQL: &str =
    include_str!("../../../../migrations/duckdb-s3-datalake/v1/v0.0-prototype.duckdb.sql");

pub fn bootstrap_control_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(CONTROL_SCHEMA_SQL)
}
