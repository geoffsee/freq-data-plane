use duckdb::{params, Connection, Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingStatus {
    Active,
    Disabled,
    Deleted,
}

impl BindingStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "disabled" => Ok(Self::Disabled),
            "deleted" => Ok(Self::Deleted),
            other => Err(Error::InvalidParameterName(format!(
                "invalid binding status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseBinding {
    pub binding_id: i64,
    pub database_ref_id: i64,
    pub deployment_key: String,
    pub binding_name: String,
    pub status: BindingStatus,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewDatabaseBinding {
    pub database_ref_id: i64,
    pub deployment_key: String,
    pub binding_name: String,
}

fn row_to_binding(row: &duckdb::Row<'_>) -> duckdb::Result<DatabaseBinding> {
    let status: String = row.get(4)?;
    Ok(DatabaseBinding {
        binding_id: row.get(0)?,
        database_ref_id: row.get(1)?,
        deployment_key: row.get(2)?,
        binding_name: row.get(3)?,
        status: BindingStatus::parse(&status)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

pub fn create_database_binding(
    conn: &Connection,
    new_binding: &NewDatabaseBinding,
) -> Result<DatabaseBinding> {
    conn.execute(
        "INSERT INTO control.database_bindings (database_ref_id, deployment_key, binding_name)
         VALUES (?, ?, ?)",
        params![
            new_binding.database_ref_id,
            new_binding.deployment_key,
            new_binding.binding_name
        ],
    )?;

    // Retrieve the just-inserted row
    conn.query_row(
        "SELECT binding_id, database_ref_id, deployment_key, binding_name, status,
                CAST(created_at AS VARCHAR), CAST(updated_at AS VARCHAR)
         FROM control.database_bindings
         WHERE deployment_key = ? AND binding_name = ?",
        params![new_binding.deployment_key, new_binding.binding_name],
        row_to_binding,
    )
}

pub fn list_bindings_for_deployment(
    conn: &Connection,
    deployment_key: &str,
) -> Result<Vec<DatabaseBinding>> {
    let mut stmt = conn.prepare(
        "SELECT binding_id, database_ref_id, deployment_key, binding_name, status,
                CAST(created_at AS VARCHAR), CAST(updated_at AS VARCHAR)
         FROM control.database_bindings
         WHERE deployment_key = ?
         ORDER BY binding_id",
    )?;
    let rows = stmt.query_map([deployment_key], row_to_binding)?;
    rows.collect()
}

pub fn list_bindings_for_database(
    conn: &Connection,
    database_ref_id: i64,
) -> Result<Vec<DatabaseBinding>> {
    let mut stmt = conn.prepare(
        "SELECT binding_id, database_ref_id, deployment_key, binding_name, status,
                CAST(created_at AS VARCHAR), CAST(updated_at AS VARCHAR)
         FROM control.database_bindings
         WHERE database_ref_id = ?
         ORDER BY binding_id",
    )?;
    let rows = stmt.query_map([database_ref_id], row_to_binding)?;
    rows.collect()
}

pub fn delete_database_binding(conn: &Connection, binding_id: i64) -> Result<usize> {
    conn.execute(
        "DELETE FROM control.database_bindings WHERE binding_id = ?",
        [binding_id],
    )
}
