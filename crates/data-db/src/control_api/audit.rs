use duckdb::{params, Connection, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub audit_id: i64,
    pub session_id: Option<String>,
    pub principal_id: Option<i64>,
    pub action: String,
    pub success: bool,
    pub asset_id: Option<i64>,
    pub error_message: Option<String>,
    pub occurred_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAuditLogEntry {
    pub session_id: Option<String>,
    pub principal_id: Option<i64>,
    pub action: String,
    pub success: bool,
    pub asset_id: Option<i64>,
    pub error_message: Option<String>,
}

pub fn create_audit_log_entry(conn: &Connection, entry: &NewAuditLogEntry) -> Result<AuditLogEntry> {
    conn.execute(
        "INSERT INTO control.audit_log (
            session_id,
            principal_id,
            action,
            success,
            asset_id,
            error_message
         ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            entry.session_id,
            entry.principal_id,
            entry.action,
            entry.success,
            entry.asset_id,
            entry.error_message
        ],
    )?;
    conn.query_row(
        "SELECT audit_id, CAST(session_id AS VARCHAR), principal_id, action, success, asset_id, error_message, CAST(occurred_at AS VARCHAR)
         FROM control.audit_log
         ORDER BY occurred_at DESC
         LIMIT 1",
        [],
        |row| {
            Ok(AuditLogEntry {
                audit_id: row.get(0)?,
                session_id: row.get(1)?,
                principal_id: row.get(2)?,
                action: row.get(3)?,
                success: row.get(4)?,
                asset_id: row.get(5)?,
                error_message: row.get(6)?,
                occurred_at: row.get(7)?,
            })
        },
    )
}

pub fn list_audit_log_entries(conn: &Connection, limit: i64) -> Result<Vec<AuditLogEntry>> {
    let mut statement = conn.prepare(
        "SELECT audit_id, CAST(session_id AS VARCHAR), principal_id, action, success, asset_id, error_message, CAST(occurred_at AS VARCHAR)
         FROM control.audit_log
         ORDER BY occurred_at DESC
         LIMIT ?",
    )?;
    let rows = statement.query_map([limit], |row| {
        Ok(AuditLogEntry {
            audit_id: row.get(0)?,
            session_id: row.get(1)?,
            principal_id: row.get(2)?,
            action: row.get(3)?,
            success: row.get(4)?,
            asset_id: row.get(5)?,
            error_message: row.get(6)?,
            occurred_at: row.get(7)?,
        })
    })?;
    rows.collect::<Result<Vec<_>>>()
}
