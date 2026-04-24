use duckdb::{params, Connection, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub migration_id: i64,
    pub database_ref_id: i64,
    pub version: String,
    pub name: String,
    pub checksum: String,
    pub applied_at: String,
    pub execution_time_ms: Option<i64>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAppliedMigration {
    pub database_ref_id: i64,
    pub version: String,
    pub name: String,
    pub checksum: String,
}

pub fn record_applied_migration(
    conn: &Connection,
    migration: &NewAppliedMigration,
) -> Result<AppliedMigration> {
    conn.execute(
        "INSERT INTO control.applied_migrations (database_ref_id, version, name, checksum)
         VALUES (?, ?, ?, ?)",
        params![
            migration.database_ref_id,
            migration.version,
            migration.name,
            migration.checksum
        ],
    )?;

    conn.query_row(
        "SELECT migration_id, database_ref_id, version, name, checksum,
                CAST(applied_at AS VARCHAR), execution_time_ms, success, error_message
         FROM control.applied_migrations
         WHERE database_ref_id = ? AND version = ?",
        params![migration.database_ref_id, migration.version],
        |row| {
            Ok(AppliedMigration {
                migration_id: row.get(0)?,
                database_ref_id: row.get(1)?,
                version: row.get(2)?,
                name: row.get(3)?,
                checksum: row.get(4)?,
                applied_at: row.get(5)?,
                execution_time_ms: row.get(6)?,
                success: row.get(7)?,
                error_message: row.get(8)?,
            })
        },
    )
}

pub fn record_migration_result(
    conn: &Connection,
    database_ref_id: i64,
    version: &str,
    execution_time_ms: i64,
    success: bool,
    error_message: Option<&str>,
) -> Result<usize> {
    conn.execute(
        "UPDATE control.applied_migrations
         SET execution_time_ms = ?, success = ?, error_message = ?
         WHERE database_ref_id = ? AND version = ?",
        params![execution_time_ms, success, error_message, database_ref_id, version],
    )
}

pub fn list_applied_migrations(
    conn: &Connection,
    database_ref_id: i64,
) -> Result<Vec<AppliedMigration>> {
    let mut stmt = conn.prepare(
        "SELECT migration_id, database_ref_id, version, name, checksum,
                CAST(applied_at AS VARCHAR), execution_time_ms, success, error_message
         FROM control.applied_migrations
         WHERE database_ref_id = ?
         ORDER BY version",
    )?;
    let rows = stmt.query_map([database_ref_id], |row| {
        Ok(AppliedMigration {
            migration_id: row.get(0)?,
            database_ref_id: row.get(1)?,
            version: row.get(2)?,
            name: row.get(3)?,
            checksum: row.get(4)?,
            applied_at: row.get(5)?,
            execution_time_ms: row.get(6)?,
            success: row.get(7)?,
            error_message: row.get(8)?,
        })
    })?;
    rows.collect()
}

/// Parses a migration filename like `v0.0.create_users.sql` into (version, name).
/// Returns None if the filename doesn't match the expected pattern.
pub fn parse_migration_filename(filename: &str) -> Option<(String, String)> {
    let stem = filename.strip_suffix(".sql")?;
    // Pattern: v{major}.{minor}.{name}
    // Split on '.' to get parts: ["v0", "0", "create_users"]
    let parts: Vec<&str> = stem.splitn(3, '.').collect();
    if parts.len() != 3 {
        return None;
    }
    if !parts[0].starts_with('v') {
        return None;
    }
    let version = format!("{}.{}", parts[0], parts[1]);
    let name = parts[2].to_string();
    Some((version, name))
}

#[cfg(test)]
mod filename_tests {
    use super::*;

    #[test]
    fn parse_valid_migration_filename() {
        let result = parse_migration_filename("v0.0.create_users.sql");
        assert_eq!(result, Some(("v0.0".to_string(), "create_users".to_string())));
    }

    #[test]
    fn parse_multi_dot_name() {
        let result = parse_migration_filename("v1.2.add_index.users.sql");
        assert_eq!(result, Some(("v1.2".to_string(), "add_index.users".to_string())));
    }

    #[test]
    fn parse_invalid_no_sql_extension() {
        assert_eq!(parse_migration_filename("v0.0.create_users.txt"), None);
    }

    #[test]
    fn parse_invalid_no_version_prefix() {
        assert_eq!(parse_migration_filename("0.0.create_users.sql"), None);
    }

    #[test]
    fn parse_invalid_too_few_parts() {
        assert_eq!(parse_migration_filename("v0.sql"), None);
    }
}
