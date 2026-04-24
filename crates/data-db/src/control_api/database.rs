use duckdb::{params, Connection, Error, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseKind {
    DuckdbFile,
    ParquetDataset,
    CsvDataset,
    JsonDataset,
    IcebergCatalog,
    DeltaTable,
    SqliteFile,
    PostgresDatabase,
}

impl DatabaseKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DuckdbFile => "duckdb_file",
            Self::ParquetDataset => "parquet_dataset",
            Self::CsvDataset => "csv_dataset",
            Self::JsonDataset => "json_dataset",
            Self::IcebergCatalog => "iceberg_catalog",
            Self::DeltaTable => "delta_table",
            Self::SqliteFile => "sqlite_file",
            Self::PostgresDatabase => "postgres_database",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "duckdb_file" => Ok(Self::DuckdbFile),
            "parquet_dataset" => Ok(Self::ParquetDataset),
            "csv_dataset" => Ok(Self::CsvDataset),
            "json_dataset" => Ok(Self::JsonDataset),
            "iceberg_catalog" => Ok(Self::IcebergCatalog),
            "delta_table" => Ok(Self::DeltaTable),
            "sqlite_file" => Ok(Self::SqliteFile),
            "postgres_database" => Ok(Self::PostgresDatabase),
            other => Err(Error::InvalidParameterName(format!(
                "invalid database kind: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseRefStatus {
    Active,
    Archived,
    Deleted,
}

impl DatabaseRefStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "archived" => Ok(Self::Archived),
            "deleted" => Ok(Self::Deleted),
            other => Err(Error::InvalidParameterName(format!(
                "invalid database ref status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseRef {
    pub database_ref_id: i64,
    pub database_key: String,
    pub database_name: String,
    pub database_kind: DatabaseKind,
    pub uri: String,
    pub attach_alias: Option<String>,
    pub status: DatabaseRefStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewDatabaseRef {
    pub database_key: String,
    pub database_name: String,
    pub database_kind: DatabaseKind,
    pub uri: String,
    pub attach_alias: Option<String>,
}

pub fn create_database_ref(conn: &Connection, new_database_ref: &NewDatabaseRef) -> Result<DatabaseRef> {
    conn.execute(
        "INSERT INTO control.database_refs (
            database_key,
            database_name,
            database_kind,
            uri,
            attach_alias
        ) VALUES (?, ?, ?, ?, ?)",
        params![
            new_database_ref.database_key,
            new_database_ref.database_name,
            new_database_ref.database_kind.as_str(),
            new_database_ref.uri,
            new_database_ref.attach_alias
        ],
    )?;
    get_database_ref_by_key(conn, &new_database_ref.database_key)?
        .ok_or_else(|| Error::InvalidParameterName("database ref insert did not return row".to_string()))
}

pub fn list_database_refs(conn: &Connection) -> Result<Vec<DatabaseRef>> {
    let mut stmt = conn.prepare(
        "SELECT database_ref_id, database_key, database_name, database_kind, uri, attach_alias, status
         FROM control.database_refs
         ORDER BY database_ref_id",
    )?;
    let rows = stmt.query_map([], |row| {
        let database_kind: String = row.get(3)?;
        let status: String = row.get(6)?;
        Ok(DatabaseRef {
            database_ref_id: row.get(0)?,
            database_key: row.get(1)?,
            database_name: row.get(2)?,
            database_kind: DatabaseKind::parse(&database_kind)?,
            uri: row.get(4)?,
            attach_alias: row.get(5)?,
            status: DatabaseRefStatus::parse(&status)?,
        })
    })?;
    rows.collect()
}

pub fn update_database_ref_status(
    conn: &Connection,
    database_key: &str,
    status: DatabaseRefStatus,
) -> Result<usize> {
    conn.execute(
        "UPDATE control.database_refs SET status = ?, updated_at = now() WHERE database_key = ?",
        params![status.as_str(), database_key],
    )
}

pub fn get_database_ref_by_key(conn: &Connection, database_key: &str) -> Result<Option<DatabaseRef>> {
    match conn.query_row(
        "SELECT database_ref_id, database_key, database_name, database_kind, uri, attach_alias, status
         FROM control.database_refs
         WHERE database_key = ?",
        [database_key],
        |row| {
            let database_kind: String = row.get(3)?;
            let status: String = row.get(6)?;
            Ok(DatabaseRef {
                database_ref_id: row.get(0)?,
                database_key: row.get(1)?,
                database_name: row.get(2)?,
                database_kind: DatabaseKind::parse(&database_kind)?,
                uri: row.get(4)?,
                attach_alias: row.get(5)?,
                status: DatabaseRefStatus::parse(&status)?,
            })
        },
    ) {
        Ok(db_ref) => Ok(Some(db_ref)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}
