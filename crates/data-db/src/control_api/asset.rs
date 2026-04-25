use duckdb::{params, Connection, Error, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetType {
    Database,
    Schema,
    Table,
    View,
    File,
    Dataset,
    Feed,
    Export,
    Report,
    Model,
    Endpoint,
}

impl AssetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::Schema => "schema",
            Self::Table => "table",
            Self::View => "view",
            Self::File => "file",
            Self::Dataset => "dataset",
            Self::Feed => "feed",
            Self::Export => "export",
            Self::Report => "report",
            Self::Model => "model",
            Self::Endpoint => "endpoint",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "database" => Ok(Self::Database),
            "schema" => Ok(Self::Schema),
            "table" => Ok(Self::Table),
            "view" => Ok(Self::View),
            "file" => Ok(Self::File),
            "dataset" => Ok(Self::Dataset),
            "feed" => Ok(Self::Feed),
            "export" => Ok(Self::Export),
            "report" => Ok(Self::Report),
            "model" => Ok(Self::Model),
            "endpoint" => Ok(Self::Endpoint),
            other => Err(Error::InvalidParameterName(format!("invalid asset type: {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

impl Classification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Confidential => "confidential",
            Self::Restricted => "restricted",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "public" => Ok(Self::Public),
            "internal" => Ok(Self::Internal),
            "confidential" => Ok(Self::Confidential),
            "restricted" => Ok(Self::Restricted),
            other => Err(Error::InvalidParameterName(format!(
                "invalid classification: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetStatus {
    Active,
    Deprecated,
    Archived,
    Deleted,
}

impl AssetStatus {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "deprecated" => Ok(Self::Deprecated),
            "archived" => Ok(Self::Archived),
            "deleted" => Ok(Self::Deleted),
            other => Err(Error::InvalidParameterName(format!("invalid asset status: {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Asset {
    pub asset_id: i64,
    pub asset_key: String,
    pub asset_name: String,
    pub asset_type: AssetType,
    pub database_ref_id: Option<i64>,
    pub owner_principal_id: Option<i64>,
    pub classification: Classification,
    pub status: AssetStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAsset {
    pub asset_key: String,
    pub asset_name: String,
    pub asset_type: AssetType,
    pub database_ref_id: Option<i64>,
    pub owner_principal_id: Option<i64>,
    pub classification: Classification,
}

pub fn create_asset(conn: &Connection, new_asset: &NewAsset) -> Result<Asset> {
    conn.execute(
        "INSERT INTO control.assets (
            asset_key,
            asset_name,
            asset_type,
            database_ref_id,
            owner_principal_id,
            classification
         ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            new_asset.asset_key,
            new_asset.asset_name,
            new_asset.asset_type.as_str(),
            new_asset.database_ref_id,
            new_asset.owner_principal_id,
            new_asset.classification.as_str()
        ],
    )?;
    get_asset_by_key(conn, &new_asset.asset_key)?
        .ok_or_else(|| Error::InvalidParameterName("asset insert did not return row".to_string()))
}

pub fn list_assets(conn: &Connection) -> Result<Vec<Asset>> {
    let mut statement = conn.prepare(
        "SELECT asset_id, asset_key, asset_name, asset_type, database_ref_id, owner_principal_id, classification, status
         FROM control.assets
         ORDER BY asset_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        let asset_type: String = row.get(3)?;
        let classification: String = row.get(6)?;
        let status: String = row.get(7)?;
        Ok(Asset {
            asset_id: row.get(0)?,
            asset_key: row.get(1)?,
            asset_name: row.get(2)?,
            asset_type: AssetType::parse(&asset_type)?,
            database_ref_id: row.get(4)?,
            owner_principal_id: row.get(5)?,
            classification: Classification::parse(&classification)?,
            status: AssetStatus::parse(&status)?,
        })
    })?;
    rows.collect::<Result<Vec<_>>>()
}

pub fn get_asset_by_key(conn: &Connection, asset_key: &str) -> Result<Option<Asset>> {
    match conn.query_row(
        "SELECT asset_id, asset_key, asset_name, asset_type, database_ref_id, owner_principal_id, classification, status
         FROM control.assets
         WHERE asset_key = ?",
        [asset_key],
        |row| {
            let asset_type: String = row.get(3)?;
            let classification: String = row.get(6)?;
            let status: String = row.get(7)?;
            Ok(Asset {
                asset_id: row.get(0)?,
                asset_key: row.get(1)?,
                asset_name: row.get(2)?,
                asset_type: AssetType::parse(&asset_type)?,
                database_ref_id: row.get(4)?,
                owner_principal_id: row.get(5)?,
                classification: Classification::parse(&classification)?,
                status: AssetStatus::parse(&status)?,
            })
        },
    ) {
        Ok(asset) => Ok(Some(asset)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}
