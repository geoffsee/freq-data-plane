use duckdb::{params, Connection, Error, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantStatus {
    Active,
    Suspended,
    Deleted,
}

impl TenantStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "suspended" => Ok(Self::Suspended),
            "deleted" => Ok(Self::Deleted),
            other => Err(Error::InvalidParameterName(format!(
                "invalid tenant status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantProfile {
    pub tenant_key: String,
    pub tenant_name: String,
    pub control_plane_uri: String,
    pub default_region: Option<String>,
    pub default_bucket: Option<String>,
    pub default_prefix: Option<String>,
    pub status: TenantStatus,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTenantProfile {
    pub tenant_key: String,
    pub tenant_name: String,
    pub control_plane_uri: String,
    pub default_region: Option<String>,
    pub default_bucket: Option<String>,
    pub default_prefix: Option<String>,
}

pub fn create_tenant_profile(conn: &Connection, new_profile: &NewTenantProfile) -> Result<TenantProfile> {
    conn.execute(
        "INSERT INTO control.tenant_profile (
            tenant_key,
            tenant_name,
            control_plane_uri,
            default_region,
            default_bucket,
            default_prefix
        ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            new_profile.tenant_key,
            new_profile.tenant_name,
            new_profile.control_plane_uri,
            new_profile.default_region,
            new_profile.default_bucket,
            new_profile.default_prefix
        ],
    )?;

    get_tenant_profile(conn, &new_profile.tenant_key)?
        .ok_or_else(|| Error::InvalidParameterName("tenant insert did not return row".to_string()))
}

pub fn get_tenant_profile(conn: &Connection, tenant_key: &str) -> Result<Option<TenantProfile>> {
    match conn.query_row(
        "SELECT
            tenant_key,
            tenant_name,
            control_plane_uri,
            default_region,
            default_bucket,
            default_prefix,
            status,
            CAST(created_at AS VARCHAR),
            CAST(updated_at AS VARCHAR)
         FROM control.tenant_profile
         WHERE tenant_key = ?",
        [tenant_key],
        |row| {
            let status: String = row.get(6)?;
            Ok(TenantProfile {
                tenant_key: row.get(0)?,
                tenant_name: row.get(1)?,
                control_plane_uri: row.get(2)?,
                default_region: row.get(3)?,
                default_bucket: row.get(4)?,
                default_prefix: row.get(5)?,
                status: TenantStatus::parse(&status)?,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
            })
        },
    ) {
        Ok(profile) => Ok(Some(profile)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn list_tenant_profiles(conn: &Connection) -> Result<Vec<TenantProfile>> {
    let mut statement = conn.prepare(
        "SELECT
            tenant_key,
            tenant_name,
            control_plane_uri,
            default_region,
            default_bucket,
            default_prefix,
            status,
            CAST(created_at AS VARCHAR),
            CAST(updated_at AS VARCHAR)
         FROM control.tenant_profile
         ORDER BY tenant_key ASC",
    )?;

    let rows = statement.query_map([], |row| {
        let status: String = row.get(6)?;
        Ok(TenantProfile {
            tenant_key: row.get(0)?,
            tenant_name: row.get(1)?,
            control_plane_uri: row.get(2)?,
            default_region: row.get(3)?,
            default_bucket: row.get(4)?,
            default_prefix: row.get(5)?,
            status: TenantStatus::parse(&status)?,
            created_at: row.get(7)?,
            updated_at: row.get(8)?,
        })
    })?;

    rows.collect::<Result<Vec<_>>>()
}

pub fn update_tenant_status(conn: &Connection, tenant_key: &str, status: TenantStatus) -> Result<usize> {
    conn.execute(
        "UPDATE control.tenant_profile
         SET status = ?, updated_at = now()
         WHERE tenant_key = ?",
        params![status.as_str(), tenant_key],
    )
}
