use duckdb::{params, Connection, Error, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretProvider {
    Aws,
    Minio,
    R2,
    Gcs,
    Azure,
    Vault,
    Other,
}

impl SecretProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Minio => "minio",
            Self::R2 => "r2",
            Self::Gcs => "gcs",
            Self::Azure => "azure",
            Self::Vault => "vault",
            Self::Other => "other",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "aws" => Ok(Self::Aws),
            "minio" => Ok(Self::Minio),
            "r2" => Ok(Self::R2),
            "gcs" => Ok(Self::Gcs),
            "azure" => Ok(Self::Azure),
            "vault" => Ok(Self::Vault),
            "other" => Ok(Self::Other),
            other => Err(Error::InvalidParameterName(format!(
                "invalid secret provider: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    IamRole,
    InstanceProfile,
    Sts,
    AccessKey,
    Oauth,
    ExternalSecret,
}

impl AuthMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IamRole => "iam_role",
            Self::InstanceProfile => "instance_profile",
            Self::Sts => "sts",
            Self::AccessKey => "access_key",
            Self::Oauth => "oauth",
            Self::ExternalSecret => "external_secret",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "iam_role" => Ok(Self::IamRole),
            "instance_profile" => Ok(Self::InstanceProfile),
            "sts" => Ok(Self::Sts),
            "access_key" => Ok(Self::AccessKey),
            "oauth" => Ok(Self::Oauth),
            "external_secret" => Ok(Self::ExternalSecret),
            other => Err(Error::InvalidParameterName(format!(
                "invalid auth method: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretStatus {
    Active,
    Disabled,
    Deleted,
}

impl SecretStatus {
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
                "invalid secret status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretHandle {
    pub secret_handle_id: String,
    pub secret_key: String,
    pub secret_name: String,
    pub provider: SecretProvider,
    pub auth_method: AuthMethod,
    pub external_secret_ref: String,
    pub allowed_uri_prefix: Option<String>,
    pub status: SecretStatus,
    pub created_at: String,
    pub rotated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSecretHandle {
    pub secret_key: String,
    pub secret_name: String,
    pub provider: SecretProvider,
    pub auth_method: AuthMethod,
    pub external_secret_ref: String,
    pub allowed_uri_prefix: Option<String>,
}

pub fn create_secret_handle(conn: &Connection, new_secret: &NewSecretHandle) -> Result<SecretHandle> {
    conn.execute(
        "INSERT INTO control.secret_handles (
            secret_key,
            secret_name,
            provider,
            auth_method,
            external_secret_ref,
            allowed_uri_prefix
        ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            new_secret.secret_key,
            new_secret.secret_name,
            new_secret.provider.as_str(),
            new_secret.auth_method.as_str(),
            new_secret.external_secret_ref,
            new_secret.allowed_uri_prefix,
        ],
    )?;

    get_secret_handle_by_key(conn, &new_secret.secret_key)?
        .ok_or_else(|| Error::InvalidParameterName("secret handle insert did not return row".to_string()))
}

pub fn list_secret_handles(conn: &Connection) -> Result<Vec<SecretHandle>> {
    let mut statement = conn.prepare(
        "SELECT
            CAST(secret_handle_id AS VARCHAR),
            secret_key,
            secret_name,
            provider,
            auth_method,
            external_secret_ref,
            allowed_uri_prefix,
            status,
            CAST(created_at AS VARCHAR),
            CAST(rotated_at AS VARCHAR)
         FROM control.secret_handles
         ORDER BY created_at ASC",
    )?;
    let rows = statement.query_map([], |row| {
        let provider: String = row.get(3)?;
        let auth_method: String = row.get(4)?;
        let status: String = row.get(7)?;
        Ok(SecretHandle {
            secret_handle_id: row.get(0)?,
            secret_key: row.get(1)?,
            secret_name: row.get(2)?,
            provider: SecretProvider::parse(&provider)?,
            auth_method: AuthMethod::parse(&auth_method)?,
            external_secret_ref: row.get(5)?,
            allowed_uri_prefix: row.get(6)?,
            status: SecretStatus::parse(&status)?,
            created_at: row.get(8)?,
            rotated_at: row.get(9)?,
        })
    })?;
    rows.collect::<Result<Vec<_>>>()
}

pub fn get_secret_handle_by_key(conn: &Connection, secret_key: &str) -> Result<Option<SecretHandle>> {
    match conn.query_row(
        "SELECT
            CAST(secret_handle_id AS VARCHAR),
            secret_key,
            secret_name,
            provider,
            auth_method,
            external_secret_ref,
            allowed_uri_prefix,
            status,
            CAST(created_at AS VARCHAR),
            CAST(rotated_at AS VARCHAR)
         FROM control.secret_handles
         WHERE secret_key = ?",
        [secret_key],
        |row| {
            let provider: String = row.get(3)?;
            let auth_method: String = row.get(4)?;
            let status: String = row.get(7)?;
            Ok(SecretHandle {
                secret_handle_id: row.get(0)?,
                secret_key: row.get(1)?,
                secret_name: row.get(2)?,
                provider: SecretProvider::parse(&provider)?,
                auth_method: AuthMethod::parse(&auth_method)?,
                external_secret_ref: row.get(5)?,
                allowed_uri_prefix: row.get(6)?,
                status: SecretStatus::parse(&status)?,
                created_at: row.get(8)?,
                rotated_at: row.get(9)?,
            })
        },
    ) {
        Ok(s) => Ok(Some(s)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}
