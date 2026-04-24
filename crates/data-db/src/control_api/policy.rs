use duckdb::{params, Connection, Error, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyEffect {
    Allow,
    Deny,
}

impl PolicyEffect {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "allow" => Ok(Self::Allow),
            "deny" => Ok(Self::Deny),
            other => Err(Error::InvalidParameterName(format!(
                "invalid policy effect: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubjectType {
    Principal,
    Role,
    Public,
}

impl SubjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Principal => "principal",
            Self::Role => "role",
            Self::Public => "public",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "principal" => Ok(Self::Principal),
            "role" => Ok(Self::Role),
            "public" => Ok(Self::Public),
            other => Err(Error::InvalidParameterName(format!(
                "invalid subject type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceType {
    DatabaseRef,
    Asset,
    Schema,
    Table,
    View,
    UriPrefix,
    System,
}

impl ResourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DatabaseRef => "database_ref",
            Self::Asset => "asset",
            Self::Schema => "schema",
            Self::Table => "table",
            Self::View => "view",
            Self::UriPrefix => "uri_prefix",
            Self::System => "system",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "database_ref" => Ok(Self::DatabaseRef),
            "asset" => Ok(Self::Asset),
            "schema" => Ok(Self::Schema),
            "table" => Ok(Self::Table),
            "view" => Ok(Self::View),
            "uri_prefix" => Ok(Self::UriPrefix),
            "system" => Ok(Self::System),
            other => Err(Error::InvalidParameterName(format!(
                "invalid resource type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessPolicy {
    pub policy_key: String,
    pub policy_name: String,
    pub effect: PolicyEffect,
    pub subject_type: SubjectType,
    pub principal_id: Option<i64>,
    pub role_id: Option<i64>,
    pub resource_type: ResourceType,
    pub database_ref_id: Option<i64>,
    pub asset_id: Option<i64>,
    pub can_read: bool,
    pub can_write: bool,
    pub can_admin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAccessPolicy {
    pub policy_key: String,
    pub policy_name: String,
    pub effect: PolicyEffect,
    pub subject_type: SubjectType,
    pub principal_id: Option<i64>,
    pub role_id: Option<i64>,
    pub resource_type: ResourceType,
    pub database_ref_id: Option<i64>,
    pub asset_id: Option<i64>,
    pub can_read: bool,
    pub can_write: bool,
    pub can_admin: bool,
}

pub fn create_access_policy(conn: &Connection, new_policy: &NewAccessPolicy) -> Result<AccessPolicy> {
    conn.execute(
        "INSERT INTO control.access_policies (
            policy_key,
            policy_name,
            effect,
            subject_type,
            principal_id,
            role_id,
            resource_type,
            database_ref_id,
            asset_id,
            can_read,
            can_write,
            can_admin
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            new_policy.policy_key,
            new_policy.policy_name,
            new_policy.effect.as_str(),
            new_policy.subject_type.as_str(),
            new_policy.principal_id,
            new_policy.role_id,
            new_policy.resource_type.as_str(),
            new_policy.database_ref_id,
            new_policy.asset_id,
            new_policy.can_read,
            new_policy.can_write,
            new_policy.can_admin
        ],
    )?;
    get_access_policy_by_key(conn, &new_policy.policy_key)?.ok_or_else(|| {
        Error::InvalidParameterName("access policy insert did not return row".to_string())
    })
}

pub fn get_access_policy_by_key(conn: &Connection, policy_key: &str) -> Result<Option<AccessPolicy>> {
    match conn.query_row(
        "SELECT policy_key, policy_name, effect, subject_type, principal_id, role_id, resource_type, database_ref_id, asset_id, can_read, can_write, can_admin
         FROM control.access_policies
         WHERE policy_key = ?",
        [policy_key],
        |row| {
            let effect: String = row.get(2)?;
            let subject_type: String = row.get(3)?;
            let resource_type: String = row.get(6)?;
            Ok(AccessPolicy {
                policy_key: row.get(0)?,
                policy_name: row.get(1)?,
                effect: PolicyEffect::parse(&effect)?,
                subject_type: SubjectType::parse(&subject_type)?,
                principal_id: row.get(4)?,
                role_id: row.get(5)?,
                resource_type: ResourceType::parse(&resource_type)?,
                database_ref_id: row.get(7)?,
                asset_id: row.get(8)?,
                can_read: row.get(9)?,
                can_write: row.get(10)?,
                can_admin: row.get(11)?,
            })
        },
    ) {
        Ok(policy) => Ok(Some(policy)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}
