use duckdb::{params, Connection, Error, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalType {
    User,
    ServiceAccount,
    Group,
    System,
}

impl PrincipalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::ServiceAccount => "service_account",
            Self::Group => "group",
            Self::System => "system",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "user" => Ok(Self::User),
            "service_account" => Ok(Self::ServiceAccount),
            "group" => Ok(Self::Group),
            "system" => Ok(Self::System),
            other => Err(Error::InvalidParameterName(format!(
                "invalid principal type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalStatus {
    Active,
    Disabled,
    Deleted,
}

impl PrincipalStatus {
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
                "invalid principal status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub principal_id: i64,
    pub principal_key: String,
    pub principal_type: PrincipalType,
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub status: PrincipalStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewPrincipal {
    pub principal_key: String,
    pub principal_type: PrincipalType,
    pub display_name: Option<String>,
    pub email: Option<String>,
}

pub fn create_principal(conn: &Connection, new_principal: &NewPrincipal) -> Result<Principal> {
    conn.execute(
        "INSERT INTO control.principals (
            principal_key,
            principal_type,
            display_name,
            email
        ) VALUES (?, ?, ?, ?)",
        params![
            new_principal.principal_key,
            new_principal.principal_type.as_str(),
            new_principal.display_name,
            new_principal.email
        ],
    )?;
    get_principal_by_key(conn, &new_principal.principal_key)?
        .ok_or_else(|| Error::InvalidParameterName("principal insert did not return row".to_string()))
}

pub fn get_principal_by_key(conn: &Connection, principal_key: &str) -> Result<Option<Principal>> {
    match conn.query_row(
        "SELECT principal_id, principal_key, principal_type, display_name, email, status
         FROM control.principals
         WHERE principal_key = ?",
        [principal_key],
        |row| {
            let principal_type: String = row.get(2)?;
            let status: String = row.get(5)?;
            Ok(Principal {
                principal_id: row.get(0)?,
                principal_key: row.get(1)?,
                principal_type: PrincipalType::parse(&principal_type)?,
                display_name: row.get(3)?,
                email: row.get(4)?,
                status: PrincipalStatus::parse(&status)?,
            })
        },
    ) {
        Ok(principal) => Ok(Some(principal)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn list_principals(conn: &Connection) -> Result<Vec<Principal>> {
    let mut statement = conn.prepare(
        "SELECT principal_id, principal_key, principal_type, display_name, email, status
         FROM control.principals
         ORDER BY principal_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        let principal_type: String = row.get(2)?;
        let status: String = row.get(5)?;
        Ok(Principal {
            principal_id: row.get(0)?,
            principal_key: row.get(1)?,
            principal_type: PrincipalType::parse(&principal_type)?,
            display_name: row.get(3)?,
            email: row.get(4)?,
            status: PrincipalStatus::parse(&status)?,
        })
    })?;
    rows.collect::<Result<Vec<_>>>()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    pub role_id: i64,
    pub role_key: String,
    pub role_name: String,
    pub description: Option<String>,
    pub is_system_role: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRole {
    pub role_key: String,
    pub role_name: String,
    pub description: Option<String>,
}

pub fn create_role(conn: &Connection, new_role: &NewRole) -> Result<Role> {
    conn.execute(
        "INSERT INTO control.roles (role_key, role_name, description)
         VALUES (?, ?, ?)",
        params![new_role.role_key, new_role.role_name, new_role.description],
    )?;
    get_role_by_key(conn, &new_role.role_key)?
        .ok_or_else(|| Error::InvalidParameterName("role insert did not return row".to_string()))
}

pub fn get_role_by_key(conn: &Connection, role_key: &str) -> Result<Option<Role>> {
    match conn.query_row(
        "SELECT role_id, role_key, role_name, description, is_system_role
         FROM control.roles
         WHERE role_key = ?",
        [role_key],
        |row| {
            Ok(Role {
                role_id: row.get(0)?,
                role_key: row.get(1)?,
                role_name: row.get(2)?,
                description: row.get(3)?,
                is_system_role: row.get(4)?,
            })
        },
    ) {
        Ok(role) => Ok(Some(role)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn grant_role_to_principal(
    conn: &Connection,
    principal_id: i64,
    role_id: i64,
    granted_by_principal_id: Option<i64>,
) -> Result<usize> {
    conn.execute(
        "INSERT INTO control.principal_roles (principal_id, role_id, granted_by_principal_id)
         VALUES (?, ?, ?)",
        params![principal_id, role_id, granted_by_principal_id],
    )
}

pub fn list_roles_for_principal(conn: &Connection, principal_id: i64) -> Result<Vec<Role>> {
    let mut statement = conn.prepare(
        "SELECT r.role_id, r.role_key, r.role_name, r.description, r.is_system_role
         FROM control.roles r
         INNER JOIN control.principal_roles pr ON pr.role_id = r.role_id
         WHERE pr.principal_id = ?
         ORDER BY r.role_key ASC",
    )?;
    let rows = statement.query_map([principal_id], |row| {
        Ok(Role {
            role_id: row.get(0)?,
            role_key: row.get(1)?,
            role_name: row.get(2)?,
            description: row.get(3)?,
            is_system_role: row.get(4)?,
        })
    })?;
    rows.collect::<Result<Vec<_>>>()
}
