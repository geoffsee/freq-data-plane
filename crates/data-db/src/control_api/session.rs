use duckdb::{params, Connection, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub principal_id: Option<i64>,
    pub client_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSession {
    pub principal_id: Option<i64>,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub client_ip: Option<String>,
}

pub fn create_session(conn: &Connection, session: &NewSession) -> Result<Session> {
    conn.execute(
        "INSERT INTO control.sessions (principal_id, client_name, client_version, client_ip)
         VALUES (?, ?, ?, ?)",
        params![
            session.principal_id,
            session.client_name,
            session.client_version,
            session.client_ip
        ],
    )?;
    conn.query_row(
        "SELECT CAST(session_id AS VARCHAR), principal_id, client_name
         FROM control.sessions
         ORDER BY started_at DESC
         LIMIT 1",
        [],
        |row| {
            Ok(Session {
                session_id: row.get(0)?,
                principal_id: row.get(1)?,
                client_name: row.get(2)?,
            })
        },
    )
}
