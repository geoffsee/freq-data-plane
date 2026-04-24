use argon2::Argon2;
use duckdb::{params, Connection, Error, Result};
use password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng};
use rand_core::RngCore;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const SCHEMA_SQL: &str = "
CREATE SCHEMA IF NOT EXISTS auth;

CREATE SEQUENCE IF NOT EXISTS auth.user_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS auth.token_id_seq START 1;

CREATE TABLE IF NOT EXISTS auth.users (
    user_id    INTEGER PRIMARY KEY DEFAULT nextval('auth.user_id_seq'),
    username   VARCHAR NOT NULL UNIQUE,
    password_hash VARCHAR NOT NULL,
    display_name  VARCHAR,
    disabled   BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS auth.api_tokens (
    token_id   INTEGER PRIMARY KEY DEFAULT nextval('auth.token_id_seq'),
    user_id    INTEGER NOT NULL REFERENCES auth.users(user_id),
    token_hash VARCHAR NOT NULL UNIQUE,
    label      VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);
";

pub fn bootstrap_auth_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA_SQL)
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct User {
    pub user_id: i64,
    pub username: String,
    pub display_name: Option<String>,
    pub disabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

pub struct NewUser<'a> {
    pub username: &'a str,
    pub password: &'a str,
    pub display_name: Option<&'a str>,
}

struct UserRow {
    user_id: i64,
    username: String,
    display_name: Option<String>,
    disabled: bool,
    created_at: String,
    updated_at: String,
    password_hash: String,
}

#[derive(Debug, Clone)]
pub struct ApiToken {
    pub token_id: i64,
    pub user_id: i64,
    pub label: String,
    pub created_at: String,
    pub revoked_at: Option<String>,
}

/// Result of creating a token — includes the raw token (shown once, never stored).
pub struct CreatedToken {
    pub token: ApiToken,
    pub raw_token: String,
}

// ---------------------------------------------------------------------------
// Password hashing — Argon2id with default (strong) params
// ---------------------------------------------------------------------------

fn hash_password(password: &str) -> std::result::Result<String, password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default(); // Argon2id v19, 19 MiB, 2 iterations, 1 lane
    let hash = argon2.hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

fn verify_password(
    password: &str,
    phc_hash: &str,
) -> std::result::Result<bool, password_hash::Error> {
    let parsed = PasswordHash::new(phc_hash)?;
    match Argon2::default().verify_password(password.as_bytes(), &parsed) {
        Ok(()) => Ok(true),
        Err(password_hash::Error::Password) => Ok(false),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

pub fn create_user(conn: &Connection, new_user: &NewUser) -> Result<User> {
    let phc = hash_password(new_user.password).map_err(|e| {
        Error::InvalidParameterName(format!("password hash failed: {e}"))
    })?;

    conn.execute(
        "INSERT INTO auth.users (username, password_hash, display_name)
         VALUES (?, ?, ?)",
        params![new_user.username, phc, new_user.display_name],
    )?;

    get_user_by_username(conn, new_user.username)?
        .ok_or_else(|| Error::InvalidParameterName("user insert did not return row".to_string()))
}

pub fn get_user_by_username(conn: &Connection, username: &str) -> Result<Option<User>> {
    match conn.query_row(
        "SELECT user_id, username, display_name, disabled,
                CAST(created_at AS VARCHAR), CAST(updated_at AS VARCHAR)
         FROM auth.users WHERE username = ?",
        [username],
        |row| {
            Ok(User {
                user_id: row.get(0)?,
                username: row.get(1)?,
                display_name: row.get(2)?,
                disabled: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        },
    ) {
        Ok(user) => Ok(Some(user)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn list_users(conn: &Connection) -> Result<Vec<User>> {
    let mut stmt = conn.prepare(
        "SELECT user_id, username, display_name, disabled,
                CAST(created_at AS VARCHAR), CAST(updated_at AS VARCHAR)
         FROM auth.users ORDER BY user_id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(User {
            user_id: row.get(0)?,
            username: row.get(1)?,
            display_name: row.get(2)?,
            disabled: row.get(3)?,
            created_at: row.get(4)?,
            updated_at: row.get(5)?,
        })
    })?;
    rows.collect::<Result<Vec<_>>>()
}

pub fn disable_user(conn: &Connection, username: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE auth.users SET disabled = true, updated_at = now() WHERE username = ?",
        [username],
    )?;
    Ok(changed > 0)
}

/// Verify a username/password pair. Returns `Ok(Some(user))` on success,
/// `Ok(None)` if the user doesn't exist or the password is wrong.
pub fn authenticate(conn: &Connection, username: &str, password: &str) -> Result<Option<User>> {
    // Fetch the hash — single query so we can do constant-time comparison
    let row = match conn.query_row(
        "SELECT user_id, username, display_name, disabled,
                CAST(created_at AS VARCHAR), CAST(updated_at AS VARCHAR),
                password_hash
         FROM auth.users WHERE username = ?",
        [username],
        |row| {
            Ok(UserRow {
                user_id: row.get(0)?,
                username: row.get(1)?,
                display_name: row.get(2)?,
                disabled: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
                password_hash: row.get(6)?,
            })
        },
    ) {
        Ok(r) => r,
        Err(Error::QueryReturnedNoRows) => return Ok(None),
        Err(e) => return Err(e),
    };

    if row.disabled {
        return Ok(None);
    }

    let ok = verify_password(password, &row.password_hash).map_err(|e| {
        Error::InvalidParameterName(format!("password verify failed: {e}"))
    })?;

    if ok {
        Ok(Some(User {
            user_id: row.user_id,
            username: row.username,
            display_name: row.display_name,
            disabled: row.disabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// API token helpers
// ---------------------------------------------------------------------------

fn generate_raw_token() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hash_token(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------
// API token CRUD
// ---------------------------------------------------------------------------

/// Create a new API token for a user. Returns the raw token string (only shown once).
pub fn create_api_token(conn: &Connection, user_id: i64, label: &str) -> Result<CreatedToken> {
    let raw = generate_raw_token();
    let hashed = hash_token(&raw);

    conn.execute(
        "INSERT INTO auth.api_tokens (user_id, token_hash, label) VALUES (?, ?, ?)",
        params![user_id, hashed, label],
    )?;

    let token = conn.query_row(
        "SELECT token_id, user_id, label,
                CAST(created_at AS VARCHAR), CAST(revoked_at AS VARCHAR)
         FROM auth.api_tokens WHERE token_hash = ?",
        [&hashed],
        |row| {
            Ok(ApiToken {
                token_id: row.get(0)?,
                user_id: row.get(1)?,
                label: row.get(2)?,
                created_at: row.get(3)?,
                revoked_at: row.get(4)?,
            })
        },
    )?;

    Ok(CreatedToken {
        token,
        raw_token: raw,
    })
}

/// List all tokens for a user (active and revoked).
pub fn list_api_tokens(conn: &Connection, user_id: i64) -> Result<Vec<ApiToken>> {
    let mut stmt = conn.prepare(
        "SELECT token_id, user_id, label,
                CAST(created_at AS VARCHAR), CAST(revoked_at AS VARCHAR)
         FROM auth.api_tokens WHERE user_id = ? ORDER BY token_id ASC",
    )?;
    let rows = stmt.query_map([user_id], |row| {
        Ok(ApiToken {
            token_id: row.get(0)?,
            user_id: row.get(1)?,
            label: row.get(2)?,
            created_at: row.get(3)?,
            revoked_at: row.get(4)?,
        })
    })?;
    rows.collect::<Result<Vec<_>>>()
}

/// Revoke a token. Returns true if the token was found and revoked.
pub fn revoke_api_token(conn: &Connection, token_id: i64, user_id: i64) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE auth.api_tokens SET revoked_at = now()
         WHERE token_id = ? AND user_id = ? AND revoked_at IS NULL",
        params![token_id, user_id],
    )?;
    Ok(changed > 0)
}

/// Authenticate a raw bearer token. Returns the owning user if the token is
/// valid (exists, not revoked, user not disabled).
pub fn authenticate_token(conn: &Connection, raw_token: &str) -> Result<Option<User>> {
    let hashed = hash_token(raw_token);

    let row = match conn.query_row(
        "SELECT t.user_id, u.username, u.display_name, u.disabled,
                CAST(u.created_at AS VARCHAR), CAST(u.updated_at AS VARCHAR)
         FROM auth.api_tokens t
         JOIN auth.users u ON u.user_id = t.user_id
         WHERE t.token_hash = ? AND t.revoked_at IS NULL AND u.disabled = false",
        [&hashed],
        |row| {
            Ok(User {
                user_id: row.get(0)?,
                username: row.get(1)?,
                display_name: row.get(2)?,
                disabled: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        },
    ) {
        Ok(user) => Some(user),
        Err(Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e),
    };

    Ok(row)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        bootstrap_auth_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn create_and_authenticate() {
        let conn = setup();
        let user = create_user(
            &conn,
            &NewUser {
                username: "alice",
                password: "hunter2",
                display_name: Some("Alice"),
            },
        )
        .unwrap();

        assert_eq!(user.username, "alice");
        assert_eq!(user.display_name.as_deref(), Some("Alice"));
        assert!(!user.disabled);

        // Correct password
        let authed = authenticate(&conn, "alice", "hunter2").unwrap();
        assert!(authed.is_some());

        // Wrong password
        let bad = authenticate(&conn, "alice", "wrong").unwrap();
        assert!(bad.is_none());

        // Unknown user
        let unknown = authenticate(&conn, "bob", "hunter2").unwrap();
        assert!(unknown.is_none());
    }

    #[test]
    fn disabled_user_cannot_authenticate() {
        let conn = setup();
        create_user(
            &conn,
            &NewUser {
                username: "carol",
                password: "secret",
                display_name: None,
            },
        )
        .unwrap();

        disable_user(&conn, "carol").unwrap();

        let result = authenticate(&conn, "carol", "secret").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn duplicate_username_rejected() {
        let conn = setup();
        create_user(
            &conn,
            &NewUser {
                username: "dave",
                password: "pw1",
                display_name: None,
            },
        )
        .unwrap();

        let dup = create_user(
            &conn,
            &NewUser {
                username: "dave",
                password: "pw2",
                display_name: None,
            },
        );
        assert!(dup.is_err());
    }

    #[test]
    fn list_users_returns_all() {
        let conn = setup();
        create_user(&conn, &NewUser { username: "u1", password: "p", display_name: None }).unwrap();
        create_user(&conn, &NewUser { username: "u2", password: "p", display_name: None }).unwrap();

        let users = list_users(&conn).unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn create_and_authenticate_token() {
        let conn = setup();
        let user = create_user(&conn, &NewUser { username: "alice", password: "pw", display_name: None }).unwrap();

        let created = create_api_token(&conn, user.user_id, "test token").unwrap();
        assert_eq!(created.token.label, "test token");
        assert_eq!(created.raw_token.len(), 64); // 32 bytes hex-encoded

        let authed = authenticate_token(&conn, &created.raw_token).unwrap();
        assert!(authed.is_some());
        assert_eq!(authed.unwrap().username, "alice");

        // Wrong token
        let bad = authenticate_token(&conn, "not-a-real-token").unwrap();
        assert!(bad.is_none());
    }

    #[test]
    fn revoked_token_cannot_authenticate() {
        let conn = setup();
        let user = create_user(&conn, &NewUser { username: "bob", password: "pw", display_name: None }).unwrap();

        let created = create_api_token(&conn, user.user_id, "ephemeral").unwrap();
        revoke_api_token(&conn, created.token.token_id, user.user_id).unwrap();

        let result = authenticate_token(&conn, &created.raw_token).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn disabled_user_token_rejected() {
        let conn = setup();
        let user = create_user(&conn, &NewUser { username: "carol", password: "pw", display_name: None }).unwrap();
        let created = create_api_token(&conn, user.user_id, "my key").unwrap();

        disable_user(&conn, "carol").unwrap();

        let result = authenticate_token(&conn, &created.raw_token).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn list_tokens_for_user() {
        let conn = setup();
        let user = create_user(&conn, &NewUser { username: "dave", password: "pw", display_name: None }).unwrap();

        create_api_token(&conn, user.user_id, "first").unwrap();
        create_api_token(&conn, user.user_id, "second").unwrap();

        let tokens = list_api_tokens(&conn, user.user_id).unwrap();
        assert_eq!(tokens.len(), 2);
    }
}
