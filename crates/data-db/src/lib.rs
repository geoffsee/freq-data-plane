use duckdb::{
    params_from_iter,
    params,
    types::{Type, Value as DuckValue},
    Connection, Error, Result,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

pub mod control_api;

#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    pub id: i64,
    pub kind: String,
    pub payload: Value,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TypedRecord<T> {
    pub id: i64,
    pub kind: String,
    pub payload: T,
}

pub trait JsonEntity: Serialize + DeserializeOwned {
    const KIND: &'static str;
}

pub trait Entity: JsonEntity {}
impl<T: JsonEntity> Entity for T {}

pub enum Filter {
    All,
    JsonEq { path: String, value: String },
}

impl Filter {
    pub fn json_eq(path: &str, value: &str) -> Self {
        Self::JsonEq {
            path: path.to_string(),
            value: value.to_string(),
        }
    }
}

pub trait QuerySpec<T: JsonEntity> {
    fn where_clause(&self) -> &str;
    fn bindings(&self) -> Vec<DuckValue>;
}

struct FilterSpec {
    where_clause: String,
    bindings: Vec<DuckValue>,
}

impl<T: JsonEntity> QuerySpec<T> for FilterSpec {
    fn where_clause(&self) -> &str {
        &self.where_clause
    }

    fn bindings(&self) -> Vec<DuckValue> {
        self.bindings.clone()
    }
}

pub fn establish_connection() -> Result<Connection> {
    Connection::open_in_memory()
}

pub fn bootstrap_schema(conn: &Connection) -> Result<usize> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS records (
            id BIGINT PRIMARY KEY,
            kind TEXT NOT NULL,
            payload JSON NOT NULL
        )",
        [],
    )
}

pub fn create_record(conn: &Connection, kind: &str, payload: &Value) -> Result<usize> {
    conn.execute(
        "INSERT INTO records (id, kind, payload)
         VALUES (
            COALESCE((SELECT MAX(id) + 1 FROM records), 1),
            ?,
            CAST(? AS JSON)
         )",
        params![kind, payload.to_string()],
    )
}

pub fn create_typed_record<T: Serialize>(conn: &Connection, kind: &str, payload: &T) -> Result<usize> {
    let payload_value =
        serde_json::to_value(payload).map_err(|e| Error::ToSqlConversionFailure(Box::new(e)))?;
    create_record(conn, kind, &payload_value)
}

pub fn create_entity<T: JsonEntity>(conn: &Connection, payload: &T) -> Result<usize> {
    create_typed_record(conn, T::KIND, payload)
}

pub fn insert_entity<T: Entity>(conn: &Connection, payload: &T) -> Result<usize> {
    create_entity(conn, payload)
}

pub fn list_records(conn: &Connection) -> Result<Vec<Record>> {
    let mut statement =
        conn.prepare("SELECT id, kind, CAST(payload AS VARCHAR) FROM records ORDER BY id ASC")?;
    let rows = statement.query_map([], |row| {
        let payload_text: String = row.get(2)?;
        let payload = serde_json::from_str::<Value>(&payload_text)
            .map_err(|e| Error::FromSqlConversionFailure(2, Type::Text, Box::new(e)))?;
        Ok(Record {
            id: row.get(0)?,
            kind: row.get(1)?,
            payload,
        })
    })?;

    rows.collect::<Result<Vec<_>>>()
}

pub fn list_records_by_kind(conn: &Connection, kind: &str) -> Result<Vec<Record>> {
    let mut statement = conn.prepare(
        "SELECT id, kind, CAST(payload AS VARCHAR)
         FROM records
         WHERE kind = ?
         ORDER BY id ASC",
    )?;
    let rows = statement.query_map([kind], |row| {
        let payload_text: String = row.get(2)?;
        let payload = serde_json::from_str::<Value>(&payload_text)
            .map_err(|e| Error::FromSqlConversionFailure(2, Type::Text, Box::new(e)))?;
        Ok(Record {
            id: row.get(0)?,
            kind: row.get(1)?,
            payload,
        })
    })?;

    rows.collect::<Result<Vec<_>>>()
}

pub fn list_typed_records_by_kind<T: DeserializeOwned>(
    conn: &Connection,
    kind: &str,
) -> Result<Vec<TypedRecord<T>>> {
    let records = list_records_by_kind(conn, kind)?;
    records
        .into_iter()
        .map(|record| {
            let payload = serde_json::from_value::<T>(record.payload)
                .map_err(|e| Error::FromSqlConversionFailure(2, Type::Text, Box::new(e)))?;
            Ok(TypedRecord {
                id: record.id,
                kind: record.kind,
                payload,
            })
        })
        .collect::<Result<Vec<_>>>()
}

pub fn list_entities<T: JsonEntity>(conn: &Connection) -> Result<Vec<TypedRecord<T>>> {
    list_typed_records_by_kind::<T>(conn, T::KIND)
}

pub fn find_entities<T: Entity>(conn: &Connection, filter: Filter) -> Result<Vec<TypedRecord<T>>> {
    match filter {
        Filter::All => list_entities::<T>(conn),
        Filter::JsonEq { path, value } => {
            if !path.starts_with("$.") || path.contains('\'') {
                return Err(Error::InvalidParameterName(
                    "json path must start with '$.' and not contain single quotes".to_string(),
                ));
            }
            let spec = FilterSpec {
                where_clause: format!("json_extract_string(payload, '{}') = ?", path),
                bindings: vec![DuckValue::from(value)],
            };
            query_entities(conn, &spec)
        }
    }
}

pub fn query_entities<T, S>(conn: &Connection, spec: &S) -> Result<Vec<TypedRecord<T>>>
where
    T: JsonEntity,
    S: QuerySpec<T>,
{
    let sql = format!(
        "SELECT id, kind, CAST(payload AS VARCHAR)
         FROM records
         WHERE kind = ? AND {}
         ORDER BY id ASC",
        spec.where_clause()
    );
    let mut bindings = vec![DuckValue::from(T::KIND.to_string())];
    bindings.extend(spec.bindings());

    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(bindings.iter()), |row| {
        let payload_text: String = row.get(2)?;
        let payload = serde_json::from_str::<T>(&payload_text)
            .map_err(|e| Error::FromSqlConversionFailure(2, Type::Text, Box::new(e)))?;
        Ok(TypedRecord {
            id: row.get(0)?,
            kind: row.get(1)?,
            payload,
        })
    })?;

    rows.collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct UserCreated {
        name: String,
        plan: String,
    }

    impl JsonEntity for UserCreated {
        const KIND: &'static str = "user.created";
    }

    #[test]
    fn insert_and_read_json_records() {
        let conn = establish_connection().expect("in-memory duckdb connection should open");
        bootstrap_schema(&conn).expect("schema should be created");

        create_record(&conn, "user.created", &json!({"name": "Will", "plan": "pro"}))
            .expect("insert should succeed");
        create_record(&conn, "user.created", &json!({"name": "Alex", "plan": "free"}))
            .expect("insert should succeed");

        assert_eq!(
            list_records_by_kind(&conn, "user.created").expect("query should succeed"),
            vec![
                Record {
                    id: 1,
                    kind: "user.created".to_string(),
                    payload: json!({"name": "Will", "plan": "pro"})
                },
                Record {
                    id: 2,
                    kind: "user.created".to_string(),
                    payload: json!({"name": "Alex", "plan": "free"})
                }
            ]
        );
        assert_eq!(
            list_records(&conn).expect("list should succeed")[0],
            Record {
                id: 1,
                kind: "user.created".to_string(),
                payload: json!({"name": "Will", "plan": "pro"})
            }
        );
    }

    #[test]
    fn generic_entity_traits_work_for_typed_payloads() {
        let conn = establish_connection().expect("in-memory duckdb connection should open");
        bootstrap_schema(&conn).expect("schema should be created");

        insert_entity(
            &conn,
            &UserCreated {
                name: "Will".to_string(),
                plan: "pro".to_string(),
            },
        )
        .expect("typed insert should succeed");

        let entities = list_entities::<UserCreated>(&conn).expect("typed list should succeed");
        assert_eq!(entities.len(), 1);
        assert_eq!(
            entities[0].payload,
            UserCreated {
                name: "Will".to_string(),
                plan: "pro".to_string()
            }
        );
    }

    #[test]
    fn query_spec_filters_entities_by_json_field() {
        let conn = establish_connection().expect("in-memory duckdb connection should open");
        bootstrap_schema(&conn).expect("schema should be created");

        insert_entity(
            &conn,
            &UserCreated {
                name: "Will".to_string(),
                plan: "pro".to_string(),
            },
        )
        .expect("typed insert should succeed");
        insert_entity(
            &conn,
            &UserCreated {
                name: "Alex".to_string(),
                plan: "free".to_string(),
            },
        )
        .expect("typed insert should succeed");

        let pro_users =
            find_entities::<UserCreated>(&conn, Filter::json_eq("$.plan", "pro"))
                .expect("spec query should succeed");

        assert_eq!(pro_users.len(), 1);
        assert_eq!(pro_users[0].payload.name, "Will");
    }
}
