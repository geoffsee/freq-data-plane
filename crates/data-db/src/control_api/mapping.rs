use duckdb::{params, Connection, Error, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MappingType {
    Database,
    Schema,
    Table,
    View,
    Function,
}

impl MappingType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::Schema => "schema",
            Self::Table => "table",
            Self::View => "view",
            Self::Function => "function",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "database" => Ok(Self::Database),
            "schema" => Ok(Self::Schema),
            "table" => Ok(Self::Table),
            "view" => Ok(Self::View),
            "function" => Ok(Self::Function),
            other => Err(Error::InvalidParameterName(format!(
                "invalid mapping type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMapping {
    pub mapping_key: String,
    pub database_ref_id: i64,
    pub logical_schema: String,
    pub logical_object: Option<String>,
    pub physical_schema: String,
    pub physical_object: Option<String>,
    pub mapping_type: MappingType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSchemaMapping {
    pub mapping_key: String,
    pub database_ref_id: i64,
    pub logical_schema: String,
    pub logical_object: Option<String>,
    pub physical_schema: String,
    pub physical_object: Option<String>,
    pub mapping_type: MappingType,
}

pub fn create_schema_mapping(conn: &Connection, new_mapping: &NewSchemaMapping) -> Result<SchemaMapping> {
    conn.execute(
        "INSERT INTO control.schema_mappings (
            mapping_key,
            database_ref_id,
            logical_schema,
            logical_object,
            physical_schema,
            physical_object,
            mapping_type
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            new_mapping.mapping_key,
            new_mapping.database_ref_id,
            new_mapping.logical_schema,
            new_mapping.logical_object,
            new_mapping.physical_schema,
            new_mapping.physical_object,
            new_mapping.mapping_type.as_str()
        ],
    )?;
    get_schema_mapping_by_key(conn, &new_mapping.mapping_key)?.ok_or_else(|| {
        Error::InvalidParameterName("schema mapping insert did not return row".to_string())
    })
}

pub fn get_schema_mapping_by_key(conn: &Connection, mapping_key: &str) -> Result<Option<SchemaMapping>> {
    match conn.query_row(
        "SELECT mapping_key, database_ref_id, logical_schema, logical_object, physical_schema, physical_object, mapping_type
         FROM control.schema_mappings
         WHERE mapping_key = ?",
        [mapping_key],
        |row| {
            let mapping_type: String = row.get(6)?;
            Ok(SchemaMapping {
                mapping_key: row.get(0)?,
                database_ref_id: row.get(1)?,
                logical_schema: row.get(2)?,
                logical_object: row.get(3)?,
                physical_schema: row.get(4)?,
                physical_object: row.get(5)?,
                mapping_type: MappingType::parse(&mapping_type)?,
            })
        },
    ) {
        Ok(mapping) => Ok(Some(mapping)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}
