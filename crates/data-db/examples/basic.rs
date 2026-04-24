use duckdb_experiment::{
    JsonEntity, bootstrap_schema, establish_connection, find_entities, insert_entity, Filter,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserCreated {
    name: String,
    plan: String,
}

impl JsonEntity for UserCreated {
    const KIND: &'static str = "user.created";
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = establish_connection()?;
    bootstrap_schema(&conn)?;

    insert_entity(
        &conn,
        &UserCreated {
            name: "Will".to_string(),
            plan: "pro".to_string(),
        },
    )?;
    insert_entity(
        &conn,
        &UserCreated {
            name: "Alex".to_string(),
            plan: "free".to_string(),
        },
    )?;

    let created = find_entities::<UserCreated>(&conn, Filter::All)?;
    for record in created {
        println!("{} {} {:?}", record.id, record.kind, record.payload);
    }

    let pro_users = find_entities::<UserCreated>(&conn, Filter::json_eq("$.plan", "pro"))?;
    for record in pro_users {
        println!("pro user: {} {}", record.id, record.payload.name);
    }

    Ok(())
}
