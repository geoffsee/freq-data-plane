use duckdb::Connection;

pub use data_db::control_api::{
    // Tenant
    TenantProfile, NewTenantProfile, TenantStatus,
    // Principal & Role
    Principal, NewPrincipal, PrincipalType, PrincipalStatus,
    Role, NewRole,
    // Secret
    SecretHandle, NewSecretHandle, SecretProvider, AuthMethod, SecretStatus,
    // Database
    DatabaseRef, NewDatabaseRef, DatabaseKind, DatabaseRefStatus,
    // Bindings
    DatabaseBinding, NewDatabaseBinding, BindingStatus,
    // Migrations
    AppliedMigration, NewAppliedMigration,
    parse_migration_filename, record_migration_result,
    // Asset
    Asset, NewAsset, AssetType, AssetStatus, Classification,
    // Schema mapping
    SchemaMapping, NewSchemaMapping, MappingType,
    // Access policy
    AccessPolicy, NewAccessPolicy, PolicyEffect, SubjectType, ResourceType,
    // Delivery
    DeliveryChannel, NewDeliveryChannel, DeliveryChannelType, DeliveryChannelStatus,
    Delivery, NewDelivery, DeliveryType, DeliveryStatus,
    // Session & Audit
    Session, NewSession,
    AuditLogEntry, NewAuditLogEntry,
};

#[derive(Debug)]
pub enum Error {
    Db(duckdb::Error),
    NotFound { entity: &'static str, key: String },
}

impl From<duckdb::Error> for Error {
    fn from(err: duckdb::Error) -> Self {
        Self::Db(err)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "database error: {e}"),
            Self::NotFound { entity, key } => write!(f, "{entity} not found: {key}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Db(e) => Some(e),
            Self::NotFound { .. } => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct ControlPlane {
    conn: Connection,
}

impl ControlPlane {
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        enable_required_extensions(&conn)?;
        data_db::control_api::bootstrap_control_schema(&conn)?;
        Ok(Self { conn })
    }

    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        enable_required_extensions(&conn)?;
        data_db::control_api::bootstrap_control_schema(&conn)?;
        Ok(Self { conn })
    }

    /// Open an encrypted DuckDB file using AES-GCM-256 (DuckDB 1.4.2+).
    /// Creates the file encrypted if it doesn't exist.
    pub fn open_encrypted(path: &str, key: &str) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("LOAD '/app/duckdb_extensions/httpfs.duckdb_extension';")?;
        let safe_path = path.replace('\'', "''");
        let safe_key = key.replace('\'', "''");
        conn.execute_batch(&format!(
            "ATTACH '{safe_path}' AS enc_db (ENCRYPTION_KEY '{safe_key}'); USE enc_db;"
        ))?;
        enable_required_extensions(&conn)?;
        data_db::control_api::bootstrap_control_schema(&conn)?;
        Ok(Self { conn })
    }

    pub fn from_connection(conn: Connection) -> Result<Self> {
        enable_required_extensions(&conn)?;
        data_db::control_api::bootstrap_control_schema(&conn)?;
        Ok(Self { conn })
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    // -- Tenant --

    pub fn create_tenant(&self, tenant: &NewTenantProfile) -> Result<TenantProfile> {
        Ok(data_db::control_api::create_tenant_profile(&self.conn, tenant)?)
    }

    pub fn get_tenant(&self, tenant_key: &str) -> Result<Option<TenantProfile>> {
        Ok(data_db::control_api::get_tenant_profile(&self.conn, tenant_key)?)
    }

    pub fn require_tenant(&self, tenant_key: &str) -> Result<TenantProfile> {
        self.get_tenant(tenant_key)?.ok_or_else(|| Error::NotFound {
            entity: "tenant",
            key: tenant_key.to_string(),
        })
    }

    pub fn list_tenants(&self) -> Result<Vec<TenantProfile>> {
        Ok(data_db::control_api::list_tenant_profiles(&self.conn)?)
    }

    pub fn update_tenant_status(&self, tenant_key: &str, status: TenantStatus) -> Result<usize> {
        Ok(data_db::control_api::update_tenant_status(&self.conn, tenant_key, status)?)
    }

    // -- Principal --

    pub fn create_principal(&self, principal: &NewPrincipal) -> Result<Principal> {
        Ok(data_db::control_api::create_principal(&self.conn, principal)?)
    }

    pub fn get_principal(&self, principal_key: &str) -> Result<Option<Principal>> {
        Ok(data_db::control_api::get_principal_by_key(&self.conn, principal_key)?)
    }

    pub fn require_principal(&self, principal_key: &str) -> Result<Principal> {
        self.get_principal(principal_key)?.ok_or_else(|| Error::NotFound {
            entity: "principal",
            key: principal_key.to_string(),
        })
    }

    pub fn list_principals(&self) -> Result<Vec<Principal>> {
        Ok(data_db::control_api::list_principals(&self.conn)?)
    }

    // -- Role --

    pub fn create_role(&self, role: &NewRole) -> Result<Role> {
        Ok(data_db::control_api::create_role(&self.conn, role)?)
    }

    pub fn get_role(&self, role_key: &str) -> Result<Option<Role>> {
        Ok(data_db::control_api::get_role_by_key(&self.conn, role_key)?)
    }

    pub fn require_role(&self, role_key: &str) -> Result<Role> {
        self.get_role(role_key)?.ok_or_else(|| Error::NotFound {
            entity: "role",
            key: role_key.to_string(),
        })
    }

    pub fn grant_role(
        &self,
        principal_id: i64,
        role_id: i64,
        granted_by: Option<i64>,
    ) -> Result<usize> {
        Ok(data_db::control_api::grant_role_to_principal(
            &self.conn,
            principal_id,
            role_id,
            granted_by,
        )?)
    }

    pub fn list_roles_for_principal(&self, principal_id: i64) -> Result<Vec<Role>> {
        Ok(data_db::control_api::list_roles_for_principal(&self.conn, principal_id)?)
    }

    // -- Secret --

    pub fn create_secret(&self, secret: &NewSecretHandle) -> Result<SecretHandle> {
        Ok(data_db::control_api::create_secret_handle(&self.conn, secret)?)
    }

    pub fn get_secret(&self, secret_key: &str) -> Result<Option<SecretHandle>> {
        Ok(data_db::control_api::get_secret_handle_by_key(&self.conn, secret_key)?)
    }

    pub fn require_secret(&self, secret_key: &str) -> Result<SecretHandle> {
        self.get_secret(secret_key)?.ok_or_else(|| Error::NotFound {
            entity: "secret",
            key: secret_key.to_string(),
        })
    }

    pub fn list_secrets(&self) -> Result<Vec<SecretHandle>> {
        Ok(data_db::control_api::list_secret_handles(&self.conn)?)
    }

    // -- Database Ref --

    pub fn create_database_ref(&self, db_ref: &NewDatabaseRef) -> Result<DatabaseRef> {
        Ok(data_db::control_api::create_database_ref(&self.conn, db_ref)?)
    }

    pub fn get_database_ref(&self, database_key: &str) -> Result<Option<DatabaseRef>> {
        Ok(data_db::control_api::get_database_ref_by_key(&self.conn, database_key)?)
    }

    pub fn require_database_ref(&self, database_key: &str) -> Result<DatabaseRef> {
        self.get_database_ref(database_key)?.ok_or_else(|| Error::NotFound {
            entity: "database_ref",
            key: database_key.to_string(),
        })
    }

    pub fn list_database_refs(&self) -> Result<Vec<DatabaseRef>> {
        Ok(data_db::control_api::list_database_refs(&self.conn)?)
    }

    pub fn update_database_ref_status(&self, database_key: &str, status: DatabaseRefStatus) -> Result<usize> {
        Ok(data_db::control_api::update_database_ref_status(&self.conn, database_key, status)?)
    }

    // -- Bindings --

    pub fn create_database_binding(&self, binding: &NewDatabaseBinding) -> Result<DatabaseBinding> {
        Ok(data_db::control_api::create_database_binding(&self.conn, binding)?)
    }

    pub fn list_bindings_for_deployment(&self, deployment_key: &str) -> Result<Vec<DatabaseBinding>> {
        Ok(data_db::control_api::list_bindings_for_deployment(&self.conn, deployment_key)?)
    }

    pub fn list_bindings_for_database(&self, database_ref_id: i64) -> Result<Vec<DatabaseBinding>> {
        Ok(data_db::control_api::list_bindings_for_database(&self.conn, database_ref_id)?)
    }

    pub fn delete_database_binding(&self, binding_id: i64) -> Result<usize> {
        Ok(data_db::control_api::delete_database_binding(&self.conn, binding_id)?)
    }

    // -- Migrations --

    pub fn record_applied_migration(&self, migration: &NewAppliedMigration) -> Result<AppliedMigration> {
        Ok(data_db::control_api::record_applied_migration(&self.conn, migration)?)
    }

    pub fn list_applied_migrations(&self, database_ref_id: i64) -> Result<Vec<AppliedMigration>> {
        Ok(data_db::control_api::list_applied_migrations(&self.conn, database_ref_id)?)
    }

    // -- Asset --

    pub fn create_asset(&self, asset: &NewAsset) -> Result<Asset> {
        Ok(data_db::control_api::create_asset(&self.conn, asset)?)
    }

    pub fn get_asset(&self, asset_key: &str) -> Result<Option<Asset>> {
        Ok(data_db::control_api::get_asset_by_key(&self.conn, asset_key)?)
    }

    pub fn require_asset(&self, asset_key: &str) -> Result<Asset> {
        self.get_asset(asset_key)?.ok_or_else(|| Error::NotFound {
            entity: "asset",
            key: asset_key.to_string(),
        })
    }

    pub fn list_assets(&self) -> Result<Vec<Asset>> {
        Ok(data_db::control_api::list_assets(&self.conn)?)
    }

    // -- Schema Mapping --

    pub fn create_schema_mapping(&self, mapping: &NewSchemaMapping) -> Result<SchemaMapping> {
        Ok(data_db::control_api::create_schema_mapping(&self.conn, mapping)?)
    }

    pub fn get_schema_mapping(&self, mapping_key: &str) -> Result<Option<SchemaMapping>> {
        Ok(data_db::control_api::get_schema_mapping_by_key(&self.conn, mapping_key)?)
    }

    pub fn require_schema_mapping(&self, mapping_key: &str) -> Result<SchemaMapping> {
        self.get_schema_mapping(mapping_key)?.ok_or_else(|| Error::NotFound {
            entity: "schema_mapping",
            key: mapping_key.to_string(),
        })
    }

    pub fn list_schema_mappings(&self) -> Result<Vec<SchemaMapping>> {
        Ok(data_db::control_api::list_schema_mappings(&self.conn)?)
    }

    // -- Access Policy --

    pub fn create_access_policy(&self, policy: &NewAccessPolicy) -> Result<AccessPolicy> {
        Ok(data_db::control_api::create_access_policy(&self.conn, policy)?)
    }

    pub fn get_access_policy(&self, policy_key: &str) -> Result<Option<AccessPolicy>> {
        Ok(data_db::control_api::get_access_policy_by_key(&self.conn, policy_key)?)
    }

    pub fn require_access_policy(&self, policy_key: &str) -> Result<AccessPolicy> {
        self.get_access_policy(policy_key)?.ok_or_else(|| Error::NotFound {
            entity: "access_policy",
            key: policy_key.to_string(),
        })
    }

    pub fn list_access_policies(&self) -> Result<Vec<AccessPolicy>> {
        Ok(data_db::control_api::list_access_policies(&self.conn)?)
    }

    // -- Delivery --

    pub fn create_delivery_channel(&self, channel: &NewDeliveryChannel) -> Result<DeliveryChannel> {
        Ok(data_db::control_api::create_delivery_channel(&self.conn, channel)?)
    }

    pub fn get_delivery_channel(&self, channel_key: &str) -> Result<Option<DeliveryChannel>> {
        Ok(data_db::control_api::get_delivery_channel_by_key(&self.conn, channel_key)?)
    }

    pub fn create_delivery(&self, delivery: &NewDelivery) -> Result<Delivery> {
        Ok(data_db::control_api::create_delivery(&self.conn, delivery)?)
    }

    pub fn get_delivery(&self, delivery_key: &str) -> Result<Option<Delivery>> {
        Ok(data_db::control_api::get_delivery_by_key(&self.conn, delivery_key)?)
    }

    pub fn require_delivery_channel(&self, channel_key: &str) -> Result<DeliveryChannel> {
        self.get_delivery_channel(channel_key)?.ok_or_else(|| Error::NotFound {
            entity: "delivery_channel",
            key: channel_key.to_string(),
        })
    }

    pub fn require_delivery(&self, delivery_key: &str) -> Result<Delivery> {
        self.get_delivery(delivery_key)?.ok_or_else(|| Error::NotFound {
            entity: "delivery",
            key: delivery_key.to_string(),
        })
    }

    pub fn list_delivery_channels(&self) -> Result<Vec<DeliveryChannel>> {
        Ok(data_db::control_api::list_delivery_channels(&self.conn)?)
    }

    pub fn list_deliveries(&self) -> Result<Vec<Delivery>> {
        Ok(data_db::control_api::list_deliveries(&self.conn)?)
    }

    // -- Session --

    pub fn create_session(&self, session: &NewSession) -> Result<Session> {
        Ok(data_db::control_api::create_session(&self.conn, session)?)
    }

    // -- Audit --

    pub fn create_audit_entry(&self, entry: &NewAuditLogEntry) -> Result<AuditLogEntry> {
        Ok(data_db::control_api::create_audit_log_entry(&self.conn, entry)?)
    }

    pub fn list_audit_entries(&self, limit: i64) -> Result<Vec<AuditLogEntry>> {
        Ok(data_db::control_api::list_audit_log_entries(&self.conn, limit)?)
    }
}

fn enable_required_extensions(conn: &Connection) -> std::result::Result<(), duckdb::Error> {
    // The control-plane schema uses JSON columns, which require DuckDB's json extension.
    conn.execute_batch(
        "SET extension_directory = '/app/duckdb_extensions';
         SET autoinstall_known_extensions = true;
         SET autoload_known_extensions = true;
         LOAD json;",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_plane_full_workflow() {
        let cp = ControlPlane::open_in_memory().expect("should open in-memory control plane");

        // Bootstrap tenant
        let tenant = cp
            .create_tenant(&NewTenantProfile {
                tenant_key: "acme".to_string(),
                tenant_name: "Acme Corp".to_string(),
                control_plane_uri: "s3://platform-control/tenants/acme/control.duckdb".to_string(),
                default_region: Some("us-east-1".to_string()),
                default_bucket: Some("tenant-data".to_string()),
                default_prefix: Some("acme/".to_string()),
            })
            .expect("create tenant");
        assert_eq!(tenant.status, TenantStatus::Active);

        // Create principal and role, then grant
        let principal = cp
            .create_principal(&NewPrincipal {
                principal_key: "user:alice@acme.com".to_string(),
                principal_type: PrincipalType::User,
                display_name: Some("Alice".to_string()),
                email: Some("alice@acme.com".to_string()),
            })
            .expect("create principal");

        let role = cp
            .create_role(&NewRole {
                role_key: "analyst".to_string(),
                role_name: "Analyst".to_string(),
                description: Some("Can read analytical assets".to_string()),
            })
            .expect("create role");

        cp.grant_role(principal.principal_id, role.role_id, None)
            .expect("grant role");

        let roles = cp
            .list_roles_for_principal(principal.principal_id)
            .expect("list roles");
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].role_key, "analyst");

        // Register database ref and asset
        let db_ref = cp
            .create_database_ref(&NewDatabaseRef {
                database_key: "warehouse".to_string(),
                database_name: "Tenant Warehouse".to_string(),
                database_kind: DatabaseKind::DuckdbFile,
                uri: "s3://tenant-data/acme/warehouse/main.duckdb".to_string(),
                attach_alias: Some("warehouse".to_string()),
            })
            .expect("create database ref");

        let asset = cp
            .create_asset(&NewAsset {
                asset_key: "sales.orders".to_string(),
                asset_name: "Sales Orders".to_string(),
                asset_type: AssetType::Table,
                database_ref_id: Some(db_ref.database_ref_id),
                owner_principal_id: Some(principal.principal_id),
                classification: Classification::Confidential,
            })
            .expect("create asset");

        // Create access policy
        cp.create_access_policy(&NewAccessPolicy {
            policy_key: "allow-analyst-read-sales".to_string(),
            policy_name: "Allow analysts to read sales orders".to_string(),
            effect: PolicyEffect::Allow,
            subject_type: SubjectType::Role,
            principal_id: None,
            role_id: Some(role.role_id),
            resource_type: ResourceType::Asset,
            database_ref_id: None,
            asset_id: Some(asset.asset_id),
            can_read: true,
            can_write: false,
            can_admin: false,
        })
        .expect("create policy");

        // Verify require_ helpers
        let loaded = cp.require_tenant("acme").expect("require tenant");
        assert_eq!(loaded.tenant_name, "Acme Corp");

        let err = cp.require_tenant("nonexistent");
        assert!(matches!(err, Err(Error::NotFound { entity: "tenant", .. })));
    }

    #[test]
    fn session_and_audit_workflow() {
        let cp = ControlPlane::open_in_memory().expect("should open");

        let principal = cp
            .create_principal(&NewPrincipal {
                principal_key: "service:gateway".to_string(),
                principal_type: PrincipalType::ServiceAccount,
                display_name: Some("Gateway".to_string()),
                email: None,
            })
            .expect("create principal");

        let session = cp
            .create_session(&NewSession {
                principal_id: Some(principal.principal_id),
                client_name: Some("query-gateway".to_string()),
                client_version: Some("0.1.0".to_string()),
                client_ip: Some("10.0.0.1".to_string()),
            })
            .expect("create session");

        let audit = cp
            .create_audit_entry(&NewAuditLogEntry {
                session_id: Some(session.session_id.clone()),
                principal_id: Some(principal.principal_id),
                action: "query".to_string(),
                success: true,
                asset_id: None,
                error_message: None,
            })
            .expect("create audit entry");

        assert!(audit.success);
        assert_eq!(audit.action, "query");
    }
}
