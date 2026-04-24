-- Tenant-local DuckDB control plane
-- One DuckDB file per tenant.
--
-- Example control-plane file:
--   s3://platform-control/tenants/acme/control.duckdb
--
-- This database stores:
--   - tenant metadata
--   - asset catalog
--   - database references
--   - access policies
--   - credential / secret handles
--   - schema mappings
--   - delivery metadata

CREATE SCHEMA IF NOT EXISTS control;

CREATE SEQUENCE IF NOT EXISTS control.principal_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS control.role_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS control.asset_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS control.database_ref_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS control.delivery_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS control.audit_id_seq START 1;


-- ---------------------------------------------------------------------
-- Tenant identity
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS control.tenant_profile (
                                                      tenant_key VARCHAR PRIMARY KEY,
                                                      tenant_name VARCHAR NOT NULL,

                                                      control_plane_uri VARCHAR NOT NULL,
                                                      default_region VARCHAR,
                                                      default_bucket VARCHAR,
                                                      default_prefix VARCHAR,

                                                      status VARCHAR NOT NULL DEFAULT 'active',

                                                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (status IN ('active', 'suspended', 'deleted'))
    );


-- ---------------------------------------------------------------------
-- Principals and roles
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS control.principals (
                                                  principal_id BIGINT PRIMARY KEY DEFAULT nextval('control.principal_id_seq'),

    principal_key VARCHAR NOT NULL UNIQUE,

    principal_type VARCHAR NOT NULL,
    display_name VARCHAR,
    email VARCHAR,

    external_subject VARCHAR,
    identity_provider VARCHAR,

    status VARCHAR NOT NULL DEFAULT 'active',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (principal_type IN ('user', 'service_account', 'group', 'system')),
    CHECK (status IN ('active', 'disabled', 'deleted'))
    );

CREATE TABLE IF NOT EXISTS control.roles (
                                             role_id BIGINT PRIMARY KEY DEFAULT nextval('control.role_id_seq'),

    role_key VARCHAR NOT NULL UNIQUE,
    role_name VARCHAR NOT NULL,
    description VARCHAR,

    is_system_role BOOLEAN NOT NULL DEFAULT false,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

CREATE TABLE IF NOT EXISTS control.principal_roles (
                                                       principal_id BIGINT NOT NULL,
                                                       role_id BIGINT NOT NULL,

                                                       granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    granted_by_principal_id BIGINT,

    PRIMARY KEY (principal_id, role_id),

    FOREIGN KEY (principal_id)
    REFERENCES control.principals(principal_id),

    FOREIGN KEY (role_id)
    REFERENCES control.roles(role_id),

    FOREIGN KEY (granted_by_principal_id)
    REFERENCES control.principals(principal_id)
    );


-- ---------------------------------------------------------------------
-- Credential and secret handles
-- ---------------------------------------------------------------------
-- Store references/handles only.
-- Do not store raw AWS secret keys directly in this DuckDB file.

CREATE TABLE IF NOT EXISTS control.secret_handles (
                                                      secret_handle_id UUID PRIMARY KEY DEFAULT uuid(),

    secret_key VARCHAR NOT NULL UNIQUE,
    secret_name VARCHAR NOT NULL,

    provider VARCHAR NOT NULL,
    auth_method VARCHAR NOT NULL,

    -- Examples:
    --   arn:aws:secretsmanager:us-east-1:123456789012:secret:...
    --   arn:aws:iam::123456789012:role/TenantDuckDBAccess
    --   vault://kv/tenants/acme/s3
    --   k8s://namespace/secret-name
    external_secret_ref VARCHAR NOT NULL,

    allowed_uri_prefix VARCHAR,

    status VARCHAR NOT NULL DEFAULT 'active',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rotated_at TIMESTAMPTZ,

    CHECK (provider IN ('aws', 'minio', 'r2', 'gcs', 'azure', 'vault', 'other')),
    CHECK (auth_method IN (
           'iam_role',
           'instance_profile',
           'sts',
           'access_key',
           'oauth',
           'external_secret'
                          )),
    CHECK (status IN ('active', 'disabled', 'deleted'))
    );


-- ---------------------------------------------------------------------
-- Database references
-- ---------------------------------------------------------------------
-- These are logical databases or data roots this tenant can access.
--
-- Examples:
--   s3://tenant-data/acme/warehouse/main.duckdb
--   s3://tenant-data/acme/lake/sales/
--   s3://tenant-data/acme/iceberg/catalog/

CREATE TABLE IF NOT EXISTS control.database_refs (
                                                     database_ref_id BIGINT PRIMARY KEY DEFAULT nextval('control.database_ref_id_seq'),

    database_key VARCHAR NOT NULL UNIQUE,
    database_name VARCHAR NOT NULL,

    database_kind VARCHAR NOT NULL,

    uri VARCHAR NOT NULL UNIQUE,

    storage_backend VARCHAR NOT NULL DEFAULT 's3',
    region VARCHAR,
    bucket VARCHAR,
    prefix VARCHAR,

    default_secret_handle_id UUID,

    attach_alias VARCHAR,

    access_mode VARCHAR NOT NULL DEFAULT 'read_only',
    status VARCHAR NOT NULL DEFAULT 'active',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (default_secret_handle_id)
    REFERENCES control.secret_handles(secret_handle_id),

    CHECK (database_kind IN (
           'duckdb_file',
           'parquet_dataset',
           'csv_dataset',
           'json_dataset',
           'iceberg_catalog',
           'delta_table',
           'sqlite_file',
           'postgres_database'
                            )),

    CHECK (storage_backend IN ('s3', 'gcs', 'azure_blob', 'local', 'http')),
    CHECK (access_mode IN ('read_only', 'read_write')),
    CHECK (status IN ('active', 'archived', 'deleted'))
    );


CREATE TABLE IF NOT EXISTS control.database_ref_secrets (
                                                            database_ref_id BIGINT NOT NULL,
                                                            secret_handle_id UUID NOT NULL,

                                                            usage_scope VARCHAR NOT NULL DEFAULT 'read_write',

                                                            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (database_ref_id, secret_handle_id),

    FOREIGN KEY (database_ref_id)
    REFERENCES control.database_refs(database_ref_id),

    FOREIGN KEY (secret_handle_id)
    REFERENCES control.secret_handles(secret_handle_id),

    CHECK (usage_scope IN ('read_only', 'write_only', 'read_write', 'admin'))
    );


-- ---------------------------------------------------------------------
-- Asset catalog
-- ---------------------------------------------------------------------
-- Assets are tenant-visible data products, tables, files, views, feeds,
-- models, reports, exports, or logical resources.

CREATE TABLE IF NOT EXISTS control.assets (
                                              asset_id BIGINT PRIMARY KEY DEFAULT nextval('control.asset_id_seq'),

    asset_key VARCHAR NOT NULL UNIQUE,
    asset_name VARCHAR NOT NULL,

    asset_type VARCHAR NOT NULL,

    database_ref_id BIGINT,

    source_uri VARCHAR,
    canonical_uri VARCHAR,

    schema_name VARCHAR,
    object_name VARCHAR,

    content_format VARCHAR,

    owner_principal_id BIGINT,

    classification VARCHAR NOT NULL DEFAULT 'internal',
    status VARCHAR NOT NULL DEFAULT 'active',

    description VARCHAR,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (database_ref_id)
    REFERENCES control.database_refs(database_ref_id),

    FOREIGN KEY (owner_principal_id)
    REFERENCES control.principals(principal_id),

    CHECK (asset_type IN (
           'database',
           'schema',
           'table',
           'view',
           'file',
           'dataset',
           'feed',
           'export',
           'report',
           'model',
           'endpoint'
                         )),

    CHECK (content_format IN (
           'duckdb',
           'parquet',
           'csv',
           'json',
           'iceberg',
           'delta',
           'arrow',
           'unknown'
                             )),

    CHECK (classification IN ('public', 'internal', 'confidential', 'restricted')),
    CHECK (status IN ('active', 'deprecated', 'archived', 'deleted'))
    );


CREATE TABLE IF NOT EXISTS control.asset_tags (
                                                  asset_id BIGINT NOT NULL,
                                                  tag_key VARCHAR NOT NULL,
                                                  tag_value VARCHAR,

                                                  PRIMARY KEY (asset_id, tag_key),

    FOREIGN KEY (asset_id)
    REFERENCES control.assets(asset_id)
    );


CREATE TABLE IF NOT EXISTS control.asset_lineage (
                                                     upstream_asset_id BIGINT NOT NULL,
                                                     downstream_asset_id BIGINT NOT NULL,

                                                     lineage_type VARCHAR NOT NULL DEFAULT 'derived_from',

                                                     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (upstream_asset_id, downstream_asset_id, lineage_type),

    FOREIGN KEY (upstream_asset_id)
    REFERENCES control.assets(asset_id),

    FOREIGN KEY (downstream_asset_id)
    REFERENCES control.assets(asset_id),

    CHECK (lineage_type IN (
           'derived_from',
           'copied_from',
           'joined_with',
           'aggregated_from',
           'exported_from'
                           ))
    );


-- ---------------------------------------------------------------------
-- Schema mappings
-- ---------------------------------------------------------------------
-- Maps tenant-local logical names to physical database/schema/object names.

CREATE TABLE IF NOT EXISTS control.schema_mappings (
                                                       mapping_id UUID PRIMARY KEY DEFAULT uuid(),

    mapping_key VARCHAR NOT NULL UNIQUE,

    database_ref_id BIGINT NOT NULL,

    logical_database VARCHAR,
    logical_schema VARCHAR NOT NULL,
    logical_object VARCHAR,

    physical_database VARCHAR,
    physical_schema VARCHAR NOT NULL,
    physical_object VARCHAR,

    mapping_type VARCHAR NOT NULL DEFAULT 'schema',

    is_default BOOLEAN NOT NULL DEFAULT false,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (database_ref_id)
    REFERENCES control.database_refs(database_ref_id),

    CHECK (mapping_type IN ('database', 'schema', 'table', 'view', 'function'))
    );


-- ---------------------------------------------------------------------
-- Access policies
-- ---------------------------------------------------------------------
-- Policy can target a database_ref, asset, schema, table, view, URI prefix,
-- or operation class.

CREATE TABLE IF NOT EXISTS control.access_policies (
                                                       policy_id UUID PRIMARY KEY DEFAULT uuid(),

    policy_key VARCHAR NOT NULL UNIQUE,
    policy_name VARCHAR NOT NULL,

    effect VARCHAR NOT NULL DEFAULT 'allow',

    subject_type VARCHAR NOT NULL,
    principal_id BIGINT,
    role_id BIGINT,

    resource_type VARCHAR NOT NULL,

    database_ref_id BIGINT,
    asset_id BIGINT,

    schema_name VARCHAR,
    object_name VARCHAR,
    uri_prefix VARCHAR,

    can_attach BOOLEAN NOT NULL DEFAULT false,
    can_read BOOLEAN NOT NULL DEFAULT false,
    can_write BOOLEAN NOT NULL DEFAULT false,
    can_create BOOLEAN NOT NULL DEFAULT false,
    can_drop BOOLEAN NOT NULL DEFAULT false,
    can_admin BOOLEAN NOT NULL DEFAULT false,

    row_filter_sql VARCHAR,
    column_mask_policy VARCHAR,

    priority INTEGER NOT NULL DEFAULT 100,

    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_until TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (principal_id)
    REFERENCES control.principals(principal_id),

    FOREIGN KEY (role_id)
    REFERENCES control.roles(role_id),

    FOREIGN KEY (database_ref_id)
    REFERENCES control.database_refs(database_ref_id),

    FOREIGN KEY (asset_id)
    REFERENCES control.assets(asset_id),

    CHECK (effect IN ('allow', 'deny')),

    CHECK (subject_type IN ('principal', 'role', 'public')),

    CHECK (resource_type IN (
           'database_ref',
           'asset',
           'schema',
           'table',
           'view',
           'uri_prefix',
           'system'
                            )),

    CHECK (
              subject_type = 'public'
              OR principal_id IS NOT NULL
              OR role_id IS NOT NULL
          )
    );


-- ---------------------------------------------------------------------
-- Delivery metadata
-- ---------------------------------------------------------------------
-- Captures imports, exports, snapshots, shares, feeds, reports, and jobs.

CREATE TABLE IF NOT EXISTS control.delivery_channels (
                                                         delivery_channel_id UUID PRIMARY KEY DEFAULT uuid(),

    channel_key VARCHAR NOT NULL UNIQUE,
    channel_name VARCHAR NOT NULL,

    channel_type VARCHAR NOT NULL,

    destination_uri VARCHAR,
    secret_handle_id UUID,

    status VARCHAR NOT NULL DEFAULT 'active',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (secret_handle_id)
    REFERENCES control.secret_handles(secret_handle_id),

    CHECK (channel_type IN (
           's3',
           'http',
           'email',
           'webhook',
           'sftp',
           'api',
           'internal'
                           )),

    CHECK (status IN ('active', 'disabled', 'deleted'))
    );


CREATE TABLE IF NOT EXISTS control.deliveries (
                                                  delivery_id BIGINT PRIMARY KEY DEFAULT nextval('control.delivery_id_seq'),

    delivery_key VARCHAR NOT NULL UNIQUE,

    delivery_type VARCHAR NOT NULL,

    asset_id BIGINT,
    database_ref_id BIGINT,
    delivery_channel_id UUID,

    source_uri VARCHAR,
    destination_uri VARCHAR,

    status VARCHAR NOT NULL DEFAULT 'pending',

    requested_by_principal_id BIGINT,

    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    row_count BIGINT,
    byte_count BIGINT,

    checksum VARCHAR,
    content_format VARCHAR,

    error_message VARCHAR,

    metadata JSON,

    FOREIGN KEY (asset_id)
    REFERENCES control.assets(asset_id),

    FOREIGN KEY (database_ref_id)
    REFERENCES control.database_refs(database_ref_id),

    FOREIGN KEY (delivery_channel_id)
    REFERENCES control.delivery_channels(delivery_channel_id),

    FOREIGN KEY (requested_by_principal_id)
    REFERENCES control.principals(principal_id),

    CHECK (delivery_type IN (
           'import',
           'export',
           'snapshot',
           'share',
           'replication',
           'report',
           'publish'
                            )),

    CHECK (status IN (
           'pending',
           'running',
           'succeeded',
           'failed',
           'cancelled'
                     ))
    );


-- ---------------------------------------------------------------------
-- Query/session/audit metadata
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS control.sessions (
                                                session_id UUID PRIMARY KEY DEFAULT uuid(),

    principal_id BIGINT,

    client_name VARCHAR,
    client_version VARCHAR,
    client_ip VARCHAR,

    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ,

    metadata JSON,

    FOREIGN KEY (principal_id)
    REFERENCES control.principals(principal_id)
    );


CREATE TABLE IF NOT EXISTS control.audit_log (
                                                 audit_id BIGINT PRIMARY KEY DEFAULT nextval('control.audit_id_seq'),

    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    session_id UUID,
    principal_id BIGINT,

    action VARCHAR NOT NULL,

    resource_type VARCHAR,
    database_ref_id BIGINT,
    asset_id BIGINT,

    schema_name VARCHAR,
    object_name VARCHAR,
    uri VARCHAR,

    success BOOLEAN NOT NULL,
    error_message VARCHAR,

    query_text VARCHAR,
    metadata JSON,

    FOREIGN KEY (session_id)
    REFERENCES control.sessions(session_id),

    FOREIGN KEY (principal_id)
    REFERENCES control.principals(principal_id),

    FOREIGN KEY (database_ref_id)
    REFERENCES control.database_refs(database_ref_id),

    FOREIGN KEY (asset_id)
    REFERENCES control.assets(asset_id),

    CHECK (action IN (
           'login',
           'logout',
           'attach_database',
           'detach_database',
           'create_secret',
           'read_asset',
           'write_asset',
           'create_asset',
           'update_asset',
           'delete_asset',
           'grant',
           'revoke',
           'delivery_requested',
           'delivery_started',
           'delivery_completed',
           'delivery_failed',
           'query',
           'admin',
           'error'
                     ))
    );


-- ---------------------------------------------------------------------
-- Helpful indexes
-- ---------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_principals_key
    ON control.principals(principal_key);

CREATE INDEX IF NOT EXISTS idx_roles_key
    ON control.roles(role_key);

CREATE INDEX IF NOT EXISTS idx_principal_roles_role
    ON control.principal_roles(role_id);

CREATE INDEX IF NOT EXISTS idx_database_refs_key
    ON control.database_refs(database_key);

CREATE INDEX IF NOT EXISTS idx_database_refs_uri
    ON control.database_refs(uri);

CREATE INDEX IF NOT EXISTS idx_assets_key
    ON control.assets(asset_key);

CREATE INDEX IF NOT EXISTS idx_assets_database_ref
    ON control.assets(database_ref_id);

CREATE INDEX IF NOT EXISTS idx_assets_schema_object
    ON control.assets(schema_name, object_name);

CREATE INDEX IF NOT EXISTS idx_schema_mappings_logical
    ON control.schema_mappings(logical_schema, logical_object);

CREATE INDEX IF NOT EXISTS idx_schema_mappings_physical
    ON control.schema_mappings(physical_schema, physical_object);

CREATE INDEX IF NOT EXISTS idx_access_policies_role
    ON control.access_policies(role_id);

CREATE INDEX IF NOT EXISTS idx_access_policies_principal
    ON control.access_policies(principal_id);

CREATE INDEX IF NOT EXISTS idx_access_policies_database_ref
    ON control.access_policies(database_ref_id);

CREATE INDEX IF NOT EXISTS idx_access_policies_asset
    ON control.access_policies(asset_id);

CREATE INDEX IF NOT EXISTS idx_deliveries_asset
    ON control.deliveries(asset_id);

CREATE INDEX IF NOT EXISTS idx_deliveries_status
    ON control.deliveries(status);

CREATE INDEX IF NOT EXISTS idx_audit_log_time
    ON control.audit_log(occurred_at);

CREATE INDEX IF NOT EXISTS idx_audit_log_principal_time
    ON control.audit_log(principal_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_audit_log_asset_time
    ON control.audit_log(asset_id, occurred_at);

-- ─── Database Bindings ───────────────────────────────────────────────

CREATE SEQUENCE IF NOT EXISTS control.binding_id_seq START 1;

CREATE TABLE IF NOT EXISTS control.database_bindings (
    binding_id BIGINT PRIMARY KEY DEFAULT nextval('control.binding_id_seq'),
    database_ref_id BIGINT NOT NULL,
    deployment_key VARCHAR NOT NULL,
    binding_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (database_ref_id) REFERENCES control.database_refs(database_ref_id),
    UNIQUE (deployment_key, binding_name),
    CHECK (status IN ('active', 'disabled', 'deleted'))
);

CREATE INDEX IF NOT EXISTS idx_bindings_deployment
    ON control.database_bindings(deployment_key);

CREATE INDEX IF NOT EXISTS idx_bindings_database_ref
    ON control.database_bindings(database_ref_id);

-- ─── Applied Migrations ─────────────────────────────────────────────

CREATE SEQUENCE IF NOT EXISTS control.migration_id_seq START 1;

CREATE TABLE IF NOT EXISTS control.applied_migrations (
    migration_id BIGINT PRIMARY KEY DEFAULT nextval('control.migration_id_seq'),
    database_ref_id BIGINT NOT NULL,
    version VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    checksum VARCHAR NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    execution_time_ms BIGINT,
    success BOOLEAN NOT NULL DEFAULT true,
    error_message VARCHAR,

    FOREIGN KEY (database_ref_id) REFERENCES control.database_refs(database_ref_id),
    UNIQUE (database_ref_id, version)
);

CREATE INDEX IF NOT EXISTS idx_applied_migrations_database_ref
    ON control.applied_migrations(database_ref_id);