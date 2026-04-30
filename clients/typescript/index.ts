// Auto-generated from openapi.yaml — do not edit by hand.
// Regenerate with: deno run -A scripts/generate-client.ts (or manually sync with openapi.yaml)

// ── Enums ──

export type TenantStatus = "active" | "suspended" | "deleted";
export type PrincipalType = "user" | "service_account" | "group" | "system";
export type PrincipalStatus = "active" | "disabled" | "deleted";
export type SecretProvider = "aws" | "minio" | "r2" | "gcs" | "azure" | "vault" | "other";
export type AuthMethod = "iam_role" | "instance_profile" | "sts" | "access_key" | "oauth" | "external_secret";
export type SecretStatus = "active" | "disabled" | "deleted";
export type DatabaseKind = "blob_store" | "duckdb_file" | "parquet_dataset" | "csv_dataset" | "json_dataset" | "iceberg_catalog" | "delta_table" | "sqlite_file" | "postgres_database";
export type DatabaseRefStatus = "active" | "archived" | "deleted";
export type BindingStatus = "active" | "disabled" | "deleted";
export type AssetType = "database" | "schema" | "table" | "view" | "file" | "dataset" | "feed" | "export" | "report" | "model" | "endpoint";
export type Classification = "public" | "internal" | "confidential" | "restricted";
export type AssetStatus = "active" | "deprecated" | "archived" | "deleted";
export type MappingType = "database" | "schema" | "table" | "view" | "function";
export type PolicyEffect = "allow" | "deny";
export type SubjectType = "principal" | "role" | "public";
export type ResourceType = "database_ref" | "asset" | "schema" | "table" | "view" | "uri_prefix" | "system";
export type DeliveryChannelType = "s3" | "http" | "email" | "webhook" | "sftp" | "api" | "internal";
export type DeliveryChannelStatus = "active" | "disabled" | "deleted";
export type DeliveryType = "import" | "export" | "snapshot" | "share" | "replication" | "report" | "publish";
export type DeliveryStatus = "pending" | "running" | "succeeded" | "failed" | "cancelled";

// ── Types ──

export interface TenantProfile {
  tenant_key: string;
  tenant_name: string;
  control_plane_uri: string;
  default_region: string | null;
  default_bucket: string | null;
  default_prefix: string | null;
  status: TenantStatus;
  created_at: string;
  updated_at: string;
}

export interface Principal {
  principal_id: number;
  principal_key: string;
  principal_type: PrincipalType;
  display_name: string | null;
  email: string | null;
  status: PrincipalStatus;
}

export interface NewPrincipal {
  principal_key: string;
  principal_type: PrincipalType;
  display_name?: string | null;
  email?: string | null;
}

export interface Role {
  role_id: number;
  role_key: string;
  role_name: string;
  description: string | null;
  is_system_role: boolean;
}

export interface NewRole {
  role_key: string;
  role_name: string;
  description?: string | null;
}

export interface SecretHandle {
  secret_handle_id: string;
  secret_key: string;
  secret_name: string;
  provider: SecretProvider;
  auth_method: AuthMethod;
  external_secret_ref: string;
  allowed_uri_prefix: string | null;
  status: SecretStatus;
  created_at: string;
  rotated_at: string | null;
}

export interface NewSecretHandle {
  secret_key: string;
  secret_name: string;
  provider: SecretProvider;
  auth_method: AuthMethod;
  external_secret_ref: string;
  allowed_uri_prefix?: string | null;
}

export interface DatabaseRef {
  database_ref_id: number;
  database_key: string;
  database_name: string;
  database_kind: DatabaseKind;
  uri: string;
  attach_alias: string | null;
  status: DatabaseRefStatus;
}

export interface NewDatabaseRef {
  database_key: string;
  database_name: string;
  database_kind: DatabaseKind;
  uri: string;
  attach_alias?: string | null;
}

export interface ProvisionRequest {
  database_key: string;
  database_name: string;
  type: "duckdb" | "sqlite" | "postgres";
}

export interface DatabaseBinding {
  binding_id: number;
  database_ref_id: number;
  deployment_key: string;
  binding_name: string;
  status: BindingStatus;
  created_at: string;
  updated_at: string;
}

export interface AppliedMigration {
  migration_id: number;
  database_ref_id: number;
  version: string;
  name: string;
  checksum: string;
  applied_at: string;
  execution_time_ms: number | null;
  success: boolean;
  error_message: string | null;
}

export interface MigrationEntry {
  version: string;
  name: string;
  sql: string;
  checksum: string;
}

export interface MigrateResponse {
  applied: AppliedMigration[];
  skipped: number;
}

export interface SqlDbEntry {
  type: "duckdb" | "sqlite" | "postgres";
  binding: string;
  migrations?: MigrationEntry[];
}

export interface BlobStoreEntry {
  type: "rustfs";
  binding: string;
  uri: string;
}

export interface ConfigureResult {
  databases: DatabaseRef[];
  bindings: DatabaseBinding[];
  migrations_applied: number;
}

export interface Asset {
  asset_id: number;
  asset_key: string;
  asset_name: string;
  asset_type: AssetType;
  database_ref_id: number | null;
  owner_principal_id: number | null;
  classification: Classification;
  status: AssetStatus;
}

export interface NewAsset {
  asset_key: string;
  asset_name: string;
  asset_type: AssetType;
  database_ref_id?: number | null;
  owner_principal_id?: number | null;
  classification: Classification;
}

export interface SchemaMapping {
  mapping_key: string;
  database_ref_id: number;
  logical_schema: string;
  logical_object: string | null;
  physical_schema: string;
  physical_object: string | null;
  mapping_type: MappingType;
}

export interface NewSchemaMapping {
  mapping_key: string;
  database_ref_id: number;
  logical_schema: string;
  logical_object?: string | null;
  physical_schema: string;
  physical_object?: string | null;
  mapping_type: MappingType;
}

export interface AccessPolicy {
  policy_key: string;
  policy_name: string;
  effect: PolicyEffect;
  subject_type: SubjectType;
  principal_id: number | null;
  role_id: number | null;
  resource_type: ResourceType;
  database_ref_id: number | null;
  asset_id: number | null;
  can_read: boolean;
  can_write: boolean;
  can_admin: boolean;
}

export interface NewAccessPolicy {
  policy_key: string;
  policy_name: string;
  effect: PolicyEffect;
  subject_type: SubjectType;
  principal_id?: number | null;
  role_id?: number | null;
  resource_type: ResourceType;
  database_ref_id?: number | null;
  asset_id?: number | null;
  can_read: boolean;
  can_write: boolean;
  can_admin: boolean;
}

export interface DeliveryChannel {
  channel_key: string;
  channel_name: string;
  channel_type: DeliveryChannelType;
  destination_uri: string | null;
  status: DeliveryChannelStatus;
}

export interface NewDeliveryChannel {
  channel_key: string;
  channel_name: string;
  channel_type: DeliveryChannelType;
  destination_uri?: string | null;
}

export interface Delivery {
  delivery_id: number;
  delivery_key: string;
  delivery_type: DeliveryType;
  asset_id: number | null;
  database_ref_id: number | null;
  status: DeliveryStatus;
  error_message: string | null;
}

export interface NewDelivery {
  delivery_key: string;
  delivery_type: DeliveryType;
  asset_id?: number | null;
  database_ref_id?: number | null;
  delivery_channel_key?: string | null;
  requested_by_principal_id?: number | null;
}

export interface Session {
  session_id: string;
  principal_id: number | null;
  client_name: string | null;
}

export interface NewSession {
  principal_id?: number | null;
  client_name?: string | null;
  client_version?: string | null;
  client_ip?: string | null;
}

export interface AuditLogEntry {
  audit_id: number;
  session_id: string | null;
  principal_id: number | null;
  action: string;
  success: boolean;
  asset_id: number | null;
}

export interface NewAuditLogEntry {
  session_id?: string | null;
  principal_id?: number | null;
  action: string;
  success: boolean;
  asset_id?: number | null;
  error_message?: string | null;
}

export interface TokenInfo {
  token_id: number;
  label: string;
  created_at: string;
  revoked: boolean;
}

export interface CreateTokenResponse {
  token_id: number;
  label: string;
  raw_token: string;
}

// ── API Error ──

export class ApiError extends Error {
  constructor(
    public status: number,
    public body: { error: string },
  ) {
    super(body.error);
    this.name = "ApiError";
  }
}

// ── Client ──

export interface ClientOptions {
  baseUrl: string;
  token?: string;
}

export class FreqDataPlaneClient {
  private baseUrl: string;
  private token?: string;

  constructor(opts: ClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/$/, "");
    this.token = opts.token;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { "Content-Type": "application/json" };
    if (this.token) h["Authorization"] = `Bearer ${this.token}`;
    return h;
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: this.headers(),
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    if (res.status === 204) return undefined as T;
    const json = await res.json();
    if (!res.ok) throw new ApiError(res.status, json);
    return json as T;
  }

  // ── Health ──
  getHealth() { return this.request<{ status: string }>("GET", "/health"); }

  // ── Tokens ──
  listTokens() { return this.request<TokenInfo[]>("GET", "/api/tokens"); }
  createToken(label: string) { return this.request<CreateTokenResponse>("POST", "/api/tokens", { label }); }
  revokeToken(tokenId: number) { return this.request<void>("DELETE", `/api/tokens/${tokenId}`); }

  // ── Tenant ──
  getTenant() { return this.request<TenantProfile>("GET", "/tenant"); }
  updateTenantStatus(status: TenantStatus) { return this.request<TenantProfile>("PUT", "/tenant", { status }); }

  // ── Principals ──
  listPrincipals() { return this.request<Principal[]>("GET", "/principals"); }
  createPrincipal(principal: NewPrincipal) { return this.request<Principal>("POST", "/principals", principal); }
  getPrincipal(key: string) { return this.request<Principal>("GET", `/principals/${key}`); }

  // ── Roles ──
  createRole(role: NewRole) { return this.request<Role>("POST", "/roles", role); }
  getRole(key: string) { return this.request<Role>("GET", `/roles/${key}`); }
  listRolesForPrincipal(principalKey: string) { return this.request<Role[]>("GET", `/principals/${principalKey}/roles`); }
  grantRole(principalKey: string, roleId: number, grantedBy?: number | null) {
    return this.request<void>("POST", `/principals/${principalKey}/roles`, { role_id: roleId, granted_by_principal_id: grantedBy });
  }

  // ── Secrets ──
  createSecret(secret: NewSecretHandle) { return this.request<SecretHandle>("POST", "/secrets", secret); }
  getSecret(key: string) { return this.request<SecretHandle>("GET", `/secrets/${key}`); }

  // ── Databases ──
  listDatabases() { return this.request<DatabaseRef[]>("GET", "/databases"); }
  createDatabase(db: NewDatabaseRef) { return this.request<DatabaseRef>("POST", "/databases", db); }
  provisionDatabase(req: ProvisionRequest) { return this.request<DatabaseRef>("POST", "/databases/provision", req); }
  getDatabase(key: string) { return this.request<DatabaseRef>("GET", `/databases/${key}`); }
  archiveDatabase(key: string) { return this.request<void>("DELETE", `/databases/${key}`); }
  updateDatabaseStatus(key: string, status: DatabaseRefStatus) { return this.request<DatabaseRef>("PUT", `/databases/${key}/status`, { status }); }
  migrateDatabase(key: string, migrations: MigrationEntry[]) { return this.request<MigrateResponse>("POST", `/databases/${key}/migrate`, { migrations }); }
  listMigrations(key: string) { return this.request<AppliedMigration[]>("GET", `/databases/${key}/migrations`); }

  // ── Bindings ──
  createBinding(databaseKey: string, deploymentKey: string, bindingName: string) {
    return this.request<DatabaseBinding>("POST", `/databases/${databaseKey}/bindings`, { deployment_key: deploymentKey, binding_name: bindingName });
  }
  listBindingsForDatabase(databaseKey: string) { return this.request<DatabaseBinding[]>("GET", `/databases/${databaseKey}/bindings`); }
  listBindingsForDeployment(deploymentKey: string) { return this.request<DatabaseBinding[]>("GET", `/deployments/${deploymentKey}/bindings`); }
  deleteBinding(bindingId: number) { return this.request<void>("DELETE", `/bindings/${bindingId}`); }

  // ── Configure (orchestration) ──
  configureDeployment(deploymentKey: string, sqlDb: SqlDbEntry[] = [], blobStore: BlobStoreEntry[] = []) {
    return this.request<ConfigureResult>("POST", `/deployments/${deploymentKey}/configure`, { sql_db: sqlDb, blob_store: blobStore });
  }

  // ── Assets ──
  createAsset(asset: NewAsset) { return this.request<Asset>("POST", "/assets", asset); }
  getAsset(key: string) { return this.request<Asset>("GET", `/assets/${key}`); }

  // ── Mappings ──
  createMapping(mapping: NewSchemaMapping) { return this.request<SchemaMapping>("POST", "/mappings", mapping); }
  getMapping(key: string) { return this.request<SchemaMapping>("GET", `/mappings/${key}`); }

  // ── Policies ──
  createPolicy(policy: NewAccessPolicy) { return this.request<AccessPolicy>("POST", "/policies", policy); }
  getPolicy(key: string) { return this.request<AccessPolicy>("GET", `/policies/${key}`); }

  // ── Delivery Channels ──
  createDeliveryChannel(channel: NewDeliveryChannel) { return this.request<DeliveryChannel>("POST", "/delivery-channels", channel); }
  getDeliveryChannel(key: string) { return this.request<DeliveryChannel>("GET", `/delivery-channels/${key}`); }

  // ── Deliveries ──
  createDelivery(delivery: NewDelivery) { return this.request<Delivery>("POST", "/deliveries", delivery); }
  getDelivery(key: string) { return this.request<Delivery>("GET", `/deliveries/${key}`); }

  // ── Sessions ──
  createSession(session: NewSession) { return this.request<Session>("POST", "/sessions", session); }

  // ── Audit ──
  createAuditEntry(entry: NewAuditLogEntry) { return this.request<AuditLogEntry>("POST", "/audit", entry); }
}
