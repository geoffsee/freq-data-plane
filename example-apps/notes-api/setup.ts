/**
 * setup.ts — Pre-deployment script that reads freq.toml and calls
 * the data-plane configure endpoint to provision databases, run
 * migrations, and create bindings.
 *
 * Usage:
 *   DATA_PLANE_URL=http://localhost:3334 \
 *   DATA_PLANE_TOKEN=<your-token> \
 *   deno run -A setup.ts
 */

import { FreqDataPlaneClient } from "../../clients/typescript/index.ts";
import { parse as parseToml } from "https://deno.land/std@0.224.0/toml/mod.ts";
import { crypto } from "https://deno.land/std@0.224.0/crypto/mod.ts";
import { encodeHex } from "https://deno.land/std@0.224.0/encoding/hex.ts";

const DATA_PLANE_URL = Deno.env.get("DATA_PLANE_URL") ?? "http://localhost:3334";
const DATA_PLANE_TOKEN = Deno.env.get("DATA_PLANE_TOKEN");

if (!DATA_PLANE_TOKEN) {
  console.error("DATA_PLANE_TOKEN is required. Generate one at the data-plane dashboard.");
  Deno.exit(1);
}

// 1. Parse freq.toml
const raw = await Deno.readTextFile(new URL("./freq.toml", import.meta.url));
const config = parseToml(raw) as {
  name: string;
  sql_db?: Array<{
    type: string;
    migrations?: string;
    binding: string;
  }>;
  blob_store?: Array<{
    type: string;
    binding: string;
    uri: string;
  }>;
};

console.log(`App: ${config.name}`);

if (!config.sql_db?.length && !config.blob_store?.length) {
  console.log("No [[sql_db]] or [[blob_store]] entries — nothing to configure.");
  Deno.exit(0);
}

// 2. For each sql_db entry, read migration files and compute checksums
const sqlDbEntries = [];

for (const db of config.sql_db) {
  console.log(`\n  Database: ${db.binding} (${db.type})`);

  const migrations: Array<{
    version: string;
    name: string;
    sql: string;
    checksum: string;
  }> = [];

  if (db.migrations) {
    const migrationsDir = new URL(db.migrations + "/", import.meta.url);

    const files: string[] = [];
    for await (const entry of Deno.readDir(migrationsDir)) {
      if (entry.isFile && entry.name.endsWith(".sql")) {
        files.push(entry.name);
      }
    }
    files.sort();

    for (const filename of files) {
      // Parse v0.0.name.sql -> version + name
      const match = filename.match(/^(v\d+\.\d+)\.(.+)\.sql$/);
      if (!match) {
        console.warn(`    Skipping ${filename} — doesn't match v{major}.{minor}.{name}.sql`);
        continue;
      }

      const [, version, name] = match;
      const sql = await Deno.readTextFile(new URL(filename, migrationsDir));
      const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(sql));
      const checksum = encodeHex(new Uint8Array(hash)).slice(0, 16);

      console.log(`    Migration ${version} ${name} (${checksum})`);
      migrations.push({ version, name, sql, checksum });
    }
  }

  sqlDbEntries.push({
    type: db.type as "duckdb" | "sqlite" | "postgres",
    binding: db.binding,
    migrations,
  });
}

const blobStoreEntries = [];
for (const blob of config.blob_store ?? []) {
  console.log(`\n  Blob Store: ${blob.binding} (${blob.type}) -> ${blob.uri}`);
  blobStoreEntries.push({
    type: blob.type as "rustfs",
    binding: blob.binding,
    uri: blob.uri,
  });
}

// 3. Call the configure endpoint
const client = new FreqDataPlaneClient({
  baseUrl: DATA_PLANE_URL,
  token: DATA_PLANE_TOKEN,
});

console.log(`\nConfiguring deployment "${config.name}"...`);

const result = await client.configureDeployment(config.name, sqlDbEntries, blobStoreEntries);

console.log(`\nDatabases provisioned: ${result.databases.length}`);
for (const db of result.databases) {
  console.log(`  ${db.database_key} (${db.database_kind}) -> ${db.uri}`);
}

console.log(`Bindings created: ${result.bindings.length}`);
for (const b of result.bindings) {
  console.log(`  ${b.binding_name} -> database_ref_id=${b.database_ref_id}`);
}

console.log(`Migrations applied: ${result.migrations_applied}`);
console.log("\nSetup complete.");
