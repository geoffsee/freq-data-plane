/**
 * notes-api — A simple REST API for notes, backed by a DuckDB database
 * provisioned via the Freq data-plane.
 *
 * The database binding is configured in freq.toml:
 *   [[sql_db]]
 *   type = "duckdb"
 *   migrations = "./migrations"
 *   binding = "NOTES_DB"
 *
 * Run `deno task setup` first to provision the database and run migrations,
 * then `deno task dev` to start the API.
 */

import { FreqDataPlaneClient } from "../../clients/typescript/index.ts";

const DATA_PLANE_URL = Deno.env.get("DATA_PLANE_URL") ?? "http://localhost:3334";
const DATA_PLANE_TOKEN = Deno.env.get("DATA_PLANE_TOKEN") ?? "";
const DEPLOYMENT_KEY = "notes-api";
const PORT = parseInt(Deno.env.get("PORT") ?? "8080");

const client = new FreqDataPlaneClient({
  baseUrl: DATA_PLANE_URL,
  token: DATA_PLANE_TOKEN,
});

// Resolve bindings on startup
const bindings = await client.listBindingsForDeployment(DEPLOYMENT_KEY);
const notesBinding = bindings.find((b) => b.binding_name === "NOTES_DB");

if (!notesBinding) {
  console.error(
    "NOTES_DB binding not found. Run `deno task setup` first to provision the database.",
  );
  Deno.exit(1);
}

// Look up the database ref to get the URI
const databases = await client.listDatabases();
const notesDb = databases.find((d) => d.database_ref_id === notesBinding.database_ref_id);

if (!notesDb) {
  console.error(`Database ref ${notesBinding.database_ref_id} not found.`);
  Deno.exit(1);
}

console.log(`NOTES_DB bound to: ${notesDb.uri}`);
console.log(`Database kind: ${notesDb.database_kind}`);

// Check migrations are applied
const migrations = await client.listMigrations(notesDb.database_key);
console.log(`Migrations applied: ${migrations.length}`);
for (const m of migrations) {
  console.log(`  ${m.version} ${m.name} (${m.success ? "ok" : "FAILED"})`);
}

// ── In-memory store (simulating what the DuckDB connection would do) ──
// In a real deployment, the runtime would inject the DuckDB connection
// via env.NOTES_DB. Here we simulate with an in-memory Map.
interface Note {
  id: string;
  title: string;
  body: string;
  tags: string;
  created_at: string;
  updated_at: string;
}

const store = new Map<string, Note>();

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

async function handler(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const path = url.pathname;

  // GET /health
  if (path === "/health") {
    return json({
      status: "ok",
      service: "notes-api",
      database: notesDb!.database_key,
      binding: "NOTES_DB",
      migrations: migrations.length,
    });
  }

  // GET /notes
  if (path === "/notes" && req.method === "GET") {
    const notes = [...store.values()].sort(
      (a, b) => b.created_at.localeCompare(a.created_at),
    );
    return json(notes);
  }

  // POST /notes
  if (path === "/notes" && req.method === "POST") {
    const body = await req.json();
    const now = new Date().toISOString();
    const note: Note = {
      id: crypto.randomUUID(),
      title: body.title ?? "Untitled",
      body: body.body ?? "",
      tags: body.tags ?? "",
      created_at: now,
      updated_at: now,
    };
    store.set(note.id, note);
    return json(note, 201);
  }

  // GET /notes/:id
  const noteMatch = path.match(/^\/notes\/([a-f0-9-]+)$/);
  if (noteMatch && req.method === "GET") {
    const note = store.get(noteMatch[1]);
    if (!note) return json({ error: "not found" }, 404);
    return json(note);
  }

  // PATCH /notes/:id
  if (noteMatch && req.method === "PATCH") {
    const note = store.get(noteMatch[1]);
    if (!note) return json({ error: "not found" }, 404);
    const body = await req.json();
    if (body.title !== undefined) note.title = body.title;
    if (body.body !== undefined) note.body = body.body;
    if (body.tags !== undefined) note.tags = body.tags;
    note.updated_at = new Date().toISOString();
    return json(note);
  }

  // DELETE /notes/:id
  if (noteMatch && req.method === "DELETE") {
    store.delete(noteMatch[1]);
    return json({ deleted: true });
  }

  // GET /bindings — inspect the data-plane bindings for this deployment
  if (path === "/bindings") {
    return json({
      deployment: DEPLOYMENT_KEY,
      bindings: bindings.map((b) => ({
        binding_name: b.binding_name,
        database_ref_id: b.database_ref_id,
        status: b.status,
      })),
      databases: databases
        .filter((d) => bindings.some((b) => b.database_ref_id === d.database_ref_id))
        .map((d) => ({
          database_key: d.database_key,
          kind: d.database_kind,
          uri: d.uri,
          status: d.status,
        })),
    });
  }

  return json({ error: "not found" }, 404);
}

console.log(`\nnotes-api listening on http://localhost:${PORT}`);
Deno.serve({ port: PORT }, handler);
