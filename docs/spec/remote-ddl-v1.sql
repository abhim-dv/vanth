-- Vanth remote execution v1 DDL.
-- Controller (local) side: remotes, durable requests, replay tombstones, shadows.
-- Remote side: accepted operations and replay tombstones.
-- Applied with the project's sqlite conventions (WAL, 30s busy timeout,
-- foreign_keys ON); reuse configure_connection() from vanth.migrations.

-- =====================================================================
-- Controller (local) side
-- =====================================================================

CREATE TABLE IF NOT EXISTS remotes (
  remote_id TEXT PRIMARY KEY,           -- rmt_ + 32 hex
  name TEXT,
  target TEXT NOT NULL,
  state TEXT NOT NULL,                  -- unpaired|pairing|paired|error
  state_epoch INTEGER NOT NULL DEFAULT 1,
  controller_id TEXT,
  credential_state TEXT,
  pairing_state TEXT,
  key_path TEXT,
  known_hosts_path TEXT,
  installed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS remote_requests (
  request_id TEXT PRIMARY KEY,          -- req_ + 32 hex
  remote_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,        -- [A-Za-z0-9_-]{8,128}
  method TEXT NOT NULL,                 -- job.start|job.stop|job.rerun
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,                 -- sha256(canonical(method,payload,key))
  status TEXT NOT NULL,                 -- creating|submitting|accepted|completed|failed|lost
  response_json TEXT,
  error_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(remote_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS remote_replay_tombstones (
  tombstone_id TEXT PRIMARY KEY,        -- tomb_ + 32 hex
  remote_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(remote_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS remote_shadows (
  shadow_id TEXT PRIMARY KEY,           -- shd_ + 32 hex
  remote_id TEXT NOT NULL,
  remote_job_id TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_remote_requests_remote ON remote_requests(remote_id, created_at);
CREATE INDEX IF NOT EXISTS idx_remote_shadows_remote ON remote_shadows(remote_id, created_at);

-- =====================================================================
-- Remote side
-- =====================================================================

CREATE TABLE IF NOT EXISTS remote_operations (
  op_id TEXT PRIMARY KEY,               -- op_ + 32 hex
  idempotency_key TEXT NOT NULL,        -- [A-Za-z0-9_-]{8,128}
  method TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  status TEXT NOT NULL,                 -- accepted|queued|launched|running|completed|failed
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(idempotency_key)
);

CREATE TABLE IF NOT EXISTS remote_replay_tombstones (
  tombstone_id TEXT PRIMARY KEY,        -- tomb_ + 32 hex
  idempotency_key TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(idempotency_key)
);
