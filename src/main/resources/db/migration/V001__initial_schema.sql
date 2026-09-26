-- V001: FloatingPod 可恢复状态的初始 SQLite schema。

CREATE TABLE IF NOT EXISTS floating_pod_state (
  uuid BLOB PRIMARY KEY NOT NULL,
  owner_uuid BLOB,
  state_version INTEGER NOT NULL DEFAULT 0,
  world_name TEXT NOT NULL,
  pos_x REAL NOT NULL,
  pos_y REAL NOT NULL,
  pos_z REAL NOT NULL,
  yaw REAL NOT NULL,
  pitch REAL NOT NULL,
  is_active INTEGER NOT NULL,
  last_updated INTEGER NOT NULL,
  CHECK(length(uuid) = 16),
  CHECK(owner_uuid IS NULL OR length(owner_uuid) = 16),
  CHECK(state_version >= 0),
  CHECK(is_active IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_floating_pod_state_owner_uuid ON floating_pod_state(owner_uuid);
CREATE INDEX IF NOT EXISTS idx_floating_pod_state_is_active ON floating_pod_state(is_active);
CREATE INDEX IF NOT EXISTS idx_floating_pod_state_last_updated ON floating_pod_state(last_updated);

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL,
  description TEXT NOT NULL
);

INSERT INTO schema_version (version, applied_at, description)
VALUES (1, strftime('%s', 'now') * 1000, 'Initial FloatingPod state schema');
