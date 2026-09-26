-- V004: 物理恢复责任的最小持久化绑定与 locator 契约；不创建任何业务行。

CREATE TABLE IF NOT EXISTS physical_recovery_binding (
  singleton_fixed1 INTEGER PRIMARY KEY NOT NULL CHECK(singleton_fixed1 = 1),
  binding_uuid BLOB NOT NULL UNIQUE,
  cutover_state TEXT NOT NULL,
  format_version INTEGER NOT NULL,
  CHECK(length(binding_uuid) = 16),
  CHECK(cutover_state IN ('LEGACY_UNCERTIFIED', 'CERTIFIED')),
  CHECK(format_version >= 1)
);

CREATE TABLE IF NOT EXISTS physical_recovery_locator (
  pod_uuid BLOB PRIMARY KEY NOT NULL,
  attempt_uuid BLOB NOT NULL UNIQUE,
  locator_state TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  domain_version INTEGER,
  target_dimension_id INTEGER,
  CHECK(length(pod_uuid) = 16),
  CHECK(length(attempt_uuid) = 16),
  CHECK(locator_state IN (
    'CREATE_PREPARED',
    'ROW_ACTIVE',
    'ROW_INACTIVE_PENDING',
    'ROW_INACTIVE_VERIFIED',
    'ABSENT_PENDING',
    'ABSENT_VERIFIED',
    'LEGACY_UNCERTIFIED',
    'QUARANTINED'
  )),
  CHECK(length(trim(reason_code)) > 0),
  CHECK(domain_version IS NULL OR domain_version >= 0)
);

CREATE TABLE IF NOT EXISTS physical_recovery_locator_chunk (
  pod_uuid BLOB NOT NULL,
  dimension_id INTEGER NOT NULL,
  chunk_x INTEGER NOT NULL,
  chunk_z INTEGER NOT NULL,
  PRIMARY KEY(pod_uuid, dimension_id, chunk_x, chunk_z),
  FOREIGN KEY(pod_uuid) REFERENCES physical_recovery_locator(pod_uuid)
);

CREATE INDEX IF NOT EXISTS idx_physical_recovery_locator_chunk_dimension
ON physical_recovery_locator_chunk(dimension_id, chunk_x, chunk_z);

INSERT INTO schema_version (version, applied_at, description)
VALUES (4, strftime('%s', 'now') * 1000, 'Physical recovery obligations');
