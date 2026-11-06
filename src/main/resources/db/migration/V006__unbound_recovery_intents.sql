-- V006: persist unbound adoption and controlled-create ownership separately from legacy CREATE_PREPARED.
-- Rebuild both locator tables with foreign keys enabled; SQLite rewrites the renamed child's
-- reference to the renamed parent, so the old pair stays internally valid until copied/dropped.

ALTER TABLE physical_recovery_locator_chunk RENAME TO physical_recovery_locator_chunk_v5;
ALTER TABLE physical_recovery_locator RENAME TO physical_recovery_locator_v5;

CREATE TABLE physical_recovery_locator (
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
    'UNBOUND_ADOPTION_PREPARED',
    'DEBUG_ITEM_CREATE_PREPARED',
    'ROW_ACTIVE',
    'ROW_INACTIVE_PENDING',
    'ROW_INACTIVE_VERIFIED',
    'ABSENT_PENDING',
    'ABSENT_VERIFIED',
    'LEGACY_UNCERTIFIED',
    'QUARANTINED'
  )),
  CHECK(length(trim(reason_code)) > 0),
  CHECK(domain_version IS NULL OR domain_version >= 0),
  CHECK(locator_state <> 'UNBOUND_ADOPTION_PREPARED' OR (reason_code = 'unbound_adoption_prepared' AND domain_version = 0 AND target_dimension_id IS NOT NULL)),
  CHECK(locator_state <> 'DEBUG_ITEM_CREATE_PREPARED' OR (reason_code = 'debug_item_create_prepared' AND domain_version = 0 AND target_dimension_id IS NOT NULL))
);

INSERT INTO physical_recovery_locator (pod_uuid, attempt_uuid, locator_state, reason_code, domain_version, target_dimension_id)
SELECT pod_uuid, attempt_uuid, locator_state, reason_code, domain_version, target_dimension_id
FROM physical_recovery_locator_v5;

CREATE TABLE physical_recovery_locator_chunk (
  pod_uuid BLOB NOT NULL,
  dimension_id INTEGER NOT NULL,
  chunk_x INTEGER NOT NULL,
  chunk_z INTEGER NOT NULL,
  PRIMARY KEY(pod_uuid, dimension_id, chunk_x, chunk_z),
  FOREIGN KEY(pod_uuid) REFERENCES physical_recovery_locator(pod_uuid)
);

INSERT INTO physical_recovery_locator_chunk (pod_uuid, dimension_id, chunk_x, chunk_z)
SELECT pod_uuid, dimension_id, chunk_x, chunk_z
FROM physical_recovery_locator_chunk_v5;

DROP TABLE physical_recovery_locator_chunk_v5;
DROP TABLE physical_recovery_locator_v5;

CREATE INDEX idx_physical_recovery_locator_chunk_dimension
ON physical_recovery_locator_chunk(dimension_id, chunk_x, chunk_z);

CREATE TABLE physical_recovery_intent (
  pod_uuid BLOB PRIMARY KEY NOT NULL,
  attempt_uuid BLOB NOT NULL UNIQUE,
  binding_uuid BLOB NOT NULL,
  origin TEXT NOT NULL,
  phase TEXT NOT NULL,
  source_kind TEXT,
  source_identity TEXT,
  catalog_generation BLOB,
  create_operation_uuid BLOB,
  prepared_domain_version INTEGER NOT NULL,
  target_world_name TEXT NOT NULL,
  target_pos_x REAL NOT NULL,
  target_pos_y REAL NOT NULL,
  target_pos_z REAL NOT NULL,
  target_yaw REAL NOT NULL,
  target_pitch REAL NOT NULL,
  target_dimension_id INTEGER NOT NULL,
  target_chunk_x INTEGER NOT NULL,
  target_chunk_z INTEGER NOT NULL,
  committed_domain_version INTEGER,
  committed_at INTEGER,
  committed_world_name TEXT,
  committed_pos_x REAL,
  committed_pos_y REAL,
  committed_pos_z REAL,
  committed_yaw REAL,
  committed_pitch REAL,
  committed_dimension_id INTEGER,
  CHECK(length(pod_uuid) = 16),
  CHECK(length(attempt_uuid) = 16),
  CHECK(length(binding_uuid) = 16),
  CHECK(origin IN ('UNBOUND_ADOPTION', 'DEBUG_ITEM_CREATE')),
  CHECK(phase IN ('PREPARED', 'COMMITTED')),
  CHECK(prepared_domain_version = 0),
  CHECK(length(trim(target_world_name)) > 0),
  CHECK(abs(target_pos_x) < 1.0e300 AND abs(target_pos_y) < 1.0e300 AND abs(target_pos_z) < 1.0e300),
  CHECK(abs(target_yaw) < 1.0e30 AND abs(target_pitch) < 1.0e30),
  CHECK((origin = 'UNBOUND_ADOPTION'
      AND source_kind IS NOT NULL
      AND source_kind IN ('FRESH', 'LEGACY_UNREGISTERED')
      AND source_identity IS NOT NULL
      AND length(trim(source_identity)) > 0
      AND catalog_generation IS NOT NULL
      AND length(catalog_generation) = 16
      AND create_operation_uuid IS NULL)
    OR (origin = 'DEBUG_ITEM_CREATE'
      AND source_kind IS NULL
      AND source_identity IS NULL
      AND catalog_generation IS NULL
      AND create_operation_uuid IS NOT NULL
      AND length(create_operation_uuid) = 16)),
  CHECK((phase = 'PREPARED'
      AND committed_domain_version IS NULL AND committed_at IS NULL AND committed_world_name IS NULL
      AND committed_pos_x IS NULL AND committed_pos_y IS NULL AND committed_pos_z IS NULL
      AND committed_yaw IS NULL AND committed_pitch IS NULL AND committed_dimension_id IS NULL)
    OR (phase = 'COMMITTED'
      AND committed_domain_version = prepared_domain_version + 1
      AND committed_at IS NOT NULL AND committed_world_name = target_world_name
      AND committed_pos_x = target_pos_x AND committed_pos_y = target_pos_y AND committed_pos_z = target_pos_z
      AND committed_yaw = target_yaw AND committed_pitch = target_pitch AND committed_dimension_id = target_dimension_id)),
  FOREIGN KEY(pod_uuid) REFERENCES floating_pod_state(uuid),
  FOREIGN KEY(binding_uuid) REFERENCES physical_recovery_binding(binding_uuid)
);

CREATE INDEX idx_physical_recovery_intent_binding ON physical_recovery_intent(binding_uuid, phase);
CREATE UNIQUE INDEX ux_physical_recovery_intent_observed_source
ON physical_recovery_intent(binding_uuid, source_kind, catalog_generation, source_identity)
WHERE origin = 'UNBOUND_ADOPTION';
CREATE UNIQUE INDEX ux_physical_recovery_intent_create_operation
ON physical_recovery_intent(binding_uuid, create_operation_uuid)
WHERE origin = 'DEBUG_ITEM_CREATE';

CREATE TRIGGER trg_physical_recovery_intent_validate_insert
BEFORE INSERT ON physical_recovery_intent
BEGIN
  SELECT RAISE(ABORT, 'unbound intent requires an exact inactive WORLD_IDLE row and certified binding')
  WHERE NEW.phase <> 'PREPARED'
    OR NOT EXISTS (
      SELECT 1 FROM floating_pod_state s
      JOIN physical_recovery_binding b ON b.binding_uuid = NEW.binding_uuid AND b.cutover_state = 'CERTIFIED'
      WHERE s.uuid = NEW.pod_uuid AND s.owner_uuid IS NULL AND s.state_version = 0
        AND s.world_name = NEW.target_world_name
        AND s.pos_x = NEW.target_pos_x AND s.pos_y = NEW.target_pos_y AND s.pos_z = NEW.target_pos_z
        AND s.yaw = NEW.target_yaw AND s.pitch = NEW.target_pitch
        AND s.is_active = 0 AND s.dimension_id = NEW.target_dimension_id AND s.operating_mode = 'WORLD_IDLE'
    )
  ;
  SELECT RAISE(ABORT, 'unbound intent cannot replace physical recovery history')
  WHERE EXISTS (SELECT 1 FROM physical_recovery_locator WHERE pod_uuid = NEW.pod_uuid);
END;

CREATE TRIGGER trg_floating_pod_state_reject_prepared_unbound_update
BEFORE UPDATE ON floating_pod_state
WHEN EXISTS (SELECT 1 FROM physical_recovery_intent i WHERE i.pod_uuid = OLD.uuid AND i.phase = 'PREPARED')
BEGIN
  SELECT RAISE(ABORT, 'prepared unbound row only permits exact activation') WHERE NOT EXISTS (
    SELECT 1 FROM physical_recovery_intent i
    WHERE i.pod_uuid = OLD.uuid AND i.phase = 'PREPARED'
      AND OLD.owner_uuid IS NULL AND OLD.is_active = 0 AND OLD.operating_mode = 'WORLD_IDLE'
      AND NEW.uuid = OLD.uuid AND NEW.owner_uuid IS NULL AND NEW.state_version = i.prepared_domain_version + 1
      AND NEW.world_name = i.target_world_name
      AND NEW.pos_x = i.target_pos_x AND NEW.pos_y = i.target_pos_y AND NEW.pos_z = i.target_pos_z
      AND NEW.yaw = i.target_yaw AND NEW.pitch = i.target_pitch
      AND NEW.is_active = 1 AND NEW.dimension_id = i.target_dimension_id AND NEW.operating_mode = 'WORLD_IDLE'
  );
END;

CREATE TRIGGER trg_floating_pod_state_reject_unbound_delete
BEFORE DELETE ON floating_pod_state
WHEN EXISTS (SELECT 1 FROM physical_recovery_intent i WHERE i.pod_uuid = OLD.uuid)
BEGIN
  SELECT RAISE(ABORT, 'unbound intent history protects its domain row');
END;

CREATE TRIGGER trg_physical_recovery_locator_validate_unbound_insert
BEFORE INSERT ON physical_recovery_locator
WHEN NEW.locator_state IN ('UNBOUND_ADOPTION_PREPARED', 'DEBUG_ITEM_CREATE_PREPARED')
BEGIN
  SELECT RAISE(ABORT, 'unbound prepared locator requires matching intent') WHERE NOT EXISTS (
    SELECT 1 FROM physical_recovery_intent i
    JOIN floating_pod_state s ON s.uuid = i.pod_uuid
    WHERE i.pod_uuid = NEW.pod_uuid AND i.attempt_uuid = NEW.attempt_uuid
      AND i.phase = 'PREPARED' AND i.prepared_domain_version = NEW.domain_version
      AND i.target_dimension_id = NEW.target_dimension_id
      AND s.owner_uuid IS NULL AND s.state_version = NEW.domain_version
      AND s.is_active = 0 AND s.dimension_id = NEW.target_dimension_id
      AND ((i.origin = 'UNBOUND_ADOPTION' AND NEW.locator_state = 'UNBOUND_ADOPTION_PREPARED' AND NEW.reason_code = 'unbound_adoption_prepared')
        OR (i.origin = 'DEBUG_ITEM_CREATE' AND NEW.locator_state = 'DEBUG_ITEM_CREATE_PREPARED' AND NEW.reason_code = 'debug_item_create_prepared'))
  );
END;

CREATE TRIGGER trg_physical_recovery_locator_validate_unbound_update
BEFORE UPDATE ON physical_recovery_locator
WHEN EXISTS (SELECT 1 FROM physical_recovery_intent i WHERE i.pod_uuid = OLD.pod_uuid AND i.phase = 'PREPARED')
BEGIN
  SELECT RAISE(ABORT, 'prepared unbound locator only permits exact matching activation') WHERE NOT EXISTS (
    SELECT 1 FROM physical_recovery_intent i
    JOIN floating_pod_state s ON s.uuid = i.pod_uuid
    WHERE i.pod_uuid = OLD.pod_uuid AND i.attempt_uuid = OLD.attempt_uuid AND i.phase = 'PREPARED'
      AND ((i.origin = 'UNBOUND_ADOPTION' AND OLD.locator_state = 'UNBOUND_ADOPTION_PREPARED')
        OR (i.origin = 'DEBUG_ITEM_CREATE' AND OLD.locator_state = 'DEBUG_ITEM_CREATE_PREPARED'))
      AND NEW.pod_uuid = OLD.pod_uuid AND NEW.attempt_uuid = OLD.attempt_uuid
      AND NEW.locator_state = 'ROW_ACTIVE' AND NEW.reason_code = 'row_active'
      AND NEW.domain_version = i.prepared_domain_version + 1 AND NEW.target_dimension_id = i.target_dimension_id
      AND s.owner_uuid IS NULL AND s.state_version = NEW.domain_version AND s.is_active = 1
      AND s.operating_mode = 'WORLD_IDLE' AND s.dimension_id = i.target_dimension_id
      AND s.world_name = i.target_world_name
      AND s.pos_x = i.target_pos_x AND s.pos_y = i.target_pos_y AND s.pos_z = i.target_pos_z
      AND s.yaw = i.target_yaw AND s.pitch = i.target_pitch
      AND (SELECT COUNT(*) FROM physical_recovery_locator_chunk c WHERE c.pod_uuid = OLD.pod_uuid) = 25
      AND NOT EXISTS (SELECT 1 FROM physical_recovery_locator_chunk c WHERE c.pod_uuid = OLD.pod_uuid
        AND (c.dimension_id <> i.target_dimension_id OR c.chunk_x < i.target_chunk_x - 2 OR c.chunk_x > i.target_chunk_x + 2
          OR c.chunk_z < i.target_chunk_z - 2 OR c.chunk_z > i.target_chunk_z + 2))
  );
END;

CREATE TRIGGER trg_physical_recovery_locator_reject_unbound_delete
BEFORE DELETE ON physical_recovery_locator
WHEN EXISTS (SELECT 1 FROM physical_recovery_intent i WHERE i.pod_uuid = OLD.pod_uuid)
BEGIN
  SELECT RAISE(ABORT, 'unbound intent history protects its physical locator');
END;

CREATE TRIGGER trg_physical_recovery_locator_chunk_validate_unbound_insert
BEFORE INSERT ON physical_recovery_locator_chunk
WHEN EXISTS (SELECT 1 FROM physical_recovery_intent i WHERE i.pod_uuid = NEW.pod_uuid)
BEGIN
  SELECT RAISE(ABORT, 'unbound prepared locator chunks must match the target square') WHERE NOT EXISTS (
    SELECT 1 FROM physical_recovery_intent i
    JOIN physical_recovery_locator l ON l.pod_uuid = i.pod_uuid
    WHERE i.pod_uuid = NEW.pod_uuid AND i.phase = 'PREPARED'
      AND l.attempt_uuid = i.attempt_uuid AND l.target_dimension_id = i.target_dimension_id
      AND ((i.origin = 'UNBOUND_ADOPTION' AND l.locator_state = 'UNBOUND_ADOPTION_PREPARED')
        OR (i.origin = 'DEBUG_ITEM_CREATE' AND l.locator_state = 'DEBUG_ITEM_CREATE_PREPARED'))
      AND NEW.dimension_id = i.target_dimension_id
      AND NEW.chunk_x BETWEEN i.target_chunk_x - 2 AND i.target_chunk_x + 2
      AND NEW.chunk_z BETWEEN i.target_chunk_z - 2 AND i.target_chunk_z + 2
  );
END;

CREATE TRIGGER trg_physical_recovery_locator_chunk_reject_unbound_update
BEFORE UPDATE ON physical_recovery_locator_chunk
WHEN EXISTS (SELECT 1 FROM physical_recovery_intent i WHERE i.pod_uuid = OLD.pod_uuid)
BEGIN
  SELECT RAISE(ABORT, 'unbound intent locator chunks are immutable');
END;

CREATE TRIGGER trg_physical_recovery_locator_chunk_reject_unbound_delete
BEFORE DELETE ON physical_recovery_locator_chunk
WHEN EXISTS (SELECT 1 FROM physical_recovery_intent i WHERE i.pod_uuid = OLD.pod_uuid)
BEGIN
  SELECT RAISE(ABORT, 'unbound intent locator chunks are immutable');
END;

CREATE TRIGGER trg_physical_recovery_intent_validate_commit
BEFORE UPDATE ON physical_recovery_intent
BEGIN
  SELECT RAISE(ABORT, 'unbound intent only permits exact committed readback') WHERE NOT (
    OLD.phase = 'PREPARED' AND NEW.phase = 'COMMITTED'
      AND NEW.pod_uuid = OLD.pod_uuid AND NEW.attempt_uuid = OLD.attempt_uuid
      AND NEW.binding_uuid = OLD.binding_uuid AND NEW.origin = OLD.origin
      AND NEW.source_kind IS OLD.source_kind AND NEW.source_identity IS OLD.source_identity
      AND NEW.catalog_generation IS OLD.catalog_generation AND NEW.create_operation_uuid IS OLD.create_operation_uuid
      AND NEW.prepared_domain_version = OLD.prepared_domain_version
      AND NEW.target_world_name = OLD.target_world_name
      AND NEW.target_pos_x = OLD.target_pos_x AND NEW.target_pos_y = OLD.target_pos_y AND NEW.target_pos_z = OLD.target_pos_z
      AND NEW.target_yaw = OLD.target_yaw AND NEW.target_pitch = OLD.target_pitch
      AND NEW.target_dimension_id = OLD.target_dimension_id
      AND NEW.target_chunk_x = OLD.target_chunk_x AND NEW.target_chunk_z = OLD.target_chunk_z
      AND EXISTS (
        SELECT 1 FROM floating_pod_state s
        JOIN physical_recovery_locator l ON l.pod_uuid = s.uuid
        WHERE s.uuid = OLD.pod_uuid AND s.owner_uuid IS NULL AND s.state_version = NEW.committed_domain_version
          AND s.is_active = 1 AND s.operating_mode = 'WORLD_IDLE' AND s.dimension_id = NEW.committed_dimension_id
          AND s.world_name = NEW.committed_world_name
          AND s.pos_x = NEW.committed_pos_x AND s.pos_y = NEW.committed_pos_y AND s.pos_z = NEW.committed_pos_z
          AND s.yaw = NEW.committed_yaw AND s.pitch = NEW.committed_pitch AND s.last_updated = NEW.committed_at
          AND l.attempt_uuid = OLD.attempt_uuid AND l.locator_state = 'ROW_ACTIVE' AND l.reason_code = 'row_active'
          AND l.domain_version = NEW.committed_domain_version AND l.target_dimension_id = NEW.committed_dimension_id
      )
      AND NEW.committed_domain_version = OLD.prepared_domain_version + 1
      AND NEW.committed_world_name = OLD.target_world_name
      AND NEW.committed_pos_x = OLD.target_pos_x AND NEW.committed_pos_y = OLD.target_pos_y AND NEW.committed_pos_z = OLD.target_pos_z
      AND NEW.committed_yaw = OLD.target_yaw AND NEW.committed_pitch = OLD.target_pitch
      AND NEW.committed_dimension_id = OLD.target_dimension_id
      AND (SELECT COUNT(*) FROM physical_recovery_locator_chunk c WHERE c.pod_uuid = OLD.pod_uuid) = 25
      AND NOT EXISTS (SELECT 1 FROM physical_recovery_locator_chunk c WHERE c.pod_uuid = OLD.pod_uuid
        AND (c.dimension_id <> OLD.target_dimension_id OR c.chunk_x < OLD.target_chunk_x - 2 OR c.chunk_x > OLD.target_chunk_x + 2
          OR c.chunk_z < OLD.target_chunk_z - 2 OR c.chunk_z > OLD.target_chunk_z + 2))
  );
END;

CREATE TRIGGER trg_physical_recovery_intent_reject_delete
BEFORE DELETE ON physical_recovery_intent
BEGIN
  SELECT RAISE(ABORT, 'physical recovery intent history cannot be deleted');
END;

INSERT INTO schema_version (version, applied_at, description)
VALUES (6, strftime('%s', 'now') * 1000, 'Durable unbound physical recovery intents');
