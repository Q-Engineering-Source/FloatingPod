-- V005: 持久化行为模式，并将历史活动待认领行迁移为原位 WorldIdle。

ALTER TABLE floating_pod_state
ADD COLUMN operating_mode TEXT NOT NULL DEFAULT 'NORMAL'
CHECK (operating_mode IN ('NORMAL', 'WORLD_IDLE'));

-- V003 的遗留空维度保护会拒绝任何 active row 更新；临时移除后执行纯 mode 回填并立即恢复原保护。
DROP TRIGGER IF EXISTS trg_floating_pod_state_reject_active_null_dimension;

UPDATE floating_pod_state
SET operating_mode = 'WORLD_IDLE'
WHERE owner_uuid IS NULL AND is_active = 1;

CREATE TRIGGER trg_floating_pod_state_reject_active_null_dimension
BEFORE UPDATE ON floating_pod_state
WHEN NEW.is_active = 1 AND NEW.dimension_id IS NULL
BEGIN
  SELECT RAISE(ABORT, 'active FloatingPod rows require dimension_id');
END;

CREATE TRIGGER trg_floating_pod_state_reject_unclaimed_active_normal_insert
BEFORE INSERT ON floating_pod_state
WHEN NEW.is_active = 1 AND NEW.owner_uuid IS NULL AND NEW.operating_mode <> 'WORLD_IDLE'
BEGIN
  SELECT RAISE(ABORT, 'active unclaimed FloatingPod rows must use WORLD_IDLE');
END;

CREATE TRIGGER trg_floating_pod_state_reject_unclaimed_active_normal_update
BEFORE UPDATE ON floating_pod_state
WHEN NEW.is_active = 1 AND NEW.owner_uuid IS NULL AND NEW.operating_mode <> 'WORLD_IDLE'
BEGIN
  SELECT RAISE(ABORT, 'active unclaimed FloatingPod rows must use WORLD_IDLE');
END;

INSERT INTO schema_version (version, applied_at, description)
VALUES (5, strftime('%s', 'now') * 1000, 'Independent operating modes');
