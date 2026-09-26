-- V003: 通用 CRUD 不得绕过活动生命周期权威边界。

CREATE TRIGGER trg_floating_pod_state_reject_active_insert
BEFORE INSERT ON floating_pod_state
WHEN NEW.is_active = 1
BEGIN
  SELECT RAISE(ABORT, 'active FloatingPod rows must be created by activity transition');
END;

CREATE TRIGGER trg_floating_pod_state_reject_active_null_dimension
BEFORE UPDATE ON floating_pod_state
WHEN NEW.is_active = 1 AND NEW.dimension_id IS NULL
BEGIN
  SELECT RAISE(ABORT, 'active FloatingPod rows require dimension_id');
END;

CREATE TRIGGER trg_floating_pod_state_reject_active_delete
BEFORE DELETE ON floating_pod_state
WHEN OLD.is_active = 1
BEGIN
  SELECT RAISE(ABORT, 'active FloatingPod rows must be deactivated before delete');
END;

INSERT INTO schema_version (version, applied_at, description)
VALUES (3, strftime('%s', 'now') * 1000, 'Strict activity lifecycle guards');
