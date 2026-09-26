-- V002: 活动状态维度与玩家权益的服务端权威存储。

ALTER TABLE floating_pod_state ADD COLUMN dimension_id INTEGER;

CREATE TABLE IF NOT EXISTS player_entitlement (
  player_uuid BLOB PRIMARY KEY NOT NULL,
  registration_limit INTEGER NOT NULL DEFAULT 1,
  simultaneous_active_limit INTEGER NOT NULL DEFAULT 1,
  CHECK(length(player_uuid) = 16),
  CHECK(registration_limit >= 0),
  CHECK(simultaneous_active_limit >= 0)
);

-- 仅补齐 V001 中已有且已绑定的玩家；既有显式权益绝不覆盖。
INSERT OR IGNORE INTO player_entitlement (player_uuid, registration_limit, simultaneous_active_limit)
SELECT DISTINCT owner_uuid, 1, 1
FROM floating_pod_state
WHERE owner_uuid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_floating_pod_state_owner_active
ON floating_pod_state(owner_uuid, is_active);

INSERT INTO schema_version (version, applied_at, description)
VALUES (2, strftime('%s', 'now') * 1000, 'Activity transition authority');
