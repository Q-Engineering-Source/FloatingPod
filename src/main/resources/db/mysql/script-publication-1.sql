-- FloatingPod schema format 4 / ScriptPublication 1. SOURCE DEFINITION ONLY.
-- No SQL execution/installation/migration/READY acceptance is implied.
-- Baseline: MySQL-FORMAT3-DDL/SOURCE-v6, preserved full 20 relations and 60 guards.
-- Only schema/role identity, manifest=4 and SCRIPT_PUBLICATION step differ in that baseline.
-- Physical binding=2, B1 container and all historical archives remain unchanged.
-- Account creation, DDL and grants require separately authorized administration; DDL implicitly commits.
-- All accounts remain locked. Runtime grants below describe the final approved-seal privilege set;
-- no seal, unlock, credential, ALTER migration, automatic import or runtime activation occurs here.
-- SOURCE ONLY; separate approved SEC administration, not rollbackable business SQL.
CREATE DATABASE `fp_script_publication1` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'fp_s4_guard_definer'@'localhost' ACCOUNT LOCK;
CREATE USER 'fp_s4_audit_definer'@'localhost' ACCOUNT LOCK;
CREATE USER 'fp_s4_runtime'@'localhost' ACCOUNT LOCK;
CREATE USER 'fp_s4_provisioner'@'localhost' ACCOUNT LOCK;
CREATE USER 'fp_s4_importer'@'localhost' ACCOUNT LOCK;
-- No credential, unlock, global account-admin/SET_ANY_DEFINER grant or production default.
-- Actual server's explicit-definer DDL/binarylog administration is SEC authority, never a runtime grant.

-- MySQL format3 full twenty-relation SOURCE CANDIDATE; NOT EXECUTED.
-- New isolated target only. No IF NOT EXISTS, DROP, old READY ALTER, auto-seal or migration.
-- DDL has implicit commits; retain partial failure/object inventories rather than claim rollback.
-- Explicit binary parameters mandatory. UUID=16 bytes Java MSB/LSB big endian, no byte swap.
SET SESSION sql_mode='STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin;
USE fp_script_publication1;

-- METHOD sections 2,10,11,12,13: fp_schema_manifest; immediate default NO ACTION.
CREATE TABLE fp_schema_manifest (
 singleton_fixed1 TINYINT NOT NULL,
 backend VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 format_version INT NOT NULL,
 ddl_sha256 VARBINARY(32) NOT NULL,
 provisioning_uuid VARBINARY(16) NOT NULL,
 bootstrap_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 expected_server_uuid VARBINARY(16) NOT NULL,
 expected_binding_uuid VARBINARY(16) NOT NULL,
 import_manifest_sha256 VARBINARY(32) NULL,
 import_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 import_operation_uuid VARBINARY(16) NULL,
 import_snapshot_sha256 VARBINARY(32) NULL,
 import_receipt_sha256 VARBINARY(32) NULL,
 guard_manifest_sha256 VARBINARY(32) NULL,
 configuration_sha256 VARBINARY(32) NOT NULL,
 source_provenance_kind VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 source_provenance_bytes LONGBLOB NOT NULL,
 source_provenance_sha256 VARBINARY(32) NOT NULL,
 limit_profile_encoding VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 limit_profile_bytes LONGBLOB NOT NULL,
 limit_profile_sha256 VARBINARY(32) NOT NULL,
 max_region_bytes BIGINT NOT NULL,
 max_chunk_bytes BIGINT NOT NULL,
 max_decompressed_bytes BIGINT NOT NULL,
 max_nbt_depth BIGINT NOT NULL,
 max_members BIGINT NOT NULL,
 max_retained_bytes BIGINT NOT NULL,
 max_sql_packet_bytes BIGINT NOT NULL,
 max_native_artifact_bytes BIGINT NOT NULL,
 PRIMARY KEY (singleton_fixed1),
 CONSTRAINT ck_f3_01_01 CHECK (singleton_fixed1=1),
 CONSTRAINT ck_f3_01_02 CHECK (backend='MYSQL'),
 CONSTRAINT ck_f3_01_03 CHECK (format_version=4),
 CONSTRAINT ck_f3_01_04 CHECK (bootstrap_state IN ('BOOTSTRAPPING','READY')),
 CONSTRAINT ck_f3_01_05 CHECK (import_state IN ('NONE','PENDING','COMMITTED')),
 CONSTRAINT ck_f3_01_06 CHECK (source_provenance_kind IN ('FRESH_PLAN','FORMAT3_RESTORE','FORMAT2_IMPORT','SQLITE_IMPORT')),
 CONSTRAINT ck_f3_01_07 CHECK (limit_profile_encoding='FP_SOURCE_LIMIT_PROFILE_BE_V1'),
 CONSTRAINT ck_f3_01_08 CHECK ((import_state='NONE' AND import_manifest_sha256 IS NULL AND import_operation_uuid IS NULL AND import_snapshot_sha256 IS NULL AND import_receipt_sha256 IS NULL) OR (import_state='PENDING' AND import_manifest_sha256 IS NOT NULL AND import_operation_uuid IS NOT NULL AND import_snapshot_sha256 IS NOT NULL AND import_receipt_sha256 IS NULL) OR (import_state='COMMITTED' AND import_manifest_sha256 IS NOT NULL AND import_operation_uuid IS NOT NULL AND import_snapshot_sha256 IS NOT NULL AND import_receipt_sha256 IS NOT NULL)),
 CONSTRAINT ck_f3_01_09 CHECK (bootstrap_state<>'READY' OR (guard_manifest_sha256 IS NOT NULL AND import_state IN ('NONE','COMMITTED'))),
 CONSTRAINT ck_f3_01_10 CHECK (max_region_bytes>0),
 CONSTRAINT ck_f3_01_11 CHECK (max_chunk_bytes>0),
 CONSTRAINT ck_f3_01_12 CHECK (max_decompressed_bytes>0),
 CONSTRAINT ck_f3_01_13 CHECK (max_nbt_depth>0),
 CONSTRAINT ck_f3_01_14 CHECK (max_members>0),
 CONSTRAINT ck_f3_01_15 CHECK (max_retained_bytes>0),
 CONSTRAINT ck_f3_01_16 CHECK (max_sql_packet_bytes>0),
 CONSTRAINT ck_f3_01_17 CHECK (max_native_artifact_bytes>0),
 CONSTRAINT ck_f3_01_18 CHECK (OCTET_LENGTH(ddl_sha256)=32),
 CONSTRAINT ck_f3_01_19 CHECK (OCTET_LENGTH(provisioning_uuid)=16 AND provisioning_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_01_20 CHECK (OCTET_LENGTH(expected_server_uuid)=16 AND expected_server_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_01_21 CHECK (OCTET_LENGTH(expected_binding_uuid)=16 AND expected_binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_01_22 CHECK (import_manifest_sha256 IS NULL OR OCTET_LENGTH(import_manifest_sha256)=32),
 CONSTRAINT ck_f3_01_23 CHECK (import_operation_uuid IS NULL OR (OCTET_LENGTH(import_operation_uuid)=16 AND import_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_01_24 CHECK (import_snapshot_sha256 IS NULL OR OCTET_LENGTH(import_snapshot_sha256)=32),
 CONSTRAINT ck_f3_01_25 CHECK (import_receipt_sha256 IS NULL OR OCTET_LENGTH(import_receipt_sha256)=32),
 CONSTRAINT ck_f3_01_26 CHECK (guard_manifest_sha256 IS NULL OR OCTET_LENGTH(guard_manifest_sha256)=32),
 CONSTRAINT ck_f3_01_27 CHECK (OCTET_LENGTH(configuration_sha256)=32),
 CONSTRAINT ck_f3_01_28 CHECK (OCTET_LENGTH(source_provenance_bytes)>0),
 CONSTRAINT ck_f3_01_29 CHECK (OCTET_LENGTH(source_provenance_sha256)=32),
 CONSTRAINT ck_f3_01_30 CHECK (OCTET_LENGTH(limit_profile_bytes)>0),
 CONSTRAINT ck_f3_01_31 CHECK (OCTET_LENGTH(limit_profile_sha256)=32),
 CONSTRAINT ck_f3_01_32 CHECK (max_region_bytes>=0),
 CONSTRAINT ck_f3_01_33 CHECK (max_chunk_bytes>=0),
 CONSTRAINT ck_f3_01_34 CHECK (max_decompressed_bytes>=0),
 CONSTRAINT ck_f3_01_35 CHECK (max_nbt_depth>=0),
 CONSTRAINT ck_f3_01_36 CHECK (max_members>=0),
 CONSTRAINT ck_f3_01_37 CHECK (max_retained_bytes>=0),
 CONSTRAINT ck_f3_01_38 CHECK (max_sql_packet_bytes>=0),
 CONSTRAINT ck_f3_01_39 CHECK (max_native_artifact_bytes>=0),
 CONSTRAINT ck_f3_01_40 CHECK (source_provenance_sha256=UNHEX(SHA2(source_provenance_bytes,256))),
 CONSTRAINT ck_f3_01_41 CHECK (limit_profile_sha256=UNHEX(SHA2(limit_profile_bytes,256)))
) ENGINE=InnoDB;

-- METHOD sections 3,6,11: fp_runtime_writer; immediate default NO ACTION.
CREATE TABLE fp_runtime_writer (
 singleton_fixed1 TINYINT NOT NULL,
 binding_uuid VARBINARY(16) NOT NULL,
 writer_epoch BIGINT NOT NULL,
 owner_boot_uuid VARBINARY(16) NULL,
 lease_until DATETIME(6) NOT NULL,
 last_commit_operation_uuid VARBINARY(16) NULL,
 last_commit_fingerprint VARBINARY(32) NULL,
 PRIMARY KEY (singleton_fixed1),
 CONSTRAINT ck_f3_02_01 CHECK (singleton_fixed1=1),
 CONSTRAINT ck_f3_02_02 CHECK (((last_commit_operation_uuid IS NULL AND last_commit_fingerprint IS NULL) OR (last_commit_operation_uuid IS NOT NULL AND last_commit_fingerprint IS NOT NULL))),
 CONSTRAINT ck_f3_02_03 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_02_04 CHECK (writer_epoch>=0),
 CONSTRAINT ck_f3_02_05 CHECK (owner_boot_uuid IS NULL OR (OCTET_LENGTH(owner_boot_uuid)=16 AND owner_boot_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_02_06 CHECK (last_commit_operation_uuid IS NULL OR (OCTET_LENGTH(last_commit_operation_uuid)=16 AND last_commit_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_02_07 CHECK (last_commit_fingerprint IS NULL OR OCTET_LENGTH(last_commit_fingerprint)=32)
) ENGINE=InnoDB;

-- METHOD sections 2,12: physical_recovery_binding; immediate default NO ACTION.
CREATE TABLE physical_recovery_binding (
 singleton_fixed1 TINYINT NOT NULL,
 binding_uuid VARBINARY(16) NOT NULL,
 cutover_state VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 format_version INT NOT NULL,
 PRIMARY KEY (singleton_fixed1),
 UNIQUE KEY ux_binding_uuid (binding_uuid),
 CONSTRAINT ck_f3_03_01 CHECK (singleton_fixed1=1),
 CONSTRAINT ck_f3_03_02 CHECK (cutover_state IN ('LEGACY_UNCERTIFIED','CERTIFIED')),
 CONSTRAINT ck_f3_03_03 CHECK (format_version=2),
 CONSTRAINT ck_f3_03_04 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16)))
) ENGINE=InnoDB;

-- METHOD sections 6: fp_recovery_operation; immediate default NO ACTION.
CREATE TABLE fp_recovery_operation (
 binding_uuid VARBINARY(16) NOT NULL,
 operation_uuid VARBINARY(16) NOT NULL,
 workflow_uuid VARBINARY(16) NOT NULL,
 step VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 operation_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 pod_uuid VARBINARY(16) NULL,
 attempt_uuid VARBINARY(16) NULL,
 footprint_uuid VARBINARY(16) NULL,
 source_unit_uuid VARBINARY(16) NULL,
 forest_uuid VARBINARY(16) NULL,
 boot_uuid VARBINARY(16) NOT NULL,
 boot_generation BIGINT NOT NULL,
 connection_generation_uuid VARBINARY(16) NOT NULL,
 connection_id BIGINT NOT NULL,
 fence_epoch BIGINT NOT NULL,
 request_identity_bytes LONGBLOB NOT NULL,
 request_identity_sha256 VARBINARY(32) NOT NULL,
 expected_domain_version BIGINT NULL,
 expected_locator_revision BIGINT NULL,
 expected_source_revision BIGINT NULL,
 context_encoding VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 original_context_bytes LONGBLOB NOT NULL,
 original_context_sha256 VARBINARY(32) NOT NULL,
 scope_bytes LONGBLOB NOT NULL,
 scope_sha256 VARBINARY(32) NOT NULL,
 preimage_bytes LONGBLOB NOT NULL,
 preimage_sha256 VARBINARY(32) NOT NULL,
 target_bytes LONGBLOB NOT NULL,
 target_sha256 VARBINARY(32) NOT NULL,
 reserve_operation_uuid VARBINARY(16) NULL,
 previous_step_operation_uuid VARBINARY(16) NULL,
 settled_by_operation_uuid VARBINARY(16) NULL,
 commit_witness_bytes LONGBLOB NULL,
 commit_witness_sha256 VARBINARY(32) NULL,
 original_diagnostic_sha256 VARBINARY(32) NULL,
 original_diagnostic_identity LONGBLOB NULL,
 planned_at DATETIME(6) NOT NULL,
 fixed_lease_until DATETIME(6) NOT NULL,
 result_at DATETIME(6) NULL,
 operation_fingerprint VARBINARY(32) NOT NULL,
 PRIMARY KEY (binding_uuid,operation_uuid),
 KEY ix_op_workflow (binding_uuid,workflow_uuid,step),
 CONSTRAINT ck_f3_04_01 CHECK (step IN ('RESERVE','CANCEL','ENTITLEMENT_REGISTER','DOMAIN_MUTATION','REARM','ACTIVATE','RECALL_CHECKPOINT','ABORT_CLEANUP','MOVE_PREPARE','MOVE_COMMIT','MOVE_CLEANUP','MOVE_REMEDIATE','SOURCE_CAPTURE','SOURCE_PREPARE','SOURCE_WRITE_PREPARE','SOURCE_WRITE_RESULT','SOURCE_WRITE_RECONCILE','SOURCE_CUTOVER','SOURCE_RETIRE','SOURCE_REMEDIATE','RESERVATION_RELEASE','SCRIPT_PUBLICATION')),
 CONSTRAINT ck_f3_04_02 CHECK (operation_state IN ('RESERVED','COMMITTED','CANCELLED')),
 CONSTRAINT ck_f3_04_03 CHECK (context_encoding='FP_RECOVERY_OPERATION_BE_V1'),
 CONSTRAINT ck_f3_04_04 CHECK (boot_generation>0),
 CONSTRAINT ck_f3_04_05 CHECK (connection_id>0),
 CONSTRAINT ck_f3_04_06 CHECK (((original_diagnostic_sha256 IS NULL AND original_diagnostic_identity IS NULL) OR (original_diagnostic_sha256 IS NOT NULL AND original_diagnostic_identity IS NOT NULL))),
 CONSTRAINT ck_f3_04_07 CHECK ((operation_state='RESERVED' AND result_at IS NULL AND settled_by_operation_uuid IS NULL AND commit_witness_bytes IS NULL AND commit_witness_sha256 IS NULL) OR (operation_state='COMMITTED' AND result_at IS NOT NULL AND settled_by_operation_uuid IS NULL AND commit_witness_bytes IS NOT NULL AND commit_witness_sha256 IS NOT NULL) OR (operation_state='CANCELLED' AND result_at IS NOT NULL AND settled_by_operation_uuid IS NOT NULL AND commit_witness_bytes IS NOT NULL AND commit_witness_sha256 IS NOT NULL)),
 CONSTRAINT ck_f3_04_08 CHECK (reserve_operation_uuid IS NULL OR reserve_operation_uuid<>operation_uuid),
 CONSTRAINT ck_f3_04_09 CHECK (previous_step_operation_uuid IS NULL OR previous_step_operation_uuid<>operation_uuid),
 CONSTRAINT ck_f3_04_10 CHECK (settled_by_operation_uuid IS NULL OR settled_by_operation_uuid<>operation_uuid),
 CONSTRAINT ck_f3_04_11 CHECK (original_diagnostic_identity IS NULL OR original_diagnostic_sha256=UNHEX(SHA2(original_diagnostic_identity,256))),
 CONSTRAINT ck_f3_04_12 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_04_13 CHECK (OCTET_LENGTH(operation_uuid)=16 AND operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_04_14 CHECK (OCTET_LENGTH(workflow_uuid)=16 AND workflow_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_04_15 CHECK (pod_uuid IS NULL OR (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_04_16 CHECK (attempt_uuid IS NULL OR (OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_04_17 CHECK (footprint_uuid IS NULL OR (OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_04_18 CHECK (source_unit_uuid IS NULL OR (OCTET_LENGTH(source_unit_uuid)=16 AND source_unit_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_04_19 CHECK (forest_uuid IS NULL OR (OCTET_LENGTH(forest_uuid)=16 AND forest_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_04_20 CHECK (OCTET_LENGTH(boot_uuid)=16 AND boot_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_04_21 CHECK (boot_generation>=0),
 CONSTRAINT ck_f3_04_22 CHECK (OCTET_LENGTH(connection_generation_uuid)=16 AND connection_generation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_04_23 CHECK (connection_id>=0),
 CONSTRAINT ck_f3_04_24 CHECK (fence_epoch>=0),
 CONSTRAINT ck_f3_04_25 CHECK (OCTET_LENGTH(request_identity_bytes)>0),
 CONSTRAINT ck_f3_04_26 CHECK (OCTET_LENGTH(request_identity_sha256)=32),
 CONSTRAINT ck_f3_04_27 CHECK (expected_domain_version IS NULL OR expected_domain_version>=0),
 CONSTRAINT ck_f3_04_28 CHECK (expected_locator_revision IS NULL OR expected_locator_revision>=0),
 CONSTRAINT ck_f3_04_29 CHECK (expected_source_revision IS NULL OR expected_source_revision>=0),
 CONSTRAINT ck_f3_04_30 CHECK (OCTET_LENGTH(original_context_bytes)>0),
 CONSTRAINT ck_f3_04_31 CHECK (OCTET_LENGTH(original_context_sha256)=32),
 CONSTRAINT ck_f3_04_32 CHECK (OCTET_LENGTH(scope_bytes)>0),
 CONSTRAINT ck_f3_04_33 CHECK (OCTET_LENGTH(scope_sha256)=32),
 CONSTRAINT ck_f3_04_34 CHECK (OCTET_LENGTH(preimage_bytes)>0),
 CONSTRAINT ck_f3_04_35 CHECK (OCTET_LENGTH(preimage_sha256)=32),
 CONSTRAINT ck_f3_04_36 CHECK (OCTET_LENGTH(target_bytes)>0),
 CONSTRAINT ck_f3_04_37 CHECK (OCTET_LENGTH(target_sha256)=32),
 CONSTRAINT ck_f3_04_38 CHECK (reserve_operation_uuid IS NULL OR (OCTET_LENGTH(reserve_operation_uuid)=16 AND reserve_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_04_39 CHECK (previous_step_operation_uuid IS NULL OR (OCTET_LENGTH(previous_step_operation_uuid)=16 AND previous_step_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_04_40 CHECK (settled_by_operation_uuid IS NULL OR (OCTET_LENGTH(settled_by_operation_uuid)=16 AND settled_by_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_04_41 CHECK (commit_witness_sha256 IS NULL OR OCTET_LENGTH(commit_witness_sha256)=32),
 CONSTRAINT ck_f3_04_42 CHECK (original_diagnostic_sha256 IS NULL OR OCTET_LENGTH(original_diagnostic_sha256)=32),
 CONSTRAINT ck_f3_04_43 CHECK (OCTET_LENGTH(operation_fingerprint)=32),
 CONSTRAINT ck_f3_04_44 CHECK (request_identity_sha256=UNHEX(SHA2(request_identity_bytes,256))),
 CONSTRAINT ck_f3_04_45 CHECK (original_context_sha256=UNHEX(SHA2(original_context_bytes,256))),
 CONSTRAINT ck_f3_04_46 CHECK (scope_sha256=UNHEX(SHA2(scope_bytes,256))),
 CONSTRAINT ck_f3_04_47 CHECK (preimage_sha256=UNHEX(SHA2(preimage_bytes,256))),
 CONSTRAINT ck_f3_04_48 CHECK (target_sha256=UNHEX(SHA2(target_bytes,256))),
 CONSTRAINT ck_f3_04_49 CHECK (commit_witness_bytes IS NULL OR (commit_witness_sha256 IS NOT NULL AND commit_witness_sha256=UNHEX(SHA2(commit_witness_bytes,256)))),
 CONSTRAINT fk_op_binding FOREIGN KEY (binding_uuid) REFERENCES physical_recovery_binding (binding_uuid),
 CONSTRAINT fk_op_reserve FOREIGN KEY (binding_uuid,reserve_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_op_previous FOREIGN KEY (binding_uuid,previous_step_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_op_settled FOREIGN KEY (binding_uuid,settled_by_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 5,7,8: floating_pod_state; immediate default NO ACTION.
CREATE TABLE floating_pod_state (
 uuid VARBINARY(16) NOT NULL,
 owner_uuid VARBINARY(16) NULL,
 state_version BIGINT NOT NULL,
 world_name LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 pos_x DOUBLE NOT NULL,
 pos_y DOUBLE NOT NULL,
 pos_z DOUBLE NOT NULL,
 yaw DOUBLE NOT NULL,
 pitch DOUBLE NOT NULL,
 is_active TINYINT NOT NULL,
 last_updated BIGINT NOT NULL,
 dimension_id INT NULL,
 operating_mode VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 binding_uuid VARBINARY(16) NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (uuid),
 KEY ix_domain_owner (owner_uuid),
 KEY ix_domain_active (is_active),
 KEY ix_domain_updated (last_updated),
 KEY ix_domain_owner_active (owner_uuid,is_active),
 CONSTRAINT ck_f3_05_01 CHECK (is_active IN (0,1)),
 CONSTRAINT ck_f3_05_02 CHECK (operating_mode IN ('NORMAL','WORLD_IDLE')),
 CONSTRAINT ck_f3_05_04 CHECK (owner_uuid IS NOT NULL OR is_active=0 OR operating_mode='WORLD_IDLE'),
 CONSTRAINT ck_f3_05_05 CHECK (ABS(pos_x) < 1.0e300),
 CONSTRAINT ck_f3_05_06 CHECK (ABS(pos_y) < 1.0e300),
 CONSTRAINT ck_f3_05_07 CHECK (ABS(pos_z) < 1.0e300),
 CONSTRAINT ck_f3_05_08 CHECK (ABS(yaw) < 1.0e30),
 CONSTRAINT ck_f3_05_09 CHECK (ABS(pitch) < 1.0e30),
 CONSTRAINT ck_f3_05_10 CHECK (OCTET_LENGTH(uuid)=16 AND uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_05_11 CHECK (owner_uuid IS NULL OR (OCTET_LENGTH(owner_uuid)=16 AND owner_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_05_12 CHECK (state_version>=0),
 CONSTRAINT ck_f3_05_13 CHECK (OCTET_LENGTH(world_name)>0),
 CONSTRAINT ck_f3_05_14 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_05_15 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT fk_domain_binding FOREIGN KEY (binding_uuid) REFERENCES physical_recovery_binding (binding_uuid),
 CONSTRAINT fk_domain_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 4,5: player_entitlement; immediate default NO ACTION.
CREATE TABLE player_entitlement (
 player_uuid VARBINARY(16) NOT NULL,
 registration_limit INT NOT NULL,
 simultaneous_active_limit INT NOT NULL,
 binding_uuid VARBINARY(16) NOT NULL,
 created_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (player_uuid),
 CONSTRAINT ck_f3_06_01 CHECK (registration_limit>=0),
 CONSTRAINT ck_f3_06_02 CHECK (simultaneous_active_limit>=0),
 CONSTRAINT ck_f3_06_03 CHECK (OCTET_LENGTH(player_uuid)=16 AND player_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_06_04 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_06_05 CHECK (OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT fk_entitlement_binding FOREIGN KEY (binding_uuid) REFERENCES physical_recovery_binding (binding_uuid),
 CONSTRAINT fk_entitlement_created FOREIGN KEY (binding_uuid,created_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 2,12: binding_bootstrap_intent; immediate default NO ACTION.
CREATE TABLE binding_bootstrap_intent (
 singleton_fixed1 TINYINT NOT NULL,
 bootstrap_operation_uuid VARBINARY(16) NOT NULL,
 prepare_write_uuid VARBINARY(16) NOT NULL,
 certify_write_uuid VARBINARY(16) NOT NULL,
 binding_uuid VARBINARY(16) NOT NULL,
 binding_format_version INT NOT NULL,
 provisioning_uuid VARBINARY(16) NOT NULL,
 source_world_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 provision_receipt_sha256 VARBINARY(32) NOT NULL,
 stage VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 marker_ack_sha256 VARBINARY(32) NULL,
 PRIMARY KEY (singleton_fixed1),
 CONSTRAINT ck_f3_07_01 CHECK (singleton_fixed1=1),
 CONSTRAINT ck_f3_07_02 CHECK (prepare_write_uuid<>certify_write_uuid),
 CONSTRAINT ck_f3_07_03 CHECK (binding_format_version=2),
 CONSTRAINT ck_f3_07_04 CHECK (stage IN ('PREPARED','COMPLETE')),
 CONSTRAINT ck_f3_07_05 CHECK ((stage='PREPARED' AND marker_ack_sha256 IS NULL) OR (stage='COMPLETE' AND marker_ack_sha256 IS NOT NULL)),
 CONSTRAINT ck_f3_07_06 CHECK (OCTET_LENGTH(bootstrap_operation_uuid)=16 AND bootstrap_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_07_07 CHECK (OCTET_LENGTH(prepare_write_uuid)=16 AND prepare_write_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_07_08 CHECK (OCTET_LENGTH(certify_write_uuid)=16 AND certify_write_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_07_09 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_07_10 CHECK (OCTET_LENGTH(provisioning_uuid)=16 AND provisioning_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_07_11 CHECK (OCTET_LENGTH(source_world_identity)>0),
 CONSTRAINT ck_f3_07_12 CHECK (OCTET_LENGTH(provision_receipt_sha256)=32),
 CONSTRAINT ck_f3_07_13 CHECK (marker_ack_sha256 IS NULL OR OCTET_LENGTH(marker_ack_sha256)=32)
) ENGINE=InnoDB;

-- METHOD sections 6: fp_recovery_reservation; immediate default NO ACTION.
CREATE TABLE fp_recovery_reservation (
 binding_uuid VARBINARY(16) NOT NULL,
 resource_kind VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 resource_key_hash VARBINARY(32) NOT NULL,
 resource_key LONGBLOB NOT NULL,
 reservation_revision BIGINT NOT NULL,
 workflow_uuid VARBINARY(16) NOT NULL,
 operation_uuid VARBINARY(16) NOT NULL,
 reservation_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 resource_pod_uuid VARBINARY(16) NULL,
 resource_unit_uuid VARBINARY(16) NULL,
 PRIMARY KEY (binding_uuid,resource_kind,resource_key_hash),
 UNIQUE KEY ux_reservation_pod (binding_uuid,resource_kind,resource_pod_uuid),
 UNIQUE KEY ux_reservation_unit (binding_uuid,resource_kind,resource_unit_uuid),
 CONSTRAINT ck_f3_08_01 CHECK (resource_kind IN ('POD','SOURCE_UNIT','CARRIER')),
 CONSTRAINT ck_f3_08_02 CHECK (reservation_state IN ('HELD','RELEASED')),
 CONSTRAINT ck_f3_08_03 CHECK (resource_key_hash=UNHEX(SHA2(resource_key,256))),
 CONSTRAINT ck_f3_08_04 CHECK ((resource_kind='POD' AND resource_pod_uuid IS NOT NULL AND resource_unit_uuid IS NULL) OR (resource_kind='SOURCE_UNIT' AND resource_pod_uuid IS NULL AND resource_unit_uuid IS NOT NULL) OR (resource_kind='CARRIER' AND resource_pod_uuid IS NULL AND resource_unit_uuid IS NULL)),
 CONSTRAINT ck_f3_08_05 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_08_06 CHECK (OCTET_LENGTH(resource_key_hash)=32),
 CONSTRAINT ck_f3_08_07 CHECK (OCTET_LENGTH(resource_key)>0),
 CONSTRAINT ck_f3_08_08 CHECK (reservation_revision>=0),
 CONSTRAINT ck_f3_08_09 CHECK (OCTET_LENGTH(workflow_uuid)=16 AND workflow_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_08_10 CHECK (OCTET_LENGTH(operation_uuid)=16 AND operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_08_11 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_08_12 CHECK (resource_pod_uuid IS NULL OR (OCTET_LENGTH(resource_pod_uuid)=16 AND resource_pod_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_08_13 CHECK (resource_unit_uuid IS NULL OR (OCTET_LENGTH(resource_unit_uuid)=16 AND resource_unit_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT fk_reservation_operation FOREIGN KEY (binding_uuid,operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_reservation_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 5,8,11: physical_recovery_footprint; immediate default NO ACTION.
CREATE TABLE physical_recovery_footprint (
 binding_uuid VARBINARY(16) NOT NULL,
 pod_uuid VARBINARY(16) NOT NULL,
 attempt_uuid VARBINARY(16) NOT NULL,
 footprint_uuid VARBINARY(16) NOT NULL,
 origin_version BIGINT NOT NULL,
 target_world_name LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 target_pos_x DOUBLE NOT NULL,
 target_pos_y DOUBLE NOT NULL,
 target_pos_z DOUBLE NOT NULL,
 target_yaw DOUBLE NOT NULL,
 target_pitch DOUBLE NOT NULL,
 target_dimension_id INT NOT NULL,
 center_chunk_x INT NOT NULL,
 center_chunk_z INT NOT NULL,
 purpose VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 seal_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 created_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT ck_f3_09_01 CHECK (purpose IN ('CREATE_TARGET','MOVE_TARGET','MIGRATED_UNVERIFIED')),
 CONSTRAINT ck_f3_09_02 CHECK (seal_state IN ('DRAFT','SEALED')),
 CONSTRAINT ck_f3_09_03 CHECK (ABS(target_pos_x) < 1.0e300),
 CONSTRAINT ck_f3_09_04 CHECK (ABS(target_pos_y) < 1.0e300),
 CONSTRAINT ck_f3_09_05 CHECK (ABS(target_pos_z) < 1.0e300),
 CONSTRAINT ck_f3_09_06 CHECK (ABS(target_yaw) < 1.0e30),
 CONSTRAINT ck_f3_09_07 CHECK (ABS(target_pitch) < 1.0e30),
 CONSTRAINT ck_f3_09_08 CHECK (center_chunk_x=FLOOR(target_pos_x/16)),
 CONSTRAINT ck_f3_09_09 CHECK (center_chunk_z=FLOOR(target_pos_z/16)),
 CONSTRAINT ck_f3_09_10 CHECK (center_chunk_x BETWEEN -2147483646 AND 2147483645),
 CONSTRAINT ck_f3_09_11 CHECK (center_chunk_z BETWEEN -2147483646 AND 2147483645),
 CONSTRAINT ck_f3_09_12 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_09_13 CHECK (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_09_14 CHECK (OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_09_15 CHECK (OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_09_16 CHECK (origin_version>=0),
 CONSTRAINT ck_f3_09_17 CHECK (OCTET_LENGTH(target_world_name)>0),
 CONSTRAINT ck_f3_09_18 CHECK (OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT fk_footprint_domain FOREIGN KEY (pod_uuid) REFERENCES floating_pod_state (uuid),
 CONSTRAINT fk_footprint_binding FOREIGN KEY (binding_uuid) REFERENCES physical_recovery_binding (binding_uuid),
 CONSTRAINT fk_footprint_created FOREIGN KEY (binding_uuid,created_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 5,8,11: physical_recovery_footprint_chunk; immediate default NO ACTION.
CREATE TABLE physical_recovery_footprint_chunk (
 binding_uuid VARBINARY(16) NOT NULL,
 pod_uuid VARBINARY(16) NOT NULL,
 attempt_uuid VARBINARY(16) NOT NULL,
 footprint_uuid VARBINARY(16) NOT NULL,
 dimension_id INT NOT NULL,
 chunk_x INT NOT NULL,
 chunk_z INT NOT NULL,
 PRIMARY KEY (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid,dimension_id,chunk_x,chunk_z),
 CONSTRAINT ck_f3_10_01 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_10_02 CHECK (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_10_03 CHECK (OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_10_04 CHECK (OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT fk_chunk_footprint FOREIGN KEY (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid) REFERENCES physical_recovery_footprint (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid)
) ENGINE=InnoDB;

-- METHOD sections 5,7: physical_recovery_intent; immediate default NO ACTION.
CREATE TABLE physical_recovery_intent (
 binding_uuid VARBINARY(16) NOT NULL,
 pod_uuid VARBINARY(16) NOT NULL,
 attempt_uuid VARBINARY(16) NOT NULL,
 origin VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 phase VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 target_footprint_uuid VARBINARY(16) NOT NULL,
 prepare_operation_uuid VARBINARY(16) NOT NULL,
 expected_owner_uuid VARBINARY(16) NULL,
 expected_domain_version BIGINT NOT NULL,
 predecessor_attempt_uuid VARBINARY(16) NULL,
 predecessor_footprint_uuid VARBINARY(16) NULL,
 source_completion_uuid VARBINARY(16) NULL,
 fresh_absence_proof_bytes LONGBLOB NULL,
 fresh_absence_proof_sha256 VARBINARY(32) NULL,
 source_kind VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
 source_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
 source_identity_sha256 VARBINARY(32) GENERATED ALWAYS AS (UNHEX(SHA2(CAST(source_identity AS BINARY),256))) STORED,
 catalog_generation VARBINARY(16) NULL,
 create_operation_uuid VARBINARY(16) NULL,
 source_unit_uuid VARBINARY(16) NULL,
 source_forest_uuid VARBINARY(16) NULL,
 activation_operation_uuid VARBINARY(16) NULL,
 committed_domain_version BIGINT NULL,
 committed_at BIGINT NULL,
 committed_world_name LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
 committed_pos_x DOUBLE NULL,
 committed_pos_y DOUBLE NULL,
 committed_pos_z DOUBLE NULL,
 committed_yaw DOUBLE NULL,
 committed_pitch DOUBLE NULL,
 committed_dimension_id INT NULL,
 cleanup_operation_uuid VARBINARY(16) NULL,
 cleanup_proof_bytes LONGBLOB NULL,
 cleanup_proof_sha256 VARBINARY(32) NULL,
 intent_revision BIGINT NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,pod_uuid,attempt_uuid),
 UNIQUE KEY ux_intent_source (binding_uuid,source_kind,catalog_generation,source_identity_sha256),
 UNIQUE KEY ux_intent_create (binding_uuid,create_operation_uuid),
 UNIQUE KEY ux_intent_completion (binding_uuid,source_completion_uuid),
 CONSTRAINT ck_f3_11_01 CHECK (origin IN ('UNBOUND_ADOPTION','DEBUG_ITEM_CREATE','OWNED_RELEASE','REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP','REARM_AFTER_FRESH_ABSENCE')),
 CONSTRAINT ck_f3_11_02 CHECK (phase IN ('PREPARED','ACTIVATED','CLEANUP_PENDING','CLEANUP_VERIFIED')),
 CONSTRAINT ck_f3_11_03 CHECK (((predecessor_attempt_uuid IS NULL AND predecessor_footprint_uuid IS NULL) OR (predecessor_attempt_uuid IS NOT NULL AND predecessor_footprint_uuid IS NOT NULL))),
 CONSTRAINT ck_f3_11_04 CHECK (((source_unit_uuid IS NULL AND source_forest_uuid IS NULL) OR (source_unit_uuid IS NOT NULL AND source_forest_uuid IS NOT NULL))),
 CONSTRAINT ck_f3_11_05 CHECK (((fresh_absence_proof_bytes IS NULL AND fresh_absence_proof_sha256 IS NULL) OR (fresh_absence_proof_bytes IS NOT NULL AND fresh_absence_proof_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_11_06 CHECK (((cleanup_proof_bytes IS NULL AND cleanup_proof_sha256 IS NULL) OR (cleanup_proof_bytes IS NOT NULL AND cleanup_proof_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_11_07 CHECK (origin NOT IN ('UNBOUND_ADOPTION','DEBUG_ITEM_CREATE') OR expected_domain_version=0),
 CONSTRAINT ck_f3_11_08 CHECK (origin<>'UNBOUND_ADOPTION' OR (source_kind IS NOT NULL AND source_kind IN ('FRESH','LEGACY_UNREGISTERED') AND source_identity IS NOT NULL AND catalog_generation IS NOT NULL AND create_operation_uuid IS NULL)),
 CONSTRAINT ck_f3_11_09 CHECK (origin<>'DEBUG_ITEM_CREATE' OR (source_kind IS NULL AND source_identity IS NULL AND catalog_generation IS NULL AND create_operation_uuid IS NOT NULL)),
 CONSTRAINT ck_f3_11_10 CHECK (origin NOT IN ('REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP') OR (source_completion_uuid IS NOT NULL AND predecessor_attempt_uuid IS NOT NULL AND predecessor_footprint_uuid IS NOT NULL AND fresh_absence_proof_bytes IS NULL)),
 CONSTRAINT ck_f3_11_11 CHECK (origin<>'REARM_AFTER_FRESH_ABSENCE' OR (fresh_absence_proof_bytes IS NOT NULL AND fresh_absence_proof_sha256 IS NOT NULL AND source_completion_uuid IS NULL AND predecessor_attempt_uuid IS NOT NULL AND predecessor_footprint_uuid IS NOT NULL)),
 CONSTRAINT ck_f3_11_12 CHECK (origin<>'OWNED_RELEASE' OR expected_owner_uuid IS NOT NULL),
 CONSTRAINT ck_f3_11_13 CHECK ((phase<>'ACTIVATED' AND activation_operation_uuid IS NULL AND committed_domain_version IS NULL AND committed_at IS NULL AND committed_world_name IS NULL AND committed_pos_x IS NULL AND committed_pos_y IS NULL AND committed_pos_z IS NULL AND committed_yaw IS NULL AND committed_pitch IS NULL AND committed_dimension_id IS NULL) OR (phase='ACTIVATED' AND activation_operation_uuid IS NOT NULL AND committed_domain_version IS NOT NULL AND committed_domain_version=expected_domain_version+1 AND committed_at IS NOT NULL AND committed_world_name IS NOT NULL AND committed_pos_x IS NOT NULL AND committed_pos_y IS NOT NULL AND committed_pos_z IS NOT NULL AND committed_yaw IS NOT NULL AND committed_pitch IS NOT NULL AND committed_dimension_id IS NOT NULL)),
 CONSTRAINT ck_f3_11_14 CHECK (phase<>'CLEANUP_VERIFIED' OR (cleanup_operation_uuid IS NOT NULL AND cleanup_proof_bytes IS NOT NULL AND cleanup_proof_sha256 IS NOT NULL)),
 CONSTRAINT ck_f3_11_15 CHECK (committed_pos_x IS NULL OR ABS(committed_pos_x) < 1.0e300),
 CONSTRAINT ck_f3_11_16 CHECK (committed_pos_y IS NULL OR ABS(committed_pos_y) < 1.0e300),
 CONSTRAINT ck_f3_11_17 CHECK (committed_pos_z IS NULL OR ABS(committed_pos_z) < 1.0e300),
 CONSTRAINT ck_f3_11_18 CHECK (committed_yaw IS NULL OR ABS(committed_yaw) < 1.0e30),
 CONSTRAINT ck_f3_11_19 CHECK (committed_pitch IS NULL OR ABS(committed_pitch) < 1.0e30),
 CONSTRAINT ck_f3_11_20 CHECK (origin IN ('REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP') OR source_completion_uuid IS NULL),
 CONSTRAINT ck_f3_11_21 CHECK (origin='REARM_AFTER_FRESH_ABSENCE' OR fresh_absence_proof_bytes IS NULL),
 CONSTRAINT ck_f3_11_22 CHECK (origin IN ('REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP','REARM_AFTER_FRESH_ABSENCE') OR predecessor_attempt_uuid IS NULL),
 CONSTRAINT ck_f3_11_23 CHECK (origin='UNBOUND_ADOPTION' OR (source_kind IS NULL AND source_identity IS NULL AND catalog_generation IS NULL)),
 CONSTRAINT ck_f3_11_24 CHECK (origin='DEBUG_ITEM_CREATE' OR create_operation_uuid IS NULL),
 CONSTRAINT ck_f3_11_25 CHECK (phase='CLEANUP_VERIFIED' OR cleanup_proof_bytes IS NULL),
 CONSTRAINT ck_f3_11_26 CHECK (phase IN ('CLEANUP_PENDING','CLEANUP_VERIFIED') OR cleanup_operation_uuid IS NULL),
 CONSTRAINT ck_f3_11_27 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_11_28 CHECK (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_11_29 CHECK (OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_11_30 CHECK (OCTET_LENGTH(target_footprint_uuid)=16 AND target_footprint_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_11_31 CHECK (OCTET_LENGTH(prepare_operation_uuid)=16 AND prepare_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_11_32 CHECK (expected_owner_uuid IS NULL OR (OCTET_LENGTH(expected_owner_uuid)=16 AND expected_owner_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_33 CHECK (expected_domain_version>=0),
 CONSTRAINT ck_f3_11_34 CHECK (predecessor_attempt_uuid IS NULL OR (OCTET_LENGTH(predecessor_attempt_uuid)=16 AND predecessor_attempt_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_35 CHECK (predecessor_footprint_uuid IS NULL OR (OCTET_LENGTH(predecessor_footprint_uuid)=16 AND predecessor_footprint_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_36 CHECK (source_completion_uuid IS NULL OR (OCTET_LENGTH(source_completion_uuid)=16 AND source_completion_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_37 CHECK (fresh_absence_proof_sha256 IS NULL OR OCTET_LENGTH(fresh_absence_proof_sha256)=32),
 CONSTRAINT ck_f3_11_38 CHECK (catalog_generation IS NULL OR (OCTET_LENGTH(catalog_generation)=16 AND catalog_generation<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_39 CHECK (create_operation_uuid IS NULL OR (OCTET_LENGTH(create_operation_uuid)=16 AND create_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_40 CHECK (source_unit_uuid IS NULL OR (OCTET_LENGTH(source_unit_uuid)=16 AND source_unit_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_41 CHECK (source_forest_uuid IS NULL OR (OCTET_LENGTH(source_forest_uuid)=16 AND source_forest_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_42 CHECK (activation_operation_uuid IS NULL OR (OCTET_LENGTH(activation_operation_uuid)=16 AND activation_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_43 CHECK (committed_domain_version IS NULL OR committed_domain_version>=0),
 CONSTRAINT ck_f3_11_44 CHECK (cleanup_operation_uuid IS NULL OR (OCTET_LENGTH(cleanup_operation_uuid)=16 AND cleanup_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_11_45 CHECK (cleanup_proof_sha256 IS NULL OR OCTET_LENGTH(cleanup_proof_sha256)=32),
 CONSTRAINT ck_f3_11_46 CHECK (intent_revision>=0),
 CONSTRAINT ck_f3_11_47 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_11_48 CHECK (fresh_absence_proof_bytes IS NULL OR (fresh_absence_proof_sha256 IS NOT NULL AND fresh_absence_proof_sha256=UNHEX(SHA2(fresh_absence_proof_bytes,256)))),
 CONSTRAINT ck_f3_11_49 CHECK (cleanup_proof_bytes IS NULL OR (cleanup_proof_sha256 IS NOT NULL AND cleanup_proof_sha256=UNHEX(SHA2(cleanup_proof_bytes,256)))),
 CONSTRAINT fk_intent_target FOREIGN KEY (binding_uuid,pod_uuid,attempt_uuid,target_footprint_uuid) REFERENCES physical_recovery_footprint (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT fk_intent_predecessor FOREIGN KEY (binding_uuid,pod_uuid,predecessor_attempt_uuid,predecessor_footprint_uuid) REFERENCES physical_recovery_footprint (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT fk_intent_prepare FOREIGN KEY (binding_uuid,prepare_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_intent_activation FOREIGN KEY (binding_uuid,activation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_intent_cleanup FOREIGN KEY (binding_uuid,cleanup_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_intent_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 5,7,8: physical_recovery_locator; immediate default NO ACTION.
CREATE TABLE physical_recovery_locator (
 binding_uuid VARBINARY(16) NOT NULL,
 pod_uuid VARBINARY(16) NOT NULL,
 current_attempt_uuid VARBINARY(16) NOT NULL,
 current_footprint_uuid VARBINARY(16) NOT NULL,
 locator_state VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 reason_code LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 domain_version BIGINT NULL,
 target_dimension_id INT NOT NULL,
 locator_revision BIGINT NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,pod_uuid),
 UNIQUE KEY ux_locator_attempt (binding_uuid,current_attempt_uuid),
 CONSTRAINT ck_f3_12_01 CHECK (locator_state IN ('CREATE_PREPARED','UNBOUND_ADOPTION_PREPARED','DEBUG_ITEM_CREATE_PREPARED','ROW_ACTIVE','ROW_INACTIVE_PENDING','ROW_INACTIVE_VERIFIED','ABSENT_PENDING','ABSENT_VERIFIED','LEGACY_UNCERTIFIED','QUARANTINED')),
 CONSTRAINT ck_f3_12_02 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_12_03 CHECK (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_12_04 CHECK (OCTET_LENGTH(current_attempt_uuid)=16 AND current_attempt_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_12_05 CHECK (OCTET_LENGTH(current_footprint_uuid)=16 AND current_footprint_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_12_06 CHECK (OCTET_LENGTH(reason_code)>0),
 CONSTRAINT ck_f3_12_07 CHECK (domain_version IS NULL OR domain_version>=0),
 CONSTRAINT ck_f3_12_08 CHECK (locator_revision>=0),
 CONSTRAINT ck_f3_12_09 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT fk_locator_target FOREIGN KEY (binding_uuid,pod_uuid,current_attempt_uuid,current_footprint_uuid) REFERENCES physical_recovery_footprint (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT fk_locator_intent FOREIGN KEY (binding_uuid,pod_uuid,current_attempt_uuid) REFERENCES physical_recovery_intent (binding_uuid,pod_uuid,attempt_uuid),
 CONSTRAINT fk_locator_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 5,8: physical_recovery_obligation; immediate default NO ACTION.
CREATE TABLE physical_recovery_obligation (
 binding_uuid VARBINARY(16) NOT NULL,
 pod_uuid VARBINARY(16) NOT NULL,
 attempt_uuid VARBINARY(16) NOT NULL,
 footprint_uuid VARBINARY(16) NOT NULL,
 origin_version BIGINT NOT NULL,
 purpose VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 created_operation_uuid VARBINARY(16) NOT NULL,
 attempt_retirement_operation_uuid VARBINARY(16) NULL,
 obligation_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 obligation_revision BIGINT NOT NULL,
 verified_operation_uuid VARBINARY(16) NULL,
 verification_proof_bytes LONGBLOB NULL,
 verification_proof_sha256 VARBINARY(32) NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT ck_f3_13_01 CHECK (purpose IN ('RETIRED_ATTEMPT','ABORTED_CREATE','SAME_ATTEMPT_CHECKPOINT')),
 CONSTRAINT ck_f3_13_02 CHECK (obligation_state IN ('PENDING','VERIFIED')),
 CONSTRAINT ck_f3_13_03 CHECK (((verification_proof_bytes IS NULL AND verification_proof_sha256 IS NULL) OR (verification_proof_bytes IS NOT NULL AND verification_proof_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_13_04 CHECK ((obligation_state='PENDING' AND verified_operation_uuid IS NULL AND verification_proof_bytes IS NULL) OR (obligation_state='VERIFIED' AND verified_operation_uuid IS NOT NULL AND verification_proof_bytes IS NOT NULL)),
 CONSTRAINT ck_f3_13_05 CHECK (purpose<>'RETIRED_ATTEMPT' OR attempt_retirement_operation_uuid IS NOT NULL),
 CONSTRAINT ck_f3_13_06 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_13_07 CHECK (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_13_08 CHECK (OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_13_09 CHECK (OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_13_10 CHECK (origin_version>=0),
 CONSTRAINT ck_f3_13_11 CHECK (OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_13_12 CHECK (attempt_retirement_operation_uuid IS NULL OR (OCTET_LENGTH(attempt_retirement_operation_uuid)=16 AND attempt_retirement_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_13_13 CHECK (obligation_revision>=0),
 CONSTRAINT ck_f3_13_14 CHECK (verified_operation_uuid IS NULL OR (OCTET_LENGTH(verified_operation_uuid)=16 AND verified_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_13_15 CHECK (verification_proof_sha256 IS NULL OR OCTET_LENGTH(verification_proof_sha256)=32),
 CONSTRAINT ck_f3_13_16 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_13_17 CHECK (verification_proof_bytes IS NULL OR (verification_proof_sha256 IS NOT NULL AND verification_proof_sha256=UNHEX(SHA2(verification_proof_bytes,256)))),
 CONSTRAINT fk_obligation_footprint FOREIGN KEY (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid) REFERENCES physical_recovery_footprint (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT fk_obligation_created FOREIGN KEY (binding_uuid,created_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_obligation_retirement FOREIGN KEY (binding_uuid,attempt_retirement_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_obligation_verified FOREIGN KEY (binding_uuid,verified_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_obligation_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 7: fp_lifecycle_completion; immediate default NO ACTION.
CREATE TABLE fp_lifecycle_completion (
 binding_uuid VARBINARY(16) NOT NULL,
 receipt_uuid VARBINARY(16) NOT NULL,
 completion_operation_uuid VARBINARY(16) NOT NULL,
 completion_kind VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 boot_uuid VARBINARY(16) NOT NULL,
 boot_generation BIGINT NOT NULL,
 world_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 source_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 pod_uuid VARBINARY(16) NOT NULL,
 owner_uuid VARBINARY(16) NULL,
 attempt_uuid VARBINARY(16) NOT NULL,
 footprint_uuid VARBINARY(16) NOT NULL,
 before_domain_version BIGINT NOT NULL,
 after_domain_version BIGINT NOT NULL,
 removal_result_bytes LONGBLOB NOT NULL,
 removal_result_sha256 VARBINARY(32) NOT NULL,
 ticket_result_bytes LONGBLOB NOT NULL,
 ticket_result_sha256 VARBINARY(32) NOT NULL,
 checkpoint_bytes LONGBLOB NOT NULL,
 checkpoint_sha256 VARBINARY(32) NOT NULL,
 original_context_bytes LONGBLOB NOT NULL,
 original_context_sha256 VARBINARY(32) NOT NULL,
 allowed_use VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 completion_state VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 consumed_by_operation_uuid VARBINARY(16) NULL,
 completion_revision BIGINT NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,receipt_uuid),
 UNIQUE KEY ux_completion_operation (binding_uuid,completion_operation_uuid),
 CONSTRAINT ck_f3_14_01 CHECK (completion_kind IN ('RECALL','ABORTED_CREATE_CLEANUP')),
 CONSTRAINT ck_f3_14_02 CHECK (allowed_use IN ('REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP')),
 CONSTRAINT ck_f3_14_03 CHECK (completion_state='CHECKPOINT_COMMITTED'),
 CONSTRAINT ck_f3_14_04 CHECK (after_domain_version=before_domain_version+1),
 CONSTRAINT ck_f3_14_05 CHECK (boot_generation>0),
 CONSTRAINT ck_f3_14_06 CHECK ((completion_kind='RECALL' AND allowed_use='REARM_AFTER_RECALL') OR (completion_kind='ABORTED_CREATE_CLEANUP' AND allowed_use='REARM_AFTER_ABORT_CLEANUP')),
 CONSTRAINT ck_f3_14_07 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_14_08 CHECK (OCTET_LENGTH(receipt_uuid)=16 AND receipt_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_14_09 CHECK (OCTET_LENGTH(completion_operation_uuid)=16 AND completion_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_14_10 CHECK (OCTET_LENGTH(boot_uuid)=16 AND boot_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_14_11 CHECK (boot_generation>=0),
 CONSTRAINT ck_f3_14_12 CHECK (OCTET_LENGTH(world_identity)>0),
 CONSTRAINT ck_f3_14_13 CHECK (OCTET_LENGTH(source_identity)>0),
 CONSTRAINT ck_f3_14_14 CHECK (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_14_15 CHECK (owner_uuid IS NULL OR (OCTET_LENGTH(owner_uuid)=16 AND owner_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_14_16 CHECK (OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_14_17 CHECK (OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_14_18 CHECK (before_domain_version>=0),
 CONSTRAINT ck_f3_14_19 CHECK (after_domain_version>=0),
 CONSTRAINT ck_f3_14_20 CHECK (OCTET_LENGTH(removal_result_bytes)>0),
 CONSTRAINT ck_f3_14_21 CHECK (OCTET_LENGTH(removal_result_sha256)=32),
 CONSTRAINT ck_f3_14_22 CHECK (OCTET_LENGTH(ticket_result_bytes)>0),
 CONSTRAINT ck_f3_14_23 CHECK (OCTET_LENGTH(ticket_result_sha256)=32),
 CONSTRAINT ck_f3_14_24 CHECK (OCTET_LENGTH(checkpoint_bytes)>0),
 CONSTRAINT ck_f3_14_25 CHECK (OCTET_LENGTH(checkpoint_sha256)=32),
 CONSTRAINT ck_f3_14_26 CHECK (OCTET_LENGTH(original_context_bytes)>0),
 CONSTRAINT ck_f3_14_27 CHECK (OCTET_LENGTH(original_context_sha256)=32),
 CONSTRAINT ck_f3_14_28 CHECK (consumed_by_operation_uuid IS NULL OR (OCTET_LENGTH(consumed_by_operation_uuid)=16 AND consumed_by_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_14_29 CHECK (completion_revision>=0),
 CONSTRAINT ck_f3_14_30 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_14_31 CHECK (removal_result_sha256=UNHEX(SHA2(removal_result_bytes,256))),
 CONSTRAINT ck_f3_14_32 CHECK (ticket_result_sha256=UNHEX(SHA2(ticket_result_bytes,256))),
 CONSTRAINT ck_f3_14_33 CHECK (checkpoint_sha256=UNHEX(SHA2(checkpoint_bytes,256))),
 CONSTRAINT ck_f3_14_34 CHECK (original_context_sha256=UNHEX(SHA2(original_context_bytes,256))),
 CONSTRAINT fk_completion_footprint FOREIGN KEY (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid) REFERENCES physical_recovery_footprint (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT fk_completion_operation FOREIGN KEY (binding_uuid,completion_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_completion_consumed FOREIGN KEY (binding_uuid,consumed_by_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_completion_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 8: physical_footprint_transfer; immediate default NO ACTION.
CREATE TABLE physical_footprint_transfer (
 binding_uuid VARBINARY(16) NOT NULL,
 move_uuid VARBINARY(16) NOT NULL,
 pod_uuid VARBINARY(16) NOT NULL,
 attempt_uuid VARBINARY(16) NOT NULL,
 from_footprint_uuid VARBINARY(16) NOT NULL,
 to_footprint_uuid VARBINARY(16) NOT NULL,
 expected_domain_version BIGINT NOT NULL,
 target_domain_version BIGINT NOT NULL,
 from_world_name LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 from_pos_x DOUBLE NOT NULL,
 from_pos_y DOUBLE NOT NULL,
 from_pos_z DOUBLE NOT NULL,
 from_yaw DOUBLE NOT NULL,
 from_pitch DOUBLE NOT NULL,
 from_dimension_id INT NOT NULL,
 to_world_name LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 to_pos_x DOUBLE NOT NULL,
 to_pos_y DOUBLE NOT NULL,
 to_pos_z DOUBLE NOT NULL,
 to_yaw DOUBLE NOT NULL,
 to_pitch DOUBLE NOT NULL,
 to_dimension_id INT NOT NULL,
 prepare_operation_uuid VARBINARY(16) NOT NULL,
 commit_operation_uuid VARBINARY(16) NULL,
 cleanup_operation_uuid VARBINARY(16) NULL,
 phase VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 move_plan_bytes LONGBLOB NOT NULL,
 move_plan_sha256 VARBINARY(32) NOT NULL,
 cleanup_proof_bytes LONGBLOB NULL,
 cleanup_proof_sha256 VARBINARY(32) NULL,
 transfer_revision BIGINT NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,move_uuid),
 CONSTRAINT ck_f3_15_01 CHECK (from_footprint_uuid<>to_footprint_uuid),
 CONSTRAINT ck_f3_15_02 CHECK (target_domain_version=expected_domain_version+1),
 CONSTRAINT ck_f3_15_03 CHECK (from_dimension_id=to_dimension_id),
 CONSTRAINT ck_f3_15_04 CHECK (phase IN ('MOVE_PREPARED','MOVE_COMMITTED_CLEANUP_PENDING','MOVE_COMPLETED','MOVE_REMEDIATION_PENDING')),
 CONSTRAINT ck_f3_15_05 CHECK (((cleanup_proof_bytes IS NULL AND cleanup_proof_sha256 IS NULL) OR (cleanup_proof_bytes IS NOT NULL AND cleanup_proof_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_15_06 CHECK (phase NOT IN ('MOVE_COMMITTED_CLEANUP_PENDING','MOVE_COMPLETED') OR commit_operation_uuid IS NOT NULL),
 CONSTRAINT ck_f3_15_07 CHECK (phase<>'MOVE_COMPLETED' OR (cleanup_operation_uuid IS NOT NULL AND cleanup_proof_bytes IS NOT NULL)),
 CONSTRAINT ck_f3_15_08 CHECK (ABS(from_pos_x) < 1.0e300),
 CONSTRAINT ck_f3_15_09 CHECK (ABS(from_pos_y) < 1.0e300),
 CONSTRAINT ck_f3_15_10 CHECK (ABS(from_pos_z) < 1.0e300),
 CONSTRAINT ck_f3_15_11 CHECK (ABS(from_yaw) < 1.0e30),
 CONSTRAINT ck_f3_15_12 CHECK (ABS(from_pitch) < 1.0e30),
 CONSTRAINT ck_f3_15_13 CHECK (ABS(to_pos_x) < 1.0e300),
 CONSTRAINT ck_f3_15_14 CHECK (ABS(to_pos_y) < 1.0e300),
 CONSTRAINT ck_f3_15_15 CHECK (ABS(to_pos_z) < 1.0e300),
 CONSTRAINT ck_f3_15_16 CHECK (ABS(to_yaw) < 1.0e30),
 CONSTRAINT ck_f3_15_17 CHECK (ABS(to_pitch) < 1.0e30),
 CONSTRAINT ck_f3_15_18 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_15_19 CHECK (OCTET_LENGTH(move_uuid)=16 AND move_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_15_20 CHECK (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_15_21 CHECK (OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_15_22 CHECK (OCTET_LENGTH(from_footprint_uuid)=16 AND from_footprint_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_15_23 CHECK (OCTET_LENGTH(to_footprint_uuid)=16 AND to_footprint_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_15_24 CHECK (expected_domain_version>=0),
 CONSTRAINT ck_f3_15_25 CHECK (target_domain_version>=0),
 CONSTRAINT ck_f3_15_26 CHECK (OCTET_LENGTH(from_world_name)>0),
 CONSTRAINT ck_f3_15_27 CHECK (OCTET_LENGTH(to_world_name)>0),
 CONSTRAINT ck_f3_15_28 CHECK (OCTET_LENGTH(prepare_operation_uuid)=16 AND prepare_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_15_29 CHECK (commit_operation_uuid IS NULL OR (OCTET_LENGTH(commit_operation_uuid)=16 AND commit_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_15_30 CHECK (cleanup_operation_uuid IS NULL OR (OCTET_LENGTH(cleanup_operation_uuid)=16 AND cleanup_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_15_31 CHECK (OCTET_LENGTH(move_plan_bytes)>0),
 CONSTRAINT ck_f3_15_32 CHECK (OCTET_LENGTH(move_plan_sha256)=32),
 CONSTRAINT ck_f3_15_33 CHECK (cleanup_proof_sha256 IS NULL OR OCTET_LENGTH(cleanup_proof_sha256)=32),
 CONSTRAINT ck_f3_15_34 CHECK (transfer_revision>=0),
 CONSTRAINT ck_f3_15_35 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_15_36 CHECK (move_plan_sha256=UNHEX(SHA2(move_plan_bytes,256))),
 CONSTRAINT ck_f3_15_37 CHECK (cleanup_proof_bytes IS NULL OR (cleanup_proof_sha256 IS NOT NULL AND cleanup_proof_sha256=UNHEX(SHA2(cleanup_proof_bytes,256)))),
 CONSTRAINT fk_move_from FOREIGN KEY (binding_uuid,pod_uuid,attempt_uuid,from_footprint_uuid) REFERENCES physical_recovery_footprint (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT fk_move_to FOREIGN KEY (binding_uuid,pod_uuid,attempt_uuid,to_footprint_uuid) REFERENCES physical_recovery_footprint (binding_uuid,pod_uuid,attempt_uuid,footprint_uuid),
 CONSTRAINT fk_move_prepare FOREIGN KEY (binding_uuid,prepare_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_move_commit FOREIGN KEY (binding_uuid,commit_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_move_cleanup FOREIGN KEY (binding_uuid,cleanup_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_move_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 9: physical_source_unit; immediate default NO ACTION.
CREATE TABLE physical_source_unit (
 binding_uuid VARBINARY(16) NOT NULL,
 unit_uuid VARBINARY(16) NOT NULL,
 world_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 save_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 loader_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 catalog_generation VARBINARY(16) NOT NULL,
 read_generation BIGINT NOT NULL,
 source_observation_bytes LONGBLOB NOT NULL,
 source_observation_sha256 VARBINARY(32) NOT NULL,
 current_epoch BIGINT NULL,
 current_forest_uuid VARBINARY(16) NULL,
 unit_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 unit_revision BIGINT NOT NULL,
 created_operation_uuid VARBINARY(16) NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,unit_uuid),
 CONSTRAINT ck_f3_16_01 CHECK (unit_state IN ('DRAFT','SEALED')),
 CONSTRAINT ck_f3_16_02 CHECK (((current_epoch IS NULL AND current_forest_uuid IS NULL) OR (current_epoch IS NOT NULL AND current_forest_uuid IS NOT NULL))),
 CONSTRAINT ck_f3_16_03 CHECK ((unit_state='DRAFT' AND current_epoch IS NULL) OR (unit_state='SEALED' AND current_epoch IS NOT NULL AND current_epoch>=0)),
 CONSTRAINT ck_f3_16_04 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_16_05 CHECK (OCTET_LENGTH(unit_uuid)=16 AND unit_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_16_06 CHECK (OCTET_LENGTH(world_identity)>0),
 CONSTRAINT ck_f3_16_07 CHECK (OCTET_LENGTH(save_identity)>0),
 CONSTRAINT ck_f3_16_08 CHECK (OCTET_LENGTH(loader_identity)>0),
 CONSTRAINT ck_f3_16_09 CHECK (OCTET_LENGTH(catalog_generation)=16 AND catalog_generation<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_16_10 CHECK (read_generation>=0),
 CONSTRAINT ck_f3_16_11 CHECK (OCTET_LENGTH(source_observation_bytes)>0),
 CONSTRAINT ck_f3_16_12 CHECK (OCTET_LENGTH(source_observation_sha256)=32),
 CONSTRAINT ck_f3_16_13 CHECK (current_epoch IS NULL OR current_epoch>=0),
 CONSTRAINT ck_f3_16_14 CHECK (current_forest_uuid IS NULL OR (OCTET_LENGTH(current_forest_uuid)=16 AND current_forest_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_16_15 CHECK (unit_revision>=0),
 CONSTRAINT ck_f3_16_16 CHECK (OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_16_17 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_16_18 CHECK (source_observation_sha256=UNHEX(SHA2(source_observation_bytes,256))),
 CONSTRAINT fk_unit_binding FOREIGN KEY (binding_uuid) REFERENCES physical_recovery_binding (binding_uuid),
 CONSTRAINT fk_unit_created FOREIGN KEY (binding_uuid,created_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_unit_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 9: physical_source_forest; immediate default NO ACTION.
CREATE TABLE physical_source_forest (
 binding_uuid VARBINARY(16) NOT NULL,
 unit_uuid VARBINARY(16) NOT NULL,
 forest_uuid VARBINARY(16) NOT NULL,
 source_epoch BIGINT NOT NULL,
 forest_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 forest_codec VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 forest_bytes LONGBLOB NOT NULL,
 forest_sha256 VARBINARY(32) NOT NULL,
 member_count BIGINT NOT NULL,
 root_count BIGINT NOT NULL,
 carrier_count BIGINT NOT NULL,
 retained_bytes BIGINT NOT NULL,
 created_operation_uuid VARBINARY(16) NOT NULL,
 fp_pod_uuid_index_bytes LONGBLOB NULL,
 fp_pod_uuid_index_sha256 VARBINARY(32) NULL,
 PRIMARY KEY (binding_uuid,unit_uuid,forest_uuid),
 UNIQUE KEY ux_forest_epoch_key (binding_uuid,unit_uuid,forest_uuid,source_epoch),
 UNIQUE KEY ux_forest_epoch (binding_uuid,unit_uuid,source_epoch),
 CONSTRAINT ck_f3_17_01 CHECK (source_epoch>=0),
 CONSTRAINT ck_f3_17_02 CHECK (forest_state IN ('DRAFT','SEALED')),
 CONSTRAINT ck_f3_17_03 CHECK (forest_codec='FP_SOURCE_FOREST_BE_V2'),
 CONSTRAINT ck_f3_17_04 CHECK (member_count>0),
 CONSTRAINT ck_f3_17_05 CHECK (root_count>0),
 CONSTRAINT ck_f3_17_06 CHECK (root_count<=member_count),
 CONSTRAINT ck_f3_17_07 CHECK (carrier_count>0),
 CONSTRAINT ck_f3_17_08 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_17_09 CHECK (OCTET_LENGTH(unit_uuid)=16 AND unit_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_17_10 CHECK (OCTET_LENGTH(forest_uuid)=16 AND forest_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_17_11 CHECK (source_epoch>=0),
 CONSTRAINT ck_f3_17_12 CHECK (OCTET_LENGTH(forest_bytes)>0),
 CONSTRAINT ck_f3_17_13 CHECK (OCTET_LENGTH(forest_sha256)=32),
 CONSTRAINT ck_f3_17_14 CHECK (member_count>=0),
 CONSTRAINT ck_f3_17_15 CHECK (root_count>=0),
 CONSTRAINT ck_f3_17_16 CHECK (carrier_count>=0),
 CONSTRAINT ck_f3_17_17 CHECK (retained_bytes>=0),
 CONSTRAINT ck_f3_17_18 CHECK (OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_17_19 CHECK (forest_sha256=UNHEX(SHA2(forest_bytes,256))),
 CONSTRAINT ck_f3_17_20 CHECK ((fp_pod_uuid_index_bytes IS NULL AND fp_pod_uuid_index_sha256 IS NULL) OR (fp_pod_uuid_index_bytes IS NOT NULL AND fp_pod_uuid_index_sha256 IS NOT NULL)),
 CONSTRAINT ck_f3_17_21 CHECK (fp_pod_uuid_index_sha256 IS NULL OR (OCTET_LENGTH(fp_pod_uuid_index_sha256)=32 AND fp_pod_uuid_index_sha256=UNHEX(SHA2(fp_pod_uuid_index_bytes,256)))),
 CONSTRAINT ck_f3_17_22 CHECK (forest_state<>'SEALED' OR (fp_pod_uuid_index_bytes IS NOT NULL AND fp_pod_uuid_index_sha256 IS NOT NULL)),
 CONSTRAINT fk_forest_unit FOREIGN KEY (binding_uuid,unit_uuid) REFERENCES physical_source_unit (binding_uuid,unit_uuid),
 CONSTRAINT fk_forest_created FOREIGN KEY (binding_uuid,created_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 9: physical_source_transfer; immediate default NO ACTION.
CREATE TABLE physical_source_transfer (
 binding_uuid VARBINARY(16) NOT NULL,
 transfer_uuid VARBINARY(16) NOT NULL,
 unit_uuid VARBINARY(16) NOT NULL,
 old_epoch BIGINT NOT NULL,
 old_forest_uuid VARBINARY(16) NOT NULL,
 target_epoch BIGINT NOT NULL,
 target_forest_uuid VARBINARY(16) NOT NULL,
 transfer_plan_bytes LONGBLOB NOT NULL,
 transfer_plan_sha256 VARBINARY(32) NOT NULL,
 prepare_operation_uuid VARBINARY(16) NOT NULL,
 cutover_operation_uuid VARBINARY(16) NULL,
 retire_operation_uuid VARBINARY(16) NULL,
 phase VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 target_receipts_bytes LONGBLOB NULL,
 target_receipts_sha256 VARBINARY(32) NULL,
 retirement_proof_bytes LONGBLOB NULL,
 retirement_proof_sha256 VARBINARY(32) NULL,
 transfer_revision BIGINT NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,transfer_uuid),
 CONSTRAINT ck_f3_18_01 CHECK (old_epoch>=0),
 CONSTRAINT ck_f3_18_02 CHECK (target_epoch=old_epoch+1),
 CONSTRAINT ck_f3_18_03 CHECK (old_forest_uuid<>target_forest_uuid),
 CONSTRAINT ck_f3_18_04 CHECK (phase IN ('SOURCE_TRANSFER_PREPARED','TARGETS_DURABLE','CUTOVER_COMMITTED_RETIRE_PENDING','COMPLETED','REMEDIATION_REQUIRED')),
 CONSTRAINT ck_f3_18_05 CHECK (((target_receipts_bytes IS NULL AND target_receipts_sha256 IS NULL) OR (target_receipts_bytes IS NOT NULL AND target_receipts_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_18_06 CHECK (((retirement_proof_bytes IS NULL AND retirement_proof_sha256 IS NULL) OR (retirement_proof_bytes IS NOT NULL AND retirement_proof_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_18_07 CHECK (phase NOT IN ('TARGETS_DURABLE','CUTOVER_COMMITTED_RETIRE_PENDING','COMPLETED') OR target_receipts_bytes IS NOT NULL),
 CONSTRAINT ck_f3_18_08 CHECK (phase NOT IN ('CUTOVER_COMMITTED_RETIRE_PENDING','COMPLETED') OR cutover_operation_uuid IS NOT NULL),
 CONSTRAINT ck_f3_18_09 CHECK (phase<>'COMPLETED' OR (retire_operation_uuid IS NOT NULL AND retirement_proof_bytes IS NOT NULL)),
 CONSTRAINT ck_f3_18_10 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_18_11 CHECK (OCTET_LENGTH(transfer_uuid)=16 AND transfer_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_18_12 CHECK (OCTET_LENGTH(unit_uuid)=16 AND unit_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_18_13 CHECK (old_epoch>=0),
 CONSTRAINT ck_f3_18_14 CHECK (OCTET_LENGTH(old_forest_uuid)=16 AND old_forest_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_18_15 CHECK (target_epoch>=0),
 CONSTRAINT ck_f3_18_16 CHECK (OCTET_LENGTH(target_forest_uuid)=16 AND target_forest_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_18_17 CHECK (OCTET_LENGTH(transfer_plan_bytes)>0),
 CONSTRAINT ck_f3_18_18 CHECK (OCTET_LENGTH(transfer_plan_sha256)=32),
 CONSTRAINT ck_f3_18_19 CHECK (OCTET_LENGTH(prepare_operation_uuid)=16 AND prepare_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_18_20 CHECK (cutover_operation_uuid IS NULL OR (OCTET_LENGTH(cutover_operation_uuid)=16 AND cutover_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_18_21 CHECK (retire_operation_uuid IS NULL OR (OCTET_LENGTH(retire_operation_uuid)=16 AND retire_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_18_22 CHECK (target_receipts_sha256 IS NULL OR OCTET_LENGTH(target_receipts_sha256)=32),
 CONSTRAINT ck_f3_18_23 CHECK (retirement_proof_sha256 IS NULL OR OCTET_LENGTH(retirement_proof_sha256)=32),
 CONSTRAINT ck_f3_18_24 CHECK (transfer_revision>=0),
 CONSTRAINT ck_f3_18_25 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_18_26 CHECK (transfer_plan_sha256=UNHEX(SHA2(transfer_plan_bytes,256))),
 CONSTRAINT ck_f3_18_27 CHECK (target_receipts_bytes IS NULL OR (target_receipts_sha256 IS NOT NULL AND target_receipts_sha256=UNHEX(SHA2(target_receipts_bytes,256)))),
 CONSTRAINT ck_f3_18_28 CHECK (retirement_proof_bytes IS NULL OR (retirement_proof_sha256 IS NOT NULL AND retirement_proof_sha256=UNHEX(SHA2(retirement_proof_bytes,256)))),
 CONSTRAINT fk_source_transfer_old FOREIGN KEY (binding_uuid,unit_uuid,old_forest_uuid,old_epoch) REFERENCES physical_source_forest (binding_uuid,unit_uuid,forest_uuid,source_epoch),
 CONSTRAINT fk_source_transfer_target FOREIGN KEY (binding_uuid,unit_uuid,target_forest_uuid,target_epoch) REFERENCES physical_source_forest (binding_uuid,unit_uuid,forest_uuid,source_epoch),
 CONSTRAINT fk_source_transfer_prepare FOREIGN KEY (binding_uuid,prepare_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_source_transfer_cutover FOREIGN KEY (binding_uuid,cutover_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_source_transfer_retire FOREIGN KEY (binding_uuid,retire_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_source_transfer_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 10: physical_source_write; immediate default NO ACTION.
CREATE TABLE physical_source_write (
 binding_uuid VARBINARY(16) NOT NULL,
 write_uuid VARBINARY(16) NOT NULL,
 workflow_uuid VARBINARY(16) NOT NULL,
 resource_kind VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 carrier_key_hash VARBINARY(32) NOT NULL,
 carrier_key LONGBLOB NOT NULL,
 save_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 loader_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 region_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 slot_x INT NOT NULL,
 slot_z INT NOT NULL,
 write_generation BIGINT NOT NULL,
 payload_descriptor_bytes LONGBLOB NOT NULL,
 payload_descriptor_sha256 VARBINARY(32) NOT NULL,
 payload_length BIGINT NOT NULL,
 payload_sha256 VARBINARY(32) NOT NULL,
 payload_artifact_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 undo_kind VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 undo_descriptor_bytes LONGBLOB NOT NULL,
 undo_descriptor_sha256 VARBINARY(32) NOT NULL,
 undo_length BIGINT NOT NULL,
 undo_content_sha256 VARBINARY(32) NULL,
 undo_artifact_identity LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 member_coverage_bytes LONGBLOB NOT NULL,
 member_coverage_sha256 VARBINARY(32) NOT NULL,
 obligation_revisions_bytes LONGBLOB NOT NULL,
 obligation_revisions_sha256 VARBINARY(32) NOT NULL,
 limit_profile_sha256 VARBINARY(32) NOT NULL,
 receipt_codec VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 prepared_operation_uuid VARBINARY(16) NOT NULL,
 write_state VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 resolved_operation_uuid VARBINARY(16) NULL,
 durable_receipt_bytes LONGBLOB NULL,
 durable_receipt_sha256 VARBINARY(32) NULL,
 failure_diagnostic_identity LONGBLOB NULL,
 failure_diagnostic_sha256 VARBINARY(32) NULL,
 write_revision BIGINT NOT NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,write_uuid),
 UNIQUE KEY ux_write_generation (binding_uuid,carrier_key_hash,write_generation),
 CONSTRAINT ck_f3_19_01 CHECK (resource_kind='CARRIER'),
 CONSTRAINT ck_f3_19_02 CHECK (carrier_key_hash=UNHEX(SHA2(carrier_key,256))),
 CONSTRAINT ck_f3_19_03 CHECK (write_generation>=0),
 CONSTRAINT ck_f3_19_04 CHECK (payload_length>0),
 CONSTRAINT ck_f3_19_05 CHECK (undo_kind IN ('FULL_CHUNK','SLOT_ABSENT')),
 CONSTRAINT ck_f3_19_06 CHECK ((undo_kind='FULL_CHUNK' AND undo_length>0 AND undo_content_sha256 IS NOT NULL) OR (undo_kind='SLOT_ABSENT' AND undo_length=0 AND undo_content_sha256 IS NULL)),
 CONSTRAINT ck_f3_19_07 CHECK (receipt_codec='FP_SOURCE_WRITE_RECEIPT_BE_V1'),
 CONSTRAINT ck_f3_19_08 CHECK (write_state IN ('PREPARED','DURABLE','NO_PUT','SUPERSEDED','FAILED_NEEDS_RECONCILIATION')),
 CONSTRAINT ck_f3_19_09 CHECK (((durable_receipt_bytes IS NULL AND durable_receipt_sha256 IS NULL) OR (durable_receipt_bytes IS NOT NULL AND durable_receipt_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_19_10 CHECK (((failure_diagnostic_identity IS NULL AND failure_diagnostic_sha256 IS NULL) OR (failure_diagnostic_identity IS NOT NULL AND failure_diagnostic_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_19_11 CHECK ((write_state='PREPARED' AND resolved_operation_uuid IS NULL AND durable_receipt_bytes IS NULL AND failure_diagnostic_identity IS NULL) OR (write_state='DURABLE' AND resolved_operation_uuid IS NOT NULL AND durable_receipt_bytes IS NOT NULL) OR (write_state IN ('NO_PUT','SUPERSEDED','FAILED_NEEDS_RECONCILIATION') AND resolved_operation_uuid IS NOT NULL AND durable_receipt_bytes IS NULL)),
 CONSTRAINT ck_f3_19_12 CHECK (write_state<>'FAILED_NEEDS_RECONCILIATION' OR failure_diagnostic_identity IS NOT NULL),
 CONSTRAINT ck_f3_19_13 CHECK (failure_diagnostic_identity IS NULL OR failure_diagnostic_sha256=UNHEX(SHA2(failure_diagnostic_identity,256))),
 CONSTRAINT ck_f3_19_14 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_19_15 CHECK (OCTET_LENGTH(write_uuid)=16 AND write_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_19_16 CHECK (OCTET_LENGTH(workflow_uuid)=16 AND workflow_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_19_17 CHECK (OCTET_LENGTH(carrier_key_hash)=32),
 CONSTRAINT ck_f3_19_18 CHECK (OCTET_LENGTH(carrier_key)>0),
 CONSTRAINT ck_f3_19_19 CHECK (OCTET_LENGTH(save_identity)>0),
 CONSTRAINT ck_f3_19_20 CHECK (OCTET_LENGTH(loader_identity)>0),
 CONSTRAINT ck_f3_19_21 CHECK (OCTET_LENGTH(region_identity)>0),
 CONSTRAINT ck_f3_19_22 CHECK (write_generation>=0),
 CONSTRAINT ck_f3_19_23 CHECK (OCTET_LENGTH(payload_descriptor_bytes)>0),
 CONSTRAINT ck_f3_19_24 CHECK (OCTET_LENGTH(payload_descriptor_sha256)=32),
 CONSTRAINT ck_f3_19_25 CHECK (payload_length>=0),
 CONSTRAINT ck_f3_19_26 CHECK (OCTET_LENGTH(payload_sha256)=32),
 CONSTRAINT ck_f3_19_27 CHECK (OCTET_LENGTH(payload_artifact_identity)>0),
 CONSTRAINT ck_f3_19_28 CHECK (OCTET_LENGTH(undo_descriptor_bytes)>0),
 CONSTRAINT ck_f3_19_29 CHECK (OCTET_LENGTH(undo_descriptor_sha256)=32),
 CONSTRAINT ck_f3_19_30 CHECK (undo_length>=0),
 CONSTRAINT ck_f3_19_31 CHECK (undo_content_sha256 IS NULL OR OCTET_LENGTH(undo_content_sha256)=32),
 CONSTRAINT ck_f3_19_32 CHECK (OCTET_LENGTH(undo_artifact_identity)>0),
 CONSTRAINT ck_f3_19_33 CHECK (OCTET_LENGTH(member_coverage_bytes)>0),
 CONSTRAINT ck_f3_19_34 CHECK (OCTET_LENGTH(member_coverage_sha256)=32),
 CONSTRAINT ck_f3_19_35 CHECK (OCTET_LENGTH(obligation_revisions_bytes)>0),
 CONSTRAINT ck_f3_19_36 CHECK (OCTET_LENGTH(obligation_revisions_sha256)=32),
 CONSTRAINT ck_f3_19_37 CHECK (OCTET_LENGTH(limit_profile_sha256)=32),
 CONSTRAINT ck_f3_19_38 CHECK (OCTET_LENGTH(prepared_operation_uuid)=16 AND prepared_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_19_39 CHECK (resolved_operation_uuid IS NULL OR (OCTET_LENGTH(resolved_operation_uuid)=16 AND resolved_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_19_40 CHECK (durable_receipt_sha256 IS NULL OR OCTET_LENGTH(durable_receipt_sha256)=32),
 CONSTRAINT ck_f3_19_41 CHECK (failure_diagnostic_sha256 IS NULL OR OCTET_LENGTH(failure_diagnostic_sha256)=32),
 CONSTRAINT ck_f3_19_42 CHECK (write_revision>=0),
 CONSTRAINT ck_f3_19_43 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_19_44 CHECK (payload_descriptor_sha256=UNHEX(SHA2(payload_descriptor_bytes,256))),
 CONSTRAINT ck_f3_19_45 CHECK (undo_descriptor_sha256=UNHEX(SHA2(undo_descriptor_bytes,256))),
 CONSTRAINT ck_f3_19_46 CHECK (member_coverage_sha256=UNHEX(SHA2(member_coverage_bytes,256))),
 CONSTRAINT ck_f3_19_47 CHECK (obligation_revisions_sha256=UNHEX(SHA2(obligation_revisions_bytes,256))),
 CONSTRAINT ck_f3_19_48 CHECK (durable_receipt_bytes IS NULL OR (durable_receipt_sha256 IS NOT NULL AND durable_receipt_sha256=UNHEX(SHA2(durable_receipt_bytes,256)))),
 CONSTRAINT fk_write_carrier FOREIGN KEY (binding_uuid,resource_kind,carrier_key_hash) REFERENCES fp_recovery_reservation (binding_uuid,resource_kind,resource_key_hash),
 CONSTRAINT fk_write_prepared FOREIGN KEY (binding_uuid,prepared_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_write_resolved FOREIGN KEY (binding_uuid,resolved_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_write_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- METHOD sections 9,10: physical_source_obligation; immediate default NO ACTION.
CREATE TABLE physical_source_obligation (
 binding_uuid VARBINARY(16) NOT NULL,
 unit_uuid VARBINARY(16) NOT NULL,
 forest_uuid VARBINARY(16) NOT NULL,
 resource_kind VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 carrier_key_hash VARBINARY(32) NOT NULL,
 carrier_key LONGBLOB NOT NULL,
 purpose VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 obligation_state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 obligation_revision BIGINT NOT NULL,
 created_operation_uuid VARBINARY(16) NOT NULL,
 write_uuid VARBINARY(16) NULL,
 discharged_operation_uuid VARBINARY(16) NULL,
 discharge_kind VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
 discharge_proof_bytes LONGBLOB NULL,
 discharge_proof_sha256 VARBINARY(32) NULL,
 last_mutation_operation_uuid VARBINARY(16) NOT NULL,
 PRIMARY KEY (binding_uuid,unit_uuid,forest_uuid,carrier_key_hash,purpose),
 CONSTRAINT ck_f3_20_01 CHECK (resource_kind='CARRIER'),
 CONSTRAINT ck_f3_20_02 CHECK (carrier_key_hash=UNHEX(SHA2(carrier_key,256))),
 CONSTRAINT ck_f3_20_03 CHECK (purpose IN ('OLD_HOLD','TARGET_STAGED','CURRENT_HOLD','OLD_RETIRE','UNDO')),
 CONSTRAINT ck_f3_20_04 CHECK (obligation_state IN ('OPEN','DISCHARGED')),
 CONSTRAINT ck_f3_20_05 CHECK (((discharge_proof_bytes IS NULL AND discharge_proof_sha256 IS NULL) OR (discharge_proof_bytes IS NOT NULL AND discharge_proof_sha256 IS NOT NULL))),
 CONSTRAINT ck_f3_20_06 CHECK (discharge_kind IS NULL OR discharge_kind IN ('CUTOVER','WORLD_RETIREMENT','SAFE_UNDO','SUCCESSOR_CUTOVER')),
 CONSTRAINT ck_f3_20_07 CHECK ((obligation_state='OPEN' AND discharged_operation_uuid IS NULL AND discharge_kind IS NULL AND discharge_proof_bytes IS NULL) OR (obligation_state='DISCHARGED' AND discharged_operation_uuid IS NOT NULL AND discharge_kind IS NOT NULL AND discharge_proof_bytes IS NOT NULL)),
 CONSTRAINT ck_f3_20_08 CHECK (purpose NOT IN ('TARGET_STAGED','UNDO','OLD_RETIRE') OR write_uuid IS NOT NULL),
 CONSTRAINT ck_f3_20_09 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_20_10 CHECK (OCTET_LENGTH(unit_uuid)=16 AND unit_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_20_11 CHECK (OCTET_LENGTH(forest_uuid)=16 AND forest_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_20_12 CHECK (OCTET_LENGTH(carrier_key_hash)=32),
 CONSTRAINT ck_f3_20_13 CHECK (OCTET_LENGTH(carrier_key)>0),
 CONSTRAINT ck_f3_20_14 CHECK (obligation_revision>=0),
 CONSTRAINT ck_f3_20_15 CHECK (OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_20_16 CHECK (write_uuid IS NULL OR (OCTET_LENGTH(write_uuid)=16 AND write_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_20_17 CHECK (discharged_operation_uuid IS NULL OR (OCTET_LENGTH(discharged_operation_uuid)=16 AND discharged_operation_uuid<>UNHEX(REPEAT('00',16)))),
 CONSTRAINT ck_f3_20_18 CHECK (discharge_proof_sha256 IS NULL OR OCTET_LENGTH(discharge_proof_sha256)=32),
 CONSTRAINT ck_f3_20_19 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
 CONSTRAINT ck_f3_20_20 CHECK (discharge_proof_bytes IS NULL OR (discharge_proof_sha256 IS NOT NULL AND discharge_proof_sha256=UNHEX(SHA2(discharge_proof_bytes,256)))),
 CONSTRAINT fk_source_obligation_forest FOREIGN KEY (binding_uuid,unit_uuid,forest_uuid) REFERENCES physical_source_forest (binding_uuid,unit_uuid,forest_uuid),
 CONSTRAINT fk_source_obligation_carrier FOREIGN KEY (binding_uuid,resource_kind,carrier_key_hash) REFERENCES fp_recovery_reservation (binding_uuid,resource_kind,resource_key_hash),
 CONSTRAINT fk_source_obligation_write FOREIGN KEY (binding_uuid,write_uuid) REFERENCES physical_source_write (binding_uuid,write_uuid),
 CONSTRAINT fk_source_obligation_created FOREIGN KEY (binding_uuid,created_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_source_obligation_discharged FOREIGN KEY (binding_uuid,discharged_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
 CONSTRAINT fk_source_obligation_mutation FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB;

-- Fresh-only cycles: only these just-created empty BOOTSTRAPPING tables; never old/READY schema.
ALTER TABLE physical_source_unit ADD CONSTRAINT fk_unit_current_forest
 FOREIGN KEY (binding_uuid,unit_uuid,current_forest_uuid,current_epoch)
 REFERENCES physical_source_forest(binding_uuid,unit_uuid,forest_uuid,source_epoch);
ALTER TABLE physical_recovery_intent ADD CONSTRAINT fk_intent_completion
 FOREIGN KEY (binding_uuid,source_completion_uuid) REFERENCES fp_lifecycle_completion(binding_uuid,receipt_uuid);
ALTER TABLE physical_recovery_intent ADD CONSTRAINT fk_intent_source_forest
 FOREIGN KEY (binding_uuid,source_unit_uuid,source_forest_uuid) REFERENCES physical_source_forest(binding_uuid,unit_uuid,forest_uuid);

CREATE TABLE fp_script_catalog (
  binding_uuid VARBINARY(16) NOT NULL,
  catalog_revision BIGINT NOT NULL,
  snapshot_bytes LONGBLOB NOT NULL,
  snapshot_sha256 VARBINARY(32) NOT NULL,
  next_version BIGINT NULL,
  last_mutation_operation_uuid VARBINARY(16) NULL,
  PRIMARY KEY (binding_uuid),
  CONSTRAINT ck_s4_21_01 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_21_02 CHECK (OCTET_LENGTH(snapshot_sha256)=32),
  CONSTRAINT ck_s4_21_03 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_21_04 CHECK (catalog_revision>=0),
  CONSTRAINT ck_s4_21_05 CHECK (next_version IS NULL OR next_version>0),
  CONSTRAINT ck_s4_21_06 CHECK (snapshot_sha256=UNHEX(SHA2(snapshot_bytes,256))),
  CONSTRAINT ck_s4_21_07 CHECK ((catalog_revision=0 AND last_mutation_operation_uuid IS NULL) OR last_mutation_operation_uuid IS NOT NULL),
  CONSTRAINT fk_sc_binding FOREIGN KEY (binding_uuid) REFERENCES physical_recovery_binding (binding_uuid),
  CONSTRAINT fk_sc_op FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE fp_script_operation (
  binding_uuid VARBINARY(16) NOT NULL,
  operation_uuid VARBINARY(16) NOT NULL,
  request_kind VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  behavior_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  version BIGINT NOT NULL,
  source_sha256 VARBINARY(32) NOT NULL,
  compilation_sha256 VARBINARY(32) NULL,
  request_bytes LONGBLOB NOT NULL,
  request_sha256 VARBINARY(32) NOT NULL,
  expected_catalog_revision BIGINT NOT NULL,
  after_catalog_revision BIGINT NOT NULL,
  before_bytes LONGBLOB NOT NULL,
  before_sha256 VARBINARY(32) NOT NULL,
  after_bytes LONGBLOB NOT NULL,
  after_sha256 VARBINARY(32) NOT NULL,
  result_code VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  receipt_bytes LONGBLOB NOT NULL,
  receipt_sha256 VARBINARY(32) NOT NULL,
  PRIMARY KEY (binding_uuid,operation_uuid),
  KEY ix_so_version (binding_uuid,behavior_id,version),
  CONSTRAINT ck_s4_24_01 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_24_02 CHECK (OCTET_LENGTH(operation_uuid)=16 AND operation_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_24_03 CHECK (OCTET_LENGTH(source_sha256)=32),
  CONSTRAINT ck_s4_24_04 CHECK (OCTET_LENGTH(compilation_sha256)=32),
  CONSTRAINT ck_s4_24_05 CHECK (OCTET_LENGTH(request_sha256)=32),
  CONSTRAINT ck_s4_24_06 CHECK (OCTET_LENGTH(before_sha256)=32),
  CONSTRAINT ck_s4_24_07 CHECK (OCTET_LENGTH(after_sha256)=32),
  CONSTRAINT ck_s4_24_08 CHECK (OCTET_LENGTH(receipt_sha256)=32),
  CONSTRAINT ck_s4_24_09 CHECK ((REGEXP_LIKE(behavior_id,'^[a-z][a-z0-9_.-]{0,63}$','c') AND NOT REGEXP_LIKE(behavior_id,'[^a-z0-9_.-]','c'))),
  CONSTRAINT ck_s4_24_10 CHECK (version>0),
  CONSTRAINT ck_s4_24_11 CHECK (request_kind IN ('SUBMIT','CHECK','APPROVE','REVOKE')),
  CONSTRAINT ck_s4_24_12 CHECK (result_code IN ('APPLIED','REJECTED')),
  CONSTRAINT ck_s4_24_13 CHECK (expected_catalog_revision>=0),
  CONSTRAINT ck_s4_24_14 CHECK (after_catalog_revision>=0),
  CONSTRAINT ck_s4_24_15 CHECK (OCTET_LENGTH(request_bytes)>0),
  CONSTRAINT ck_s4_24_16 CHECK (OCTET_LENGTH(receipt_bytes)>0),
  CONSTRAINT ck_s4_24_17 CHECK (request_sha256=UNHEX(SHA2(request_bytes,256))),
  CONSTRAINT ck_s4_24_18 CHECK (before_sha256=UNHEX(SHA2(before_bytes,256))),
  CONSTRAINT ck_s4_24_19 CHECK (after_sha256=UNHEX(SHA2(after_bytes,256))),
  CONSTRAINT ck_s4_24_20 CHECK (receipt_sha256=UNHEX(SHA2(receipt_bytes,256))),
  CONSTRAINT ck_s4_24_21 CHECK ((result_code='APPLIED' AND request_kind IN ('APPROVE','REVOKE') AND expected_catalog_revision<9223372036854775807 AND after_catalog_revision=expected_catalog_revision+1) OR ((result_code='REJECTED' OR request_kind IN ('SUBMIT','CHECK')) AND after_catalog_revision=expected_catalog_revision AND after_bytes=before_bytes)),
  CONSTRAINT ck_s4_24_22 CHECK (request_kind<>'SUBMIT' OR compilation_sha256 IS NULL),
  CONSTRAINT ck_s4_24_23 CHECK (request_kind<>'APPROVE' OR compilation_sha256 IS NOT NULL),
  CONSTRAINT fk_so_binding FOREIGN KEY (binding_uuid) REFERENCES fp_script_catalog (binding_uuid),
  CONSTRAINT fk_so_original FOREIGN KEY (binding_uuid,operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE fp_script_candidate (
  binding_uuid VARBINARY(16) NOT NULL,
  behavior_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  version BIGINT NOT NULL,
  source_bytes MEDIUMBLOB NOT NULL,
  source_sha256 VARBINARY(32) NOT NULL,
  submitted_by VARBINARY(16) NOT NULL,
  submission_operation_uuid VARBINARY(16) NOT NULL,
  check_revision BIGINT NOT NULL,
  check_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  check_evidence_bytes LONGBLOB NULL,
  check_evidence_sha256 VARBINARY(32) NULL,
  diagnostics_bytes LONGBLOB NULL,
  diagnostics_sha256 VARBINARY(32) NULL,
  loader_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
  loader_artifact_sha256 VARBINARY(32) NULL,
  compiler_version VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
  schema_version VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
  schema_sha256 VARBINARY(32) NULL,
  compilation_sha256 VARBINARY(32) NULL,
  last_mutation_operation_uuid VARBINARY(16) NOT NULL,
  PRIMARY KEY (binding_uuid,behavior_id,version),
  KEY ix_sd_status (binding_uuid,check_status),
  CONSTRAINT ck_s4_22_01 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_22_02 CHECK (OCTET_LENGTH(source_sha256)=32),
  CONSTRAINT ck_s4_22_03 CHECK (OCTET_LENGTH(submitted_by)=16 AND submitted_by<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_22_04 CHECK (OCTET_LENGTH(submission_operation_uuid)=16 AND submission_operation_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_22_05 CHECK (OCTET_LENGTH(check_evidence_sha256)=32),
  CONSTRAINT ck_s4_22_06 CHECK (OCTET_LENGTH(diagnostics_sha256)=32),
  CONSTRAINT ck_s4_22_07 CHECK (OCTET_LENGTH(loader_artifact_sha256)=32),
  CONSTRAINT ck_s4_22_08 CHECK (OCTET_LENGTH(schema_sha256)=32),
  CONSTRAINT ck_s4_22_09 CHECK (OCTET_LENGTH(compilation_sha256)=32),
  CONSTRAINT ck_s4_22_10 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_22_11 CHECK ((REGEXP_LIKE(behavior_id,'^[a-z][a-z0-9_.-]{0,63}$','c') AND NOT REGEXP_LIKE(behavior_id,'[^a-z0-9_.-]','c'))),
  CONSTRAINT ck_s4_22_12 CHECK (version>0),
  CONSTRAINT ck_s4_22_13 CHECK (check_revision>=0),
  CONSTRAINT ck_s4_22_14 CHECK (OCTET_LENGTH(source_bytes)<=65536),
  CONSTRAINT ck_s4_22_15 CHECK (source_sha256=UNHEX(SHA2(source_bytes,256))),
  CONSTRAINT ck_s4_22_16 CHECK (check_evidence_sha256=UNHEX(SHA2(check_evidence_bytes,256))),
  CONSTRAINT ck_s4_22_17 CHECK (diagnostics_sha256=UNHEX(SHA2(diagnostics_bytes,256))),
  CONSTRAINT ck_s4_22_18 CHECK (loader_id IS NULL OR (REGEXP_LIKE(loader_id,'^[a-z][a-z0-9_.-]{0,63}$','c') AND NOT REGEXP_LIKE(loader_id,'[^a-z0-9_.-]','c'))),
  CONSTRAINT ck_s4_22_19 CHECK ((check_status='PENDING' AND check_revision=0 AND check_evidence_bytes IS NULL AND check_evidence_sha256 IS NULL AND diagnostics_bytes IS NULL AND diagnostics_sha256 IS NULL AND loader_id IS NULL AND loader_artifact_sha256 IS NULL AND compiler_version IS NULL AND schema_version IS NULL AND schema_sha256 IS NULL AND compilation_sha256 IS NULL) OR (check_status='INVALID' AND check_revision>0 AND check_evidence_bytes IS NOT NULL AND check_evidence_sha256 IS NOT NULL AND diagnostics_bytes IS NOT NULL AND diagnostics_sha256 IS NOT NULL AND loader_id IS NULL AND loader_artifact_sha256 IS NULL AND compiler_version IS NULL AND schema_version IS NULL AND schema_sha256 IS NULL AND compilation_sha256 IS NULL) OR (check_status='VALID' AND check_revision>0 AND check_evidence_bytes IS NOT NULL AND check_evidence_sha256 IS NOT NULL AND diagnostics_bytes IS NOT NULL AND diagnostics_sha256 IS NOT NULL AND loader_id IS NOT NULL AND loader_artifact_sha256 IS NOT NULL AND compiler_version IS NOT NULL AND schema_version IS NOT NULL AND schema_sha256 IS NOT NULL AND compilation_sha256 IS NOT NULL AND compiler_version='fp-skill-compiler/1' AND schema_version='fp-follow-layout/1' AND schema_sha256=UNHEX('fcdea1a4aef5d08c8ac275b9886986da6c9383cfa50ef57ff75d8cb0f12ffc22'))),
  CONSTRAINT fk_sd_binding FOREIGN KEY (binding_uuid) REFERENCES fp_script_catalog (binding_uuid),
  CONSTRAINT fk_sd_submit FOREIGN KEY (binding_uuid,submission_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
  CONSTRAINT fk_sd_op FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE fp_script_version (
  binding_uuid VARBINARY(16) NOT NULL,
  behavior_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  version BIGINT NOT NULL,
  source_sha256 VARBINARY(32) NOT NULL,
  compilation_bytes MEDIUMBLOB NOT NULL,
  compilation_sha256 VARBINARY(32) NOT NULL,
  compiler_version VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  schema_version VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  schema_sha256 VARBINARY(32) NOT NULL,
  layout_config_bytes LONGBLOB NOT NULL,
  layout_config_sha256 VARBINARY(32) NOT NULL,
  approved_by VARBINARY(16) NOT NULL,
  approval_operation_uuid VARBINARY(16) NOT NULL,
  approved_check_revision BIGINT NOT NULL,
  approval_catalog_revision BIGINT NOT NULL,
  status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  revoked_by VARBINARY(16) NULL,
  revocation_operation_uuid VARBINARY(16) NULL,
  revocation_catalog_revision BIGINT NULL,
  last_mutation_operation_uuid VARBINARY(16) NOT NULL,
  PRIMARY KEY (binding_uuid,behavior_id,version),
  KEY ix_sv_status (binding_uuid,status),
  CONSTRAINT ck_s4_23_01 CHECK (OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_23_02 CHECK (OCTET_LENGTH(source_sha256)=32),
  CONSTRAINT ck_s4_23_03 CHECK (OCTET_LENGTH(compilation_sha256)=32),
  CONSTRAINT ck_s4_23_04 CHECK (OCTET_LENGTH(schema_sha256)=32),
  CONSTRAINT ck_s4_23_05 CHECK (OCTET_LENGTH(layout_config_sha256)=32),
  CONSTRAINT ck_s4_23_06 CHECK (OCTET_LENGTH(approved_by)=16 AND approved_by<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_23_07 CHECK (OCTET_LENGTH(approval_operation_uuid)=16 AND approval_operation_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_23_08 CHECK (OCTET_LENGTH(revoked_by)=16 AND revoked_by<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_23_09 CHECK (OCTET_LENGTH(revocation_operation_uuid)=16 AND revocation_operation_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_23_10 CHECK (OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))),
  CONSTRAINT ck_s4_23_11 CHECK ((REGEXP_LIKE(behavior_id,'^[a-z][a-z0-9_.-]{0,63}$','c') AND NOT REGEXP_LIKE(behavior_id,'[^a-z0-9_.-]','c'))),
  CONSTRAINT ck_s4_23_12 CHECK (version>0),
  CONSTRAINT ck_s4_23_13 CHECK (approved_check_revision>0),
  CONSTRAINT ck_s4_23_14 CHECK (approval_catalog_revision>0),
  CONSTRAINT ck_s4_23_15 CHECK (OCTET_LENGTH(compilation_bytes) BETWEEN 1 AND 20357),
  CONSTRAINT ck_s4_23_16 CHECK (compilation_sha256=UNHEX(SHA2(compilation_bytes,256))),
  CONSTRAINT ck_s4_23_17 CHECK (layout_config_sha256=UNHEX(SHA2(layout_config_bytes,256))),
  CONSTRAINT ck_s4_23_18 CHECK (compiler_version='fp-skill-compiler/1' AND schema_version='fp-follow-layout/1' AND schema_sha256=UNHEX('fcdea1a4aef5d08c8ac275b9886986da6c9383cfa50ef57ff75d8cb0f12ffc22')),
  CONSTRAINT ck_s4_23_19 CHECK ((status='APPROVED' AND revoked_by IS NULL AND revocation_operation_uuid IS NULL AND revocation_catalog_revision IS NULL) OR (status='REVOKED' AND revoked_by IS NOT NULL AND revocation_operation_uuid IS NOT NULL AND revocation_catalog_revision IS NOT NULL AND revocation_catalog_revision>approval_catalog_revision)),
  CONSTRAINT fk_sv_candidate FOREIGN KEY (binding_uuid,behavior_id,version) REFERENCES fp_script_candidate (binding_uuid,behavior_id,version),
  CONSTRAINT fk_sv_approval FOREIGN KEY (binding_uuid,approval_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
  CONSTRAINT fk_sv_revoke FOREIGN KEY (binding_uuid,revocation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid),
  CONSTRAINT fk_sv_op FOREIGN KEY (binding_uuid,last_mutation_operation_uuid) REFERENCES fp_recovery_operation (binding_uuid,operation_uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- Two control seeds only; no fake B1/domain/operation/source history or READY seal.
INSERT INTO fp_schema_manifest (singleton_fixed1,backend,format_version,ddl_sha256,provisioning_uuid,bootstrap_state,
 expected_server_uuid,expected_binding_uuid,import_state,configuration_sha256,source_provenance_kind,
 source_provenance_bytes,source_provenance_sha256,limit_profile_encoding,limit_profile_bytes,limit_profile_sha256,
 max_region_bytes,max_chunk_bytes,max_decompressed_bytes,max_nbt_depth,max_members,max_retained_bytes,max_sql_packet_bytes,max_native_artifact_bytes)
 VALUES (1,'MYSQL',4,@fp_ddl_sha256,@fp_provisioning_uuid,'BOOTSTRAPPING',@fp_expected_server_uuid,@fp_expected_binding_uuid,
 'NONE',@fp_configuration_sha256,@fp_source_provenance_kind,@fp_source_provenance_bytes,@fp_source_provenance_sha256,
 'FP_SOURCE_LIMIT_PROFILE_BE_V1',@fp_limit_profile_bytes,@fp_limit_profile_sha256,@fp_max_region_bytes,@fp_max_chunk_bytes,
 @fp_max_decompressed_bytes,@fp_max_nbt_depth,@fp_max_members,@fp_max_retained_bytes,@fp_max_sql_packet_bytes,@fp_max_native_artifact_bytes);
INSERT INTO fp_runtime_writer (singleton_fixed1,binding_uuid,writer_epoch,owner_boot_uuid,lease_until,last_commit_operation_uuid,last_commit_fingerprint)
 VALUES(1,@fp_expected_binding_uuid,0,NULL,'1000-01-01 00:00:00.000000',NULL,NULL);
-- STOP BOOTSTRAPPING. Full import/seal/runtime admission are independent gates.

-- SOURCE v4 ONLY: 60 actual guards, shared inline FP index decoder. No FUNCTION/extra EXECUTE.
USE `fp_script_publication1`;
SET SESSION sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
DELIMITER $$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_01_bi BEFORE INSERT ON fp_schema_manifest FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='manifest already seeded; no second manifest'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_01_bu BEFORE UPDATE ON fp_schema_manifest FOR EACH ROW
BEGIN
 IF NOT (((OLD.singleton_fixed1 <=> NEW.singleton_fixed1) AND (CAST(OLD.backend AS BINARY) <=> CAST(NEW.backend AS BINARY)) AND (OLD.format_version <=> NEW.format_version) AND (OLD.ddl_sha256 <=> NEW.ddl_sha256) AND (OLD.provisioning_uuid <=> NEW.provisioning_uuid) AND (OLD.expected_server_uuid <=> NEW.expected_server_uuid) AND (OLD.expected_binding_uuid <=> NEW.expected_binding_uuid) AND (OLD.import_manifest_sha256 <=> NEW.import_manifest_sha256) AND (CAST(OLD.import_state AS BINARY) <=> CAST(NEW.import_state AS BINARY)) AND (OLD.import_operation_uuid <=> NEW.import_operation_uuid) AND (OLD.import_snapshot_sha256 <=> NEW.import_snapshot_sha256) AND (OLD.import_receipt_sha256 <=> NEW.import_receipt_sha256) AND (OLD.configuration_sha256 <=> NEW.configuration_sha256) AND (CAST(OLD.source_provenance_kind AS BINARY) <=> CAST(NEW.source_provenance_kind AS BINARY)) AND (OLD.source_provenance_bytes <=> NEW.source_provenance_bytes) AND (OLD.source_provenance_sha256 <=> NEW.source_provenance_sha256) AND (CAST(OLD.limit_profile_encoding AS BINARY) <=> CAST(NEW.limit_profile_encoding AS BINARY)) AND (OLD.limit_profile_bytes <=> NEW.limit_profile_bytes) AND (OLD.limit_profile_sha256 <=> NEW.limit_profile_sha256) AND (OLD.max_region_bytes <=> NEW.max_region_bytes) AND (OLD.max_chunk_bytes <=> NEW.max_chunk_bytes) AND (OLD.max_decompressed_bytes <=> NEW.max_decompressed_bytes) AND (OLD.max_nbt_depth <=> NEW.max_nbt_depth) AND (OLD.max_members <=> NEW.max_members) AND (OLD.max_retained_bytes <=> NEW.max_retained_bytes) AND (OLD.max_sql_packet_bytes <=> NEW.max_sql_packet_bytes) AND (OLD.max_native_artifact_bytes <=> NEW.max_native_artifact_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((OLD.bootstrap_state='BOOTSTRAPPING' AND NEW.bootstrap_state='READY' AND OLD.guard_manifest_sha256 IS NULL AND NEW.guard_manifest_sha256 IS NOT NULL AND NEW.import_state IN ('NONE','COMMITTED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='only controller-approved one-time seal transition'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_01_bd BEFORE DELETE ON fp_schema_manifest FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_02_bi BEFORE INSERT ON fp_runtime_writer FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='writer already seeded'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_02_bu BEFORE UPDATE ON fp_runtime_writer FOR EACH ROW
BEGIN
 IF NOT (((OLD.singleton_fixed1 <=> NEW.singleton_fixed1) AND (OLD.binding_uuid <=> NEW.binding_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT (((NEW.writer_epoch=OLD.writer_epoch+1 AND OLD.lease_until<=UTC_TIMESTAMP(6) AND NEW.owner_boot_uuid IS NOT NULL AND NEW.lease_until>UTC_TIMESTAMP(6) AND (((OLD.last_commit_operation_uuid <=> NEW.last_commit_operation_uuid) AND (OLD.last_commit_fingerprint <=> NEW.last_commit_fingerprint)) OR (EXISTS(SELECT 1 FROM binding_bootstrap_intent i WHERE i.binding_uuid=NEW.binding_uuid AND ((i.stage='PREPARED' AND i.prepare_write_uuid=NEW.last_commit_operation_uuid) OR (i.stage='COMPLETE' AND i.certify_write_uuid=NEW.last_commit_operation_uuid)))))) OR (NEW.writer_epoch=OLD.writer_epoch AND (NEW.owner_boot_uuid <=> OLD.owner_boot_uuid) AND NEW.owner_boot_uuid IS NOT NULL AND OLD.lease_until>UTC_TIMESTAMP(6) AND NEW.lease_until>=OLD.lease_until AND (((OLD.last_commit_operation_uuid <=> NEW.last_commit_operation_uuid) AND (OLD.last_commit_fingerprint <=> NEW.last_commit_fingerprint)) OR (EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_commit_operation_uuid AND o.operation_state='COMMITTED' AND o.operation_fingerprint=NEW.last_commit_fingerprint AND o.boot_uuid=NEW.owner_boot_uuid AND o.fence_epoch=NEW.writer_epoch AND o.connection_id=CONNECTION_ID())) OR (EXISTS(SELECT 1 FROM binding_bootstrap_intent i WHERE i.binding_uuid=NEW.binding_uuid AND ((i.stage='PREPARED' AND i.prepare_write_uuid=NEW.last_commit_operation_uuid) OR (i.stage='COMPLETE' AND i.certify_write_uuid=NEW.last_commit_operation_uuid)))))) OR (NEW.writer_epoch=OLD.writer_epoch AND OLD.owner_boot_uuid IS NOT NULL AND NEW.owner_boot_uuid IS NULL AND NEW.lease_until<=UTC_TIMESTAMP(6) AND ((OLD.last_commit_operation_uuid <=> NEW.last_commit_operation_uuid) AND (OLD.last_commit_fingerprint <=> NEW.last_commit_fingerprint)))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='invalid writer CAS/renew/commit/release topology'; END IF;
 IF NOT ((NEW.owner_boot_uuid IS NOT NULL OR (NOT EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=OLD.binding_uuid AND o.boot_uuid=OLD.owner_boot_uuid AND o.fence_epoch=OLD.writer_epoch AND o.operation_state='RESERVED') AND NOT EXISTS(SELECT 1 FROM binding_bootstrap_intent i WHERE i.binding_uuid=OLD.binding_uuid AND i.stage='PREPARED'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='release cannot erase unresolved current operation or B1 preparation'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_02_bd BEFORE DELETE ON fp_runtime_writer FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_03_bi BEFORE INSERT ON physical_recovery_binding FOR EACH ROW
BEGIN
 IF NOT ((NEW.cutover_state='LEGACY_UNCERTIFIED' AND NEW.format_version=2 AND EXISTS(SELECT 1 FROM binding_bootstrap_intent i WHERE i.binding_uuid=NEW.binding_uuid AND i.stage='PREPARED' AND i.binding_format_version=2)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='B1 prepared intent must precede Legacy binding'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_03_bu BEFORE UPDATE ON physical_recovery_binding FOR EACH ROW
BEGIN
 IF NOT (((OLD.singleton_fixed1 <=> NEW.singleton_fixed1) AND (OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.format_version <=> NEW.format_version)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((OLD.cutover_state='LEGACY_UNCERTIFIED' AND NEW.cutover_state='CERTIFIED' AND EXISTS(SELECT 1 FROM binding_bootstrap_intent i WHERE i.binding_uuid=NEW.binding_uuid AND i.stage='PREPARED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='binding certification must consume same prepared B1 identity'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_03_bd BEFORE DELETE ON physical_recovery_binding FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_04_bi BEFORE INSERT ON fp_recovery_operation FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_runtime_writer w WHERE w.binding_uuid=NEW.binding_uuid AND w.owner_boot_uuid=NEW.boot_uuid AND w.writer_epoch=NEW.fence_epoch AND NEW.connection_id=CONNECTION_ID() AND w.lease_until=NEW.fixed_lease_until AND NEW.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='operation original scope must match actual owned transaction'; END IF;
 IF NOT (((NEW.operation_state='COMMITTED' AND NEW.step='RESERVE' AND NEW.reserve_operation_uuid IS NULL) OR (NEW.operation_state='RESERVED' AND NEW.step<>'RESERVE' AND NEW.settled_by_operation_uuid IS NULL)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='only bounded reserve commit or planned RESERVED insert'; END IF;
 IF NOT ((NEW.reserve_operation_uuid IS NULL OR EXISTS(SELECT 1 FROM fp_recovery_operation r WHERE r.binding_uuid=NEW.binding_uuid AND r.operation_uuid=NEW.reserve_operation_uuid AND r.workflow_uuid=NEW.workflow_uuid AND r.step='RESERVE' AND r.operation_state='COMMITTED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='reserve operation scope mismatch'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_04_bu BEFORE UPDATE ON fp_recovery_operation FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.operation_uuid <=> NEW.operation_uuid) AND (OLD.workflow_uuid <=> NEW.workflow_uuid) AND (CAST(OLD.step AS BINARY) <=> CAST(NEW.step AS BINARY)) AND (OLD.pod_uuid <=> NEW.pod_uuid) AND (OLD.attempt_uuid <=> NEW.attempt_uuid) AND (OLD.footprint_uuid <=> NEW.footprint_uuid) AND (OLD.source_unit_uuid <=> NEW.source_unit_uuid) AND (OLD.forest_uuid <=> NEW.forest_uuid) AND (OLD.boot_uuid <=> NEW.boot_uuid) AND (OLD.boot_generation <=> NEW.boot_generation) AND (OLD.connection_generation_uuid <=> NEW.connection_generation_uuid) AND (OLD.connection_id <=> NEW.connection_id) AND (OLD.fence_epoch <=> NEW.fence_epoch) AND (OLD.request_identity_bytes <=> NEW.request_identity_bytes) AND (OLD.request_identity_sha256 <=> NEW.request_identity_sha256) AND (OLD.expected_domain_version <=> NEW.expected_domain_version) AND (OLD.expected_locator_revision <=> NEW.expected_locator_revision) AND (OLD.expected_source_revision <=> NEW.expected_source_revision) AND (CAST(OLD.context_encoding AS BINARY) <=> CAST(NEW.context_encoding AS BINARY)) AND (OLD.original_context_bytes <=> NEW.original_context_bytes) AND (OLD.original_context_sha256 <=> NEW.original_context_sha256) AND (OLD.scope_bytes <=> NEW.scope_bytes) AND (OLD.scope_sha256 <=> NEW.scope_sha256) AND (OLD.preimage_bytes <=> NEW.preimage_bytes) AND (OLD.preimage_sha256 <=> NEW.preimage_sha256) AND (OLD.target_bytes <=> NEW.target_bytes) AND (OLD.target_sha256 <=> NEW.target_sha256) AND (OLD.reserve_operation_uuid <=> NEW.reserve_operation_uuid) AND (OLD.previous_step_operation_uuid <=> NEW.previous_step_operation_uuid) AND (OLD.planned_at <=> NEW.planned_at) AND (OLD.fixed_lease_until <=> NEW.fixed_lease_until) AND (OLD.operation_fingerprint <=> NEW.operation_fingerprint)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((OLD.operation_state='RESERVED' AND NEW.operation_state IN ('COMMITTED','CANCELLED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='operation terminal history cannot be rewritten'; END IF;
 IF NOT ((OLD.original_diagnostic_identity IS NULL OR (OLD.original_diagnostic_identity <=> NEW.original_diagnostic_identity)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='original failure identity retained'; END IF;
 IF NOT ((NEW.operation_state<>'COMMITTED' OR EXISTS(SELECT 1 FROM fp_runtime_writer w WHERE w.binding_uuid=NEW.binding_uuid AND w.owner_boot_uuid=NEW.boot_uuid AND w.writer_epoch=NEW.fence_epoch AND NEW.connection_id=CONNECTION_ID() AND NEW.fixed_lease_until=w.lease_until AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='commit must retain original live scope'; END IF;
 IF NOT ((NEW.operation_state<>'CANCELLED' OR EXISTS(SELECT 1 FROM fp_recovery_operation s JOIN fp_runtime_writer w ON w.binding_uuid=s.binding_uuid WHERE s.binding_uuid=NEW.binding_uuid AND s.operation_uuid=NEW.settled_by_operation_uuid AND s.operation_state='RESERVED' AND s.step='CANCEL' AND s.boot_uuid=w.owner_boot_uuid AND s.fence_epoch=w.writer_epoch AND s.connection_id=CONNECTION_ID() AND s.fixed_lease_until=w.lease_until AND s.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='cancel under preinserted new settlement scope; actual commit/termination proof external'; END IF;
 IF NOT ((NEW.operation_state<>'COMMITTED' OR NOT EXISTS(SELECT 1 FROM physical_recovery_locator l JOIN floating_pod_state d ON d.binding_uuid=l.binding_uuid AND d.uuid=l.pod_uuid JOIN physical_recovery_footprint f ON f.binding_uuid=l.binding_uuid AND f.pod_uuid=l.pod_uuid AND f.attempt_uuid=l.current_attempt_uuid AND f.footprint_uuid=l.current_footprint_uuid WHERE l.binding_uuid=NEW.binding_uuid AND (NOT (l.domain_version <=> d.state_version) OR f.seal_state<>'SEALED' OR l.target_dimension_id<>f.target_dimension_id))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='commit requires coherent current domain/locator/SEALED footprint graph'; END IF;
 IF NOT ((NEW.operation_state<>'COMMITTED' OR NOT EXISTS(SELECT 1 FROM physical_source_unit u JOIN physical_source_forest f ON f.binding_uuid=u.binding_uuid AND f.unit_uuid=u.unit_uuid AND f.forest_uuid=u.current_forest_uuid WHERE u.binding_uuid=NEW.binding_uuid AND u.unit_state='SEALED' AND (SELECT COUNT(*) FROM physical_source_obligation o WHERE o.binding_uuid=u.binding_uuid AND o.unit_uuid=u.unit_uuid AND o.forest_uuid=u.current_forest_uuid AND o.purpose IN ('OLD_HOLD','CURRENT_HOLD') AND o.obligation_state='OPEN')<>f.carrier_count)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='commit requires every current source carrier hold; no empty shortcut'; END IF;
 IF NOT ((NEW.operation_state<>'COMMITTED' OR NOT EXISTS(SELECT 1 FROM fp_recovery_operation c JOIN fp_recovery_operation s ON s.binding_uuid=c.binding_uuid AND s.operation_uuid=c.settled_by_operation_uuid WHERE c.binding_uuid=NEW.binding_uuid AND c.operation_state='CANCELLED' AND s.operation_state<>'COMMITTED' AND NOT (s.operation_uuid=NEW.operation_uuid AND NEW.step='CANCEL'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='final result closes cancelled-to-settlement graph without deferred-trigger cycle'; END IF;
 IF NOT ((NEW.operation_state<>'COMMITTED' OR (NOT EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.created_operation_uuid=NEW.operation_uuid AND f.seal_state='DRAFT') AND NOT EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=NEW.binding_uuid AND f.created_operation_uuid=NEW.operation_uuid AND f.forest_state='DRAFT'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='creating operation cannot commit unsealed DRAFT footprint/forest'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_04_bd BEFORE DELETE ON fp_recovery_operation FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_05_bi BEFORE INSERT ON floating_pod_state FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT ((NEW.is_active=0 AND EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_recovery_reservation r ON r.binding_uuid=o.binding_uuid AND r.operation_uuid=o.operation_uuid AND r.workflow_uuid=o.workflow_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.pod_uuid=NEW.uuid AND o.operation_state='RESERVED' AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.uuid AND r.reservation_state='HELD' AND ((o.step='REARM' AND NEW.state_version=0) OR (o.step='DOMAIN_MUTATION' AND NEW.state_version>=0)))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='inactive REARM starts zero; controlled DOMAIN_MUTATION preserves requested version'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_05_bu BEFORE UPDATE ON floating_pod_state FOR EACH ROW
BEGIN
 DECLARE ix_bytes LONGBLOB;
 DECLARE ix_sha VARBINARY(32);
 DECLARE ix_forest_sha VARBINARY(32);
 DECLARE ix_members BIGINT;
 DECLARE ix_forest_octets BIGINT;
 DECLARE ix_target VARBINARY(16);
 DECLARE ix_hit BOOLEAN DEFAULT FALSE;
 DECLARE ix_length BIGINT;
 DECLARE ix_count_decimal DECIMAL(20,0);
 DECLARE ix_count BIGINT UNSIGNED;
 DECLARE ix_position BIGINT UNSIGNED;
 DECLARE ix_item VARBINARY(16);
 DECLARE ix_previous VARBINARY(16);
 DECLARE ix_max_members BIGINT;
 DECLARE ix_max_packet BIGINT;
 DECLARE ix_max_retained BIGINT;
 DECLARE fp_exact_backfill BOOLEAN DEFAULT FALSE;
 SET fp_exact_backfill=(OLD.is_active=1 AND NEW.is_active=1 AND OLD.dimension_id IS NULL AND NEW.dimension_id IS NOT NULL AND (OLD.uuid <=> NEW.uuid) AND (OLD.owner_uuid <=> NEW.owner_uuid) AND (OLD.state_version <=> NEW.state_version) AND (CAST(OLD.world_name AS BINARY) <=> CAST(NEW.world_name AS BINARY)) AND (OLD.pos_x <=> NEW.pos_x) AND (OLD.pos_y <=> NEW.pos_y) AND (OLD.pos_z <=> NEW.pos_z) AND (OLD.yaw <=> NEW.yaw) AND (OLD.pitch <=> NEW.pitch) AND (OLD.is_active <=> NEW.is_active) AND (OLD.last_updated <=> NEW.last_updated) AND (CAST(OLD.operating_mode AS BINARY) <=> CAST(NEW.operating_mode AS BINARY)) AND (OLD.binding_uuid <=> NEW.binding_uuid) AND EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.pod_uuid=NEW.uuid AND o.expected_domain_version=OLD.state_version AND o.step='DOMAIN_MUTATION' AND o.attempt_uuid IS NULL AND o.footprint_uuid IS NULL AND o.source_unit_uuid IS NULL AND o.forest_uuid IS NULL));
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT (((OLD.uuid <=> NEW.uuid) AND (OLD.binding_uuid <=> NEW.binding_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((CASE WHEN fp_exact_backfill THEN TRUE ELSE NEW.state_version=OLD.state_version+1 END) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact version increment or bounded legacy dimension backfill'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_locator l WHERE l.binding_uuid=OLD.binding_uuid AND l.pod_uuid=OLD.uuid) OR EXISTS(SELECT 1 FROM physical_recovery_locator l WHERE l.binding_uuid=OLD.binding_uuid AND l.pod_uuid=OLD.uuid AND l.domain_version=OLD.state_version)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='domain mutation must start with coherent current locator'; END IF;
 IF NOT ((OLD.is_active=1 OR NEW.is_active=0 OR EXISTS(SELECT 1 FROM physical_recovery_locator l JOIN physical_recovery_intent i ON i.binding_uuid=l.binding_uuid AND i.pod_uuid=l.pod_uuid AND i.attempt_uuid=l.current_attempt_uuid WHERE l.binding_uuid=NEW.binding_uuid AND l.pod_uuid=NEW.uuid AND i.phase='PREPARED' AND i.expected_domain_version=OLD.state_version AND (i.expected_owner_uuid <=> OLD.owner_uuid))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='activation requires current prepared attempt'; END IF;
 IF NOT ((OLD.is_active=NEW.is_active OR EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND ((OLD.is_active=0 AND NEW.is_active=1 AND o.step='ACTIVATE') OR (OLD.is_active=1 AND NEW.is_active=0 AND o.step IN ('RECALL_CHECKPOINT','ABORT_CLEANUP'))))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='active transition requires its exact critical operation'; END IF;
 IF NOT ((NEW.is_active=0 OR NEW.dimension_id IS NOT NULL) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='runtime active domain requires real dimension'; END IF;
 IF fp_exact_backfill THEN
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_footprint h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_recovery_footprint_chunk h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_recovery_intent h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_recovery_locator h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_recovery_obligation h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM fp_lifecycle_completion h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_footprint_transfer h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Pod physical history forbids generic mutation'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_source_unit u WHERE u.binding_uuid=OLD.binding_uuid AND (u.unit_state<>'SEALED' OR u.current_forest_uuid IS NULL OR NOT EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=u.binding_uuid AND f.unit_uuid=u.unit_uuid AND f.forest_uuid=u.current_forest_uuid AND f.forest_state='SEALED'))) AND NOT EXISTS(SELECT 1 FROM physical_source_transfer t WHERE t.binding_uuid=OLD.binding_uuid AND (NOT EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=t.binding_uuid AND f.unit_uuid=t.unit_uuid AND f.forest_uuid=t.old_forest_uuid AND f.forest_state='SEALED' AND f.fp_pod_uuid_index_bytes IS NOT NULL AND f.fp_pod_uuid_index_sha256 IS NOT NULL) OR NOT EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=t.binding_uuid AND f.unit_uuid=t.unit_uuid AND f.forest_uuid=t.target_forest_uuid AND f.forest_state='SEALED' AND f.fp_pod_uuid_index_bytes IS NOT NULL AND f.fp_pod_uuid_index_sha256 IS NOT NULL))) AND NOT EXISTS(SELECT 1 FROM physical_source_write w WHERE w.binding_uuid=OLD.binding_uuid AND w.write_state IN ('PREPARED','FAILED_NEEDS_RECONCILIATION')) AND NOT EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=OLD.binding_uuid AND o.operation_state='RESERVED' AND o.step IN ('SOURCE_CAPTURE','SOURCE_PREPARE','SOURCE_WRITE_PREPARE','SOURCE_WRITE_RESULT','SOURCE_WRITE_RECONCILE','SOURCE_CUTOVER','SOURCE_RETIRE','SOURCE_REMEDIATE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Pending source coverage or effect retains responsibility'; END IF;
 BEGIN
  DECLARE ix_done BOOLEAN DEFAULT FALSE;
  DECLARE ix_state VARCHAR(16);
  DECLARE ix_forests CURSOR FOR SELECT forest_state,forest_sha256,member_count,OCTET_LENGTH(forest_bytes),fp_pod_uuid_index_bytes,fp_pod_uuid_index_sha256 FROM physical_source_forest WHERE binding_uuid=OLD.binding_uuid ORDER BY unit_uuid,forest_uuid;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET ix_done=TRUE;
  SET ix_target=OLD.uuid;
  OPEN ix_forests;
  ix_forest_loop: LOOP
   FETCH ix_forests INTO ix_state,ix_forest_sha,ix_members,ix_forest_octets,ix_bytes,ix_sha;
   IF ix_done THEN LEAVE ix_forest_loop; END IF;
 IF NOT ((CAST(ix_state AS BINARY)=CAST('SEALED' AS BINARY)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Unclassified draft forest cannot prove pod absence'; END IF;
 SET ix_max_members=(SELECT max_members FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=OLD.binding_uuid);
 SET ix_max_packet=(SELECT max_sql_packet_bytes FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=OLD.binding_uuid);
 SET ix_max_retained=(SELECT max_retained_bytes FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=OLD.binding_uuid);
 IF NOT ((ix_bytes IS NOT NULL AND ix_sha IS NOT NULL AND OCTET_LENGTH(ix_sha)=32 AND ix_forest_sha IS NOT NULL AND OCTET_LENGTH(ix_forest_sha)=32) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index pair or forest hash missing'; END IF;
 IF NOT ((ix_max_members>0 AND ix_max_packet>0 AND ix_max_retained>0 AND ix_members>0 AND ix_members<=ix_max_members AND ix_forest_octets>0) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index requires exact bounded source profile'; END IF;
 SET ix_length=OCTET_LENGTH(ix_bytes);
 IF NOT ((ix_length>=74 AND ix_length<=ix_max_packet AND CAST(ix_length AS DECIMAL(20,0))+CAST(ix_forest_octets AS DECIMAL(20,0))<=ix_max_retained) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index bytes exceed packet or retained budget'; END IF;
 IF NOT ((ix_sha=UNHEX(SHA2(ix_bytes,256))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index hash mismatch'; END IF;
 IF NOT ((SUBSTRING(ix_bytes,1,4)=UNHEX('0000001E') AND SUBSTRING(ix_bytes,5,30)=UNHEX('46505F464F524553545F504F445F555549445F494E4445585F42455F5631')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index fixed tag mismatch'; END IF;
 IF NOT ((SUBSTRING(ix_bytes,35,32)=ix_forest_sha) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index original forest hash mismatch'; END IF;
 IF NOT ((ORD(SUBSTRING(ix_bytes,67,1))<128) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index count outside nonnegative signed range'; END IF;
 SET ix_count_decimal=CAST(CONV(HEX(SUBSTRING(ix_bytes,67,8)),16,10) AS DECIMAL(20,0));
 IF NOT ((MOD(ix_length-74,16)=0 AND ix_count_decimal=FLOOR((ix_length-74)/16)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index count or exact length mismatch'; END IF;
 IF NOT ((ix_count_decimal<=CAST(ix_members AS DECIMAL(20,0))*3) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index exceeds raw fixed actual identity bound'; END IF;
 SET ix_count=CAST(ix_count_decimal AS UNSIGNED);
 SET ix_position=0; SET ix_previous=NULL; SET ix_hit=FALSE;
 WHILE ix_position<ix_count DO
  SET ix_item=SUBSTRING(ix_bytes,75+ix_position*16,16);
 IF NOT ((OCTET_LENGTH(ix_item)=16 AND (ix_previous IS NULL OR CAST(ix_item AS BINARY)>CAST(ix_previous AS BINARY))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index UUID width sort or duplicate invalid'; END IF;
  IF ix_target IS NOT NULL AND ix_item=ix_target THEN SET ix_hit=TRUE; END IF;
  SET ix_previous=ix_item; SET ix_position=ix_position+1;
 END WHILE;
 IF NOT ((NOT ix_hit) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Historical source forest contains pod UUID'; END IF;
  END LOOP;
  CLOSE ix_forests;
 END;
 END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_05_bd BEFORE DELETE ON floating_pod_state FOR EACH ROW
BEGIN
 DECLARE ix_bytes LONGBLOB;
 DECLARE ix_sha VARBINARY(32);
 DECLARE ix_forest_sha VARBINARY(32);
 DECLARE ix_members BIGINT;
 DECLARE ix_forest_octets BIGINT;
 DECLARE ix_target VARBINARY(16);
 DECLARE ix_hit BOOLEAN DEFAULT FALSE;
 DECLARE ix_length BIGINT;
 DECLARE ix_count_decimal DECIMAL(20,0);
 DECLARE ix_count BIGINT UNSIGNED;
 DECLARE ix_position BIGINT UNSIGNED;
 DECLARE ix_item VARBINARY(16);
 DECLARE ix_previous VARBINARY(16);
 DECLARE ix_max_members BIGINT;
 DECLARE ix_max_packet BIGINT;
 DECLARE ix_max_retained BIGINT;
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=OLD.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((OLD.is_active=0) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='generic DELETE cannot remove active domain'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.operation_uuid=r.operation_uuid JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE r.binding_uuid=OLD.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=OLD.uuid AND r.reservation_state='HELD' AND r.workflow_uuid=o.workflow_uuid AND o.pod_uuid=OLD.uuid AND o.expected_domain_version=OLD.state_version AND o.operation_state='RESERVED' AND o.step='DOMAIN_MUTATION' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND w.lease_until>UTC_TIMESTAMP(6) AND o.attempt_uuid IS NULL AND o.footprint_uuid IS NULL AND o.source_unit_uuid IS NULL AND o.forest_uuid IS NULL)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DELETE requires exact current POD operation reservation'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_footprint h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_recovery_footprint_chunk h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_recovery_intent h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_recovery_locator h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_recovery_obligation h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM fp_lifecycle_completion h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid) AND NOT EXISTS(SELECT 1 FROM physical_footprint_transfer h WHERE h.binding_uuid=OLD.binding_uuid AND h.pod_uuid=OLD.uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Pod physical history forbids generic mutation'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_source_unit u WHERE u.binding_uuid=OLD.binding_uuid AND (u.unit_state<>'SEALED' OR u.current_forest_uuid IS NULL OR NOT EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=u.binding_uuid AND f.unit_uuid=u.unit_uuid AND f.forest_uuid=u.current_forest_uuid AND f.forest_state='SEALED'))) AND NOT EXISTS(SELECT 1 FROM physical_source_transfer t WHERE t.binding_uuid=OLD.binding_uuid AND (NOT EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=t.binding_uuid AND f.unit_uuid=t.unit_uuid AND f.forest_uuid=t.old_forest_uuid AND f.forest_state='SEALED' AND f.fp_pod_uuid_index_bytes IS NOT NULL AND f.fp_pod_uuid_index_sha256 IS NOT NULL) OR NOT EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=t.binding_uuid AND f.unit_uuid=t.unit_uuid AND f.forest_uuid=t.target_forest_uuid AND f.forest_state='SEALED' AND f.fp_pod_uuid_index_bytes IS NOT NULL AND f.fp_pod_uuid_index_sha256 IS NOT NULL))) AND NOT EXISTS(SELECT 1 FROM physical_source_write w WHERE w.binding_uuid=OLD.binding_uuid AND w.write_state IN ('PREPARED','FAILED_NEEDS_RECONCILIATION')) AND NOT EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=OLD.binding_uuid AND o.operation_state='RESERVED' AND o.step IN ('SOURCE_CAPTURE','SOURCE_PREPARE','SOURCE_WRITE_PREPARE','SOURCE_WRITE_RESULT','SOURCE_WRITE_RECONCILE','SOURCE_CUTOVER','SOURCE_RETIRE','SOURCE_REMEDIATE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Pending source coverage or effect retains responsibility'; END IF;
 BEGIN
  DECLARE ix_done BOOLEAN DEFAULT FALSE;
  DECLARE ix_state VARCHAR(16);
  DECLARE ix_forests CURSOR FOR SELECT forest_state,forest_sha256,member_count,OCTET_LENGTH(forest_bytes),fp_pod_uuid_index_bytes,fp_pod_uuid_index_sha256 FROM physical_source_forest WHERE binding_uuid=OLD.binding_uuid ORDER BY unit_uuid,forest_uuid;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET ix_done=TRUE;
  SET ix_target=OLD.uuid;
  OPEN ix_forests;
  ix_forest_loop: LOOP
   FETCH ix_forests INTO ix_state,ix_forest_sha,ix_members,ix_forest_octets,ix_bytes,ix_sha;
   IF ix_done THEN LEAVE ix_forest_loop; END IF;
 IF NOT ((CAST(ix_state AS BINARY)=CAST('SEALED' AS BINARY)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Unclassified draft forest cannot prove pod absence'; END IF;
 SET ix_max_members=(SELECT max_members FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=OLD.binding_uuid);
 SET ix_max_packet=(SELECT max_sql_packet_bytes FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=OLD.binding_uuid);
 SET ix_max_retained=(SELECT max_retained_bytes FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=OLD.binding_uuid);
 IF NOT ((ix_bytes IS NOT NULL AND ix_sha IS NOT NULL AND OCTET_LENGTH(ix_sha)=32 AND ix_forest_sha IS NOT NULL AND OCTET_LENGTH(ix_forest_sha)=32) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index pair or forest hash missing'; END IF;
 IF NOT ((ix_max_members>0 AND ix_max_packet>0 AND ix_max_retained>0 AND ix_members>0 AND ix_members<=ix_max_members AND ix_forest_octets>0) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index requires exact bounded source profile'; END IF;
 SET ix_length=OCTET_LENGTH(ix_bytes);
 IF NOT ((ix_length>=74 AND ix_length<=ix_max_packet AND CAST(ix_length AS DECIMAL(20,0))+CAST(ix_forest_octets AS DECIMAL(20,0))<=ix_max_retained) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index bytes exceed packet or retained budget'; END IF;
 IF NOT ((ix_sha=UNHEX(SHA2(ix_bytes,256))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index hash mismatch'; END IF;
 IF NOT ((SUBSTRING(ix_bytes,1,4)=UNHEX('0000001E') AND SUBSTRING(ix_bytes,5,30)=UNHEX('46505F464F524553545F504F445F555549445F494E4445585F42455F5631')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index fixed tag mismatch'; END IF;
 IF NOT ((SUBSTRING(ix_bytes,35,32)=ix_forest_sha) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index original forest hash mismatch'; END IF;
 IF NOT ((ORD(SUBSTRING(ix_bytes,67,1))<128) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index count outside nonnegative signed range'; END IF;
 SET ix_count_decimal=CAST(CONV(HEX(SUBSTRING(ix_bytes,67,8)),16,10) AS DECIMAL(20,0));
 IF NOT ((MOD(ix_length-74,16)=0 AND ix_count_decimal=FLOOR((ix_length-74)/16)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index count or exact length mismatch'; END IF;
 IF NOT ((ix_count_decimal<=CAST(ix_members AS DECIMAL(20,0))*3) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index exceeds raw fixed actual identity bound'; END IF;
 SET ix_count=CAST(ix_count_decimal AS UNSIGNED);
 SET ix_position=0; SET ix_previous=NULL; SET ix_hit=FALSE;
 WHILE ix_position<ix_count DO
  SET ix_item=SUBSTRING(ix_bytes,75+ix_position*16,16);
 IF NOT ((OCTET_LENGTH(ix_item)=16 AND (ix_previous IS NULL OR CAST(ix_item AS BINARY)>CAST(ix_previous AS BINARY))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index UUID width sort or duplicate invalid'; END IF;
  IF ix_target IS NOT NULL AND ix_item=ix_target THEN SET ix_hit=TRUE; END IF;
  SET ix_previous=ix_item; SET ix_position=ix_position+1;
 END WHILE;
 IF NOT ((NOT ix_hit) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Historical source forest contains pod UUID'; END IF;
  END LOOP;
  CLOSE ix_forests;
 END;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_06_bi BEFORE INSERT ON player_entitlement FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.created_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('ENTITLEMENT_REGISTER'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_06_bu BEFORE UPDATE ON player_entitlement FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='entitlement remains SELECT/INSERT baseline'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_06_bd BEFORE DELETE ON player_entitlement FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_07_bi BEFORE INSERT ON binding_bootstrap_intent FOR EACH ROW
BEGIN
 IF NOT ((NEW.stage='PREPARED' AND NEW.marker_ack_sha256 IS NULL AND EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.bootstrap_state='READY' AND m.format_version=4 AND m.expected_binding_uuid=NEW.binding_uuid AND m.provisioning_uuid=NEW.provisioning_uuid AND m.import_state='NONE')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='B1 fresh intent requires exact READY manifest'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_binding)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_recovery_binding'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM fp_recovery_operation)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty fp_recovery_operation'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM floating_pod_state)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty floating_pod_state'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM player_entitlement)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty player_entitlement'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM fp_recovery_reservation)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty fp_recovery_reservation'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_footprint)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_recovery_footprint'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_footprint_chunk)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_recovery_footprint_chunk'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_intent)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_recovery_intent'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_locator)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_recovery_locator'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_recovery_obligation)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_recovery_obligation'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM fp_lifecycle_completion)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty fp_lifecycle_completion'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_footprint_transfer)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_footprint_transfer'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_source_unit)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_source_unit'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_source_forest)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_source_forest'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_source_transfer)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_source_transfer'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_source_write)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_source_write'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_source_obligation)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new B1 intent requires empty physical_source_obligation'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_07_bu BEFORE UPDATE ON binding_bootstrap_intent FOR EACH ROW
BEGIN
 IF NOT (((OLD.singleton_fixed1 <=> NEW.singleton_fixed1) AND (OLD.bootstrap_operation_uuid <=> NEW.bootstrap_operation_uuid) AND (OLD.prepare_write_uuid <=> NEW.prepare_write_uuid) AND (OLD.certify_write_uuid <=> NEW.certify_write_uuid) AND (OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.binding_format_version <=> NEW.binding_format_version) AND (OLD.provisioning_uuid <=> NEW.provisioning_uuid) AND (CAST(OLD.source_world_identity AS BINARY) <=> CAST(NEW.source_world_identity AS BINARY)) AND (OLD.provision_receipt_sha256 <=> NEW.provision_receipt_sha256)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((OLD.stage='PREPARED' AND NEW.stage='COMPLETE' AND OLD.marker_ack_sha256 IS NULL AND NEW.marker_ack_sha256 IS NOT NULL AND EXISTS(SELECT 1 FROM physical_recovery_binding b WHERE b.binding_uuid=NEW.binding_uuid AND b.format_version=NEW.binding_format_version AND b.cutover_state='CERTIFIED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='B1 COMPLETE requires exact certified B and actual issuer ACK digest'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_07_bd BEFORE DELETE ON binding_bootstrap_intent FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_08_bi BEFORE INSERT ON fp_recovery_reservation FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND (o.operation_state='RESERVED' OR (o.operation_state='COMMITTED' AND o.step='RESERVE')) AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='reservation mutation belongs to actual reserve/current scope'; END IF;
 IF NOT ((NEW.reservation_state='HELD' AND NEW.reservation_revision=0) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='reservation begins held'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_08_bu BEFORE UPDATE ON fp_recovery_reservation FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND (o.operation_state='RESERVED' OR (o.operation_state='COMMITTED' AND o.step='RESERVE') OR (OLD.reservation_state='HELD' AND NEW.reservation_state='RELEASED' AND NEW.operation_uuid=OLD.operation_uuid AND NEW.workflow_uuid=OLD.workflow_uuid AND o.operation_uuid=OLD.operation_uuid AND o.workflow_uuid=OLD.workflow_uuid AND o.operation_state='COMMITTED' AND o.step='DOMAIN_MUTATION')) AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='reservation mutation belongs to actual reserve/current scope'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (CAST(OLD.resource_kind AS BINARY) <=> CAST(NEW.resource_kind AS BINARY)) AND (OLD.resource_key_hash <=> NEW.resource_key_hash) AND (OLD.resource_key <=> NEW.resource_key) AND (OLD.resource_pod_uuid <=> NEW.resource_pod_uuid) AND (OLD.resource_unit_uuid <=> NEW.resource_unit_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.reservation_revision=OLD.reservation_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT ((OLD.reservation_state<>'HELD' OR NEW.reservation_state<>'HELD' OR NEW.operation_uuid=OLD.operation_uuid OR EXISTS(SELECT 1 FROM fp_recovery_operation old_o JOIN fp_recovery_operation new_o ON new_o.binding_uuid=old_o.binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=new_o.binding_uuid WHERE old_o.binding_uuid=OLD.binding_uuid AND old_o.operation_uuid=OLD.operation_uuid AND old_o.workflow_uuid=OLD.workflow_uuid AND new_o.operation_uuid=NEW.operation_uuid AND new_o.workflow_uuid=OLD.workflow_uuid AND NEW.workflow_uuid=OLD.workflow_uuid AND new_o.operation_state='RESERVED' AND new_o.boot_uuid=w.owner_boot_uuid AND new_o.fence_epoch=w.writer_epoch AND new_o.connection_id=CONNECTION_ID() AND new_o.fixed_lease_until=w.lease_until AND w.lease_until>UTC_TIMESTAMP(6) AND ((old_o.operation_state='COMMITTED' AND new_o.previous_step_operation_uuid=old_o.operation_uuid) OR (old_o.operation_state='CANCELLED' AND new_o.previous_step_operation_uuid=old_o.settled_by_operation_uuid AND EXISTS(SELECT 1 FROM fp_recovery_operation settlement WHERE settlement.binding_uuid=old_o.binding_uuid AND settlement.operation_uuid=old_o.settled_by_operation_uuid AND settlement.workflow_uuid=old_o.workflow_uuid AND settlement.previous_step_operation_uuid=old_o.operation_uuid AND settlement.operation_state='COMMITTED' AND settlement.step='CANCEL' AND settlement.commit_witness_bytes IS NOT NULL AND settlement.commit_witness_sha256 IS NOT NULL AND settlement.result_at IS NOT NULL))))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='held retarget requires exact definitive predecessor/negative witness; UNKNOWN retains'; END IF;
 IF NOT (((OLD.reservation_state='HELD' AND NEW.reservation_state='RELEASED' AND NEW.workflow_uuid=OLD.workflow_uuid AND NEW.operation_uuid=OLD.operation_uuid) OR (OLD.reservation_state='RELEASED' AND NEW.reservation_state='HELD') OR (OLD.reservation_state='HELD' AND NEW.reservation_state='HELD' AND NEW.workflow_uuid=OLD.workflow_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='reservation cannot replace held foreign workflow'; END IF;
 IF NOT ((NEW.reservation_state<>'RELEASED' OR EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=OLD.binding_uuid AND o.operation_uuid=OLD.operation_uuid AND o.operation_state IN ('COMMITTED','CANCELLED'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='unknown RESERVED operation retains resource'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_08_bd BEFORE DELETE ON fp_recovery_reservation FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_09_bi BEFORE INSERT ON physical_recovery_footprint FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.created_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('REARM','MOVE_PREPARE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((NEW.seal_state='DRAFT') <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='footprint starts DRAFT'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND o.operation_uuid=NEW.created_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='new footprint under exact POD resource reservation'; END IF;
 IF NOT ((NEW.purpose IN ('CREATE_TARGET','MOVE_TARGET')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='MIGRATED_UNVERIFIED is offline import only, never a new runtime proof'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_09_bu BEFORE UPDATE ON physical_recovery_footprint FOR EACH ROW
BEGIN
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.pod_uuid <=> NEW.pod_uuid) AND (OLD.attempt_uuid <=> NEW.attempt_uuid) AND (OLD.footprint_uuid <=> NEW.footprint_uuid) AND (OLD.origin_version <=> NEW.origin_version) AND (CAST(OLD.target_world_name AS BINARY) <=> CAST(NEW.target_world_name AS BINARY)) AND (OLD.target_pos_x <=> NEW.target_pos_x) AND (OLD.target_pos_y <=> NEW.target_pos_y) AND (OLD.target_pos_z <=> NEW.target_pos_z) AND (OLD.target_yaw <=> NEW.target_yaw) AND (OLD.target_pitch <=> NEW.target_pitch) AND (OLD.target_dimension_id <=> NEW.target_dimension_id) AND (OLD.center_chunk_x <=> NEW.center_chunk_x) AND (OLD.center_chunk_z <=> NEW.center_chunk_z) AND (CAST(OLD.purpose AS BINARY) <=> CAST(NEW.purpose AS BINARY)) AND (OLD.created_operation_uuid <=> NEW.created_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.created_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('REARM','MOVE_PREPARE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((OLD.seal_state='DRAFT' AND NEW.seal_state='SEALED' AND (SELECT COUNT(*) FROM physical_recovery_footprint_chunk c WHERE c.binding_uuid=NEW.binding_uuid AND c.pod_uuid=NEW.pod_uuid AND c.attempt_uuid=NEW.attempt_uuid AND c.footprint_uuid=NEW.footprint_uuid)=25 AND NOT EXISTS(SELECT 1 FROM physical_recovery_footprint_chunk c WHERE c.binding_uuid=NEW.binding_uuid AND c.pod_uuid=NEW.pod_uuid AND c.attempt_uuid=NEW.attempt_uuid AND c.footprint_uuid=NEW.footprint_uuid AND (c.dimension_id<>NEW.target_dimension_id OR c.chunk_x NOT BETWEEN NEW.center_chunk_x-2 AND NEW.center_chunk_x+2 OR c.chunk_z NOT BETWEEN NEW.center_chunk_z-2 AND NEW.center_chunk_z+2))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SEALED requires exact immutable 5x5 set'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_09_bd BEFORE DELETE ON physical_recovery_footprint FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_10_bi BEFORE INSERT ON physical_recovery_footprint_chunk FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_recovery_footprint f JOIN fp_recovery_operation o ON o.binding_uuid=f.binding_uuid AND o.operation_uuid=f.created_operation_uuid JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.attempt_uuid AND f.footprint_uuid=NEW.footprint_uuid AND f.seal_state='DRAFT' AND f.target_dimension_id=NEW.dimension_id AND NEW.chunk_x BETWEEN f.center_chunk_x-2 AND f.center_chunk_x+2 AND NEW.chunk_z BETWEEN f.center_chunk_z-2 AND f.center_chunk_z+2 AND o.operation_state='RESERVED' AND o.connection_id=CONNECTION_ID() AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='only exact DRAFT chunk insert under creating scope'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_10_bu BEFORE UPDATE ON physical_recovery_footprint_chunk FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='footprint chunks immutable even before seal'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_10_bd BEFORE DELETE ON physical_recovery_footprint_chunk FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_11_bi BEFORE INSERT ON physical_recovery_intent FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.attempt_uuid AND f.footprint_uuid=NEW.target_footprint_uuid AND f.seal_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='sealed exact footprint missing'; END IF;
 IF NOT ((NEW.phase='PREPARED' AND NEW.intent_revision=0 AND EXISTS(SELECT 1 FROM floating_pod_state d WHERE d.uuid=NEW.pod_uuid AND d.binding_uuid=NEW.binding_uuid AND d.is_active=0 AND d.state_version=NEW.expected_domain_version AND (d.owner_uuid <=> NEW.expected_owner_uuid))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='prepare binds exact inactive owner/version'; END IF;
 IF NOT ((NEW.source_completion_uuid IS NULL OR EXISTS(SELECT 1 FROM fp_lifecycle_completion c JOIN fp_runtime_writer w ON w.binding_uuid=c.binding_uuid WHERE c.binding_uuid=NEW.binding_uuid AND c.receipt_uuid=NEW.source_completion_uuid AND c.pod_uuid=NEW.pod_uuid AND c.attempt_uuid=NEW.predecessor_attempt_uuid AND c.footprint_uuid=NEW.predecessor_footprint_uuid AND (c.owner_uuid <=> NEW.expected_owner_uuid) AND c.after_domain_version=NEW.expected_domain_version AND c.boot_uuid=w.owner_boot_uuid AND c.consumed_by_operation_uuid IS NULL)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='completion must be exact same boot unconsumed checkpoint'; END IF;
 IF NOT ((NEW.source_forest_uuid IS NULL OR EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=NEW.binding_uuid AND f.unit_uuid=NEW.source_unit_uuid AND f.forest_uuid=NEW.source_forest_uuid AND f.forest_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='adoption source forest must be SEALED'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('REARM'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_11_bu BEFORE UPDATE ON physical_recovery_intent FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.pod_uuid <=> NEW.pod_uuid) AND (OLD.attempt_uuid <=> NEW.attempt_uuid) AND (CAST(OLD.origin AS BINARY) <=> CAST(NEW.origin AS BINARY)) AND (OLD.target_footprint_uuid <=> NEW.target_footprint_uuid) AND (OLD.prepare_operation_uuid <=> NEW.prepare_operation_uuid) AND (OLD.expected_owner_uuid <=> NEW.expected_owner_uuid) AND (OLD.expected_domain_version <=> NEW.expected_domain_version) AND (OLD.predecessor_attempt_uuid <=> NEW.predecessor_attempt_uuid) AND (OLD.predecessor_footprint_uuid <=> NEW.predecessor_footprint_uuid) AND (OLD.source_completion_uuid <=> NEW.source_completion_uuid) AND (OLD.fresh_absence_proof_bytes <=> NEW.fresh_absence_proof_bytes) AND (OLD.fresh_absence_proof_sha256 <=> NEW.fresh_absence_proof_sha256) AND (CAST(OLD.source_kind AS BINARY) <=> CAST(NEW.source_kind AS BINARY)) AND (CAST(OLD.source_identity AS BINARY) <=> CAST(NEW.source_identity AS BINARY)) AND (OLD.catalog_generation <=> NEW.catalog_generation) AND (OLD.create_operation_uuid <=> NEW.create_operation_uuid) AND (OLD.source_unit_uuid <=> NEW.source_unit_uuid) AND (OLD.source_forest_uuid <=> NEW.source_forest_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.intent_revision=OLD.intent_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT (((OLD.phase='PREPARED' AND NEW.phase IN ('ACTIVATED','CLEANUP_PENDING')) OR (OLD.phase='CLEANUP_PENDING' AND NEW.phase='CLEANUP_VERIFIED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable terminal attempt and bounded phase progression'; END IF;
 IF NOT ((NEW.phase<>'ACTIVATED' OR EXISTS(SELECT 1 FROM floating_pod_state d JOIN physical_recovery_footprint f ON f.binding_uuid=d.binding_uuid AND f.pod_uuid=d.uuid WHERE d.binding_uuid=NEW.binding_uuid AND d.uuid=NEW.pod_uuid AND d.is_active=1 AND d.state_version=NEW.committed_domain_version AND NEW.committed_domain_version=NEW.expected_domain_version+1 AND f.attempt_uuid=NEW.attempt_uuid AND f.footprint_uuid=NEW.target_footprint_uuid AND f.seal_state='SEALED' AND CAST(d.world_name AS BINARY)=CAST(NEW.committed_world_name AS BINARY) AND d.pos_x=NEW.committed_pos_x AND d.pos_y=NEW.committed_pos_y AND d.pos_z=NEW.committed_pos_z AND d.yaw=NEW.committed_yaw AND d.pitch=NEW.committed_pitch AND d.dimension_id=NEW.committed_dimension_id AND CAST(f.target_world_name AS BINARY)=CAST(NEW.committed_world_name AS BINARY) AND f.target_pos_x=NEW.committed_pos_x AND f.target_pos_y=NEW.committed_pos_y AND f.target_pos_z=NEW.committed_pos_z AND f.target_yaw=NEW.committed_yaw AND f.target_pitch=NEW.committed_pitch AND f.target_dimension_id=NEW.committed_dimension_id)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='activation requires actual committed domain and exact target pose'; END IF;
 IF NOT ((NEW.phase<>'ACTIVATED' OR (NEW.activation_operation_uuid=NEW.last_mutation_operation_uuid AND EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.activation_operation_uuid AND o.step='ACTIVATE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='activation operation exact and typed'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('ACTIVATE','ABORT_CLEANUP'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_11_bd BEFORE DELETE ON physical_recovery_intent FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_12_bi BEFORE INSERT ON physical_recovery_locator FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.current_attempt_uuid AND f.footprint_uuid=NEW.current_footprint_uuid AND f.seal_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='sealed exact footprint missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM floating_pod_state d WHERE d.binding_uuid=NEW.binding_uuid AND d.uuid=NEW.pod_uuid AND (d.state_version <=> NEW.domain_version))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='locator must publish current domain version'; END IF;
 IF NOT ((NEW.locator_revision=0) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='locator starts revision zero'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_12_bu BEFORE UPDATE ON physical_recovery_locator FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.current_attempt_uuid AND f.footprint_uuid=NEW.current_footprint_uuid AND f.seal_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='sealed exact footprint missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM floating_pod_state d WHERE d.binding_uuid=NEW.binding_uuid AND d.uuid=NEW.pod_uuid AND (d.state_version <=> NEW.domain_version))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='locator must publish current domain version'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.pod_uuid <=> NEW.pod_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.locator_revision=OLD.locator_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT ((NEW.current_attempt_uuid=OLD.current_attempt_uuid OR EXISTS(SELECT 1 FROM physical_recovery_intent i JOIN fp_recovery_operation o ON o.binding_uuid=i.binding_uuid AND o.operation_uuid=i.prepare_operation_uuid WHERE i.binding_uuid=NEW.binding_uuid AND i.pod_uuid=NEW.pod_uuid AND i.attempt_uuid=NEW.current_attempt_uuid AND i.phase='PREPARED' AND o.step='REARM' AND i.prepare_operation_uuid=NEW.last_mutation_operation_uuid AND ((i.predecessor_attempt_uuid=OLD.current_attempt_uuid AND i.predecessor_footprint_uuid=OLD.current_footprint_uuid) OR i.fresh_absence_proof_bytes IS NOT NULL))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='attempt switch only exact authorized rearm'; END IF;
 IF NOT ((NEW.current_attempt_uuid<>OLD.current_attempt_uuid OR NEW.current_footprint_uuid=OLD.current_footprint_uuid OR EXISTS(SELECT 1 FROM physical_footprint_transfer t WHERE t.binding_uuid=NEW.binding_uuid AND t.pod_uuid=NEW.pod_uuid AND t.attempt_uuid=NEW.current_attempt_uuid AND t.from_footprint_uuid=OLD.current_footprint_uuid AND t.to_footprint_uuid=NEW.current_footprint_uuid AND t.phase='MOVE_PREPARED' AND t.commit_operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='same attempt footprint switch requires exact B4 transfer'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_12_bd BEFORE DELETE ON physical_recovery_locator FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_13_bi BEFORE INSERT ON physical_recovery_obligation FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.attempt_uuid AND f.footprint_uuid=NEW.footprint_uuid AND f.seal_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='sealed exact footprint missing'; END IF;
 IF NOT ((NEW.obligation_state='PENDING' AND NEW.obligation_revision=0 AND EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.attempt_uuid AND f.footprint_uuid=NEW.footprint_uuid AND f.origin_version=NEW.origin_version)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='obligation preserves full exact origin footprint'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_13_bu BEFORE UPDATE ON physical_recovery_obligation FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.pod_uuid <=> NEW.pod_uuid) AND (OLD.attempt_uuid <=> NEW.attempt_uuid) AND (OLD.footprint_uuid <=> NEW.footprint_uuid) AND (OLD.origin_version <=> NEW.origin_version) AND (CAST(OLD.purpose AS BINARY) <=> CAST(NEW.purpose AS BINARY)) AND (OLD.created_operation_uuid <=> NEW.created_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.obligation_revision=OLD.obligation_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT ((OLD.attempt_retirement_operation_uuid IS NULL OR (OLD.attempt_retirement_operation_uuid <=> NEW.attempt_retirement_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixed phase operation rewritten'; END IF;
 IF NOT ((OLD.obligation_state='PENDING' AND NEW.obligation_state IN ('PENDING','VERIFIED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='verified obligation terminal'; END IF;
 IF NOT ((NEW.obligation_state<>'PENDING' OR (OLD.attempt_retirement_operation_uuid IS NULL AND NEW.attempt_retirement_operation_uuid IS NOT NULL)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pending update only records real attempt retirement once'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_13_bd BEFORE DELETE ON physical_recovery_obligation FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_14_bi BEFORE INSERT ON fp_lifecycle_completion FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.attempt_uuid AND f.footprint_uuid=NEW.footprint_uuid AND f.seal_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='sealed exact footprint missing'; END IF;
 IF NOT ((NEW.consumed_by_operation_uuid IS NULL AND NEW.completion_revision=0 AND EXISTS(SELECT 1 FROM floating_pod_state d JOIN physical_recovery_locator l ON l.binding_uuid=d.binding_uuid AND l.pod_uuid=d.uuid JOIN fp_runtime_writer w ON w.binding_uuid=d.binding_uuid WHERE d.binding_uuid=NEW.binding_uuid AND d.uuid=NEW.pod_uuid AND d.is_active=0 AND d.state_version=NEW.after_domain_version AND (d.owner_uuid <=> NEW.owner_uuid) AND l.current_attempt_uuid=NEW.attempt_uuid AND l.current_footprint_uuid=NEW.footprint_uuid AND l.domain_version=d.state_version AND NEW.boot_uuid=w.owner_boot_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='completion row is same boot committed inactive checkpoint; not private Main receipt'; END IF;
 IF NOT ((NEW.completion_operation_uuid=NEW.last_mutation_operation_uuid AND EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.completion_operation_uuid AND o.boot_uuid=NEW.boot_uuid AND o.boot_generation=NEW.boot_generation AND o.original_context_bytes=NEW.original_context_bytes AND ((NEW.completion_kind='RECALL' AND o.step='RECALL_CHECKPOINT') OR (NEW.completion_kind='ABORTED_CREATE_CLEANUP' AND o.step='ABORT_CLEANUP')))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='checkpoint kind and original complete context must bind actual operation'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_14_bu BEFORE UPDATE ON fp_lifecycle_completion FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.receipt_uuid <=> NEW.receipt_uuid) AND (OLD.completion_operation_uuid <=> NEW.completion_operation_uuid) AND (CAST(OLD.completion_kind AS BINARY) <=> CAST(NEW.completion_kind AS BINARY)) AND (OLD.boot_uuid <=> NEW.boot_uuid) AND (OLD.boot_generation <=> NEW.boot_generation) AND (CAST(OLD.world_identity AS BINARY) <=> CAST(NEW.world_identity AS BINARY)) AND (CAST(OLD.source_identity AS BINARY) <=> CAST(NEW.source_identity AS BINARY)) AND (OLD.pod_uuid <=> NEW.pod_uuid) AND (OLD.owner_uuid <=> NEW.owner_uuid) AND (OLD.attempt_uuid <=> NEW.attempt_uuid) AND (OLD.footprint_uuid <=> NEW.footprint_uuid) AND (OLD.before_domain_version <=> NEW.before_domain_version) AND (OLD.after_domain_version <=> NEW.after_domain_version) AND (OLD.removal_result_bytes <=> NEW.removal_result_bytes) AND (OLD.removal_result_sha256 <=> NEW.removal_result_sha256) AND (OLD.ticket_result_bytes <=> NEW.ticket_result_bytes) AND (OLD.ticket_result_sha256 <=> NEW.ticket_result_sha256) AND (OLD.checkpoint_bytes <=> NEW.checkpoint_bytes) AND (OLD.checkpoint_sha256 <=> NEW.checkpoint_sha256) AND (OLD.original_context_bytes <=> NEW.original_context_bytes) AND (OLD.original_context_sha256 <=> NEW.original_context_sha256) AND (CAST(OLD.allowed_use AS BINARY) <=> CAST(NEW.allowed_use AS BINARY)) AND (CAST(OLD.completion_state AS BINARY) <=> CAST(NEW.completion_state AS BINARY))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.completion_revision=OLD.completion_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT ((OLD.consumed_by_operation_uuid IS NULL AND NEW.consumed_by_operation_uuid IS NOT NULL AND NEW.consumed_by_operation_uuid=NEW.last_mutation_operation_uuid AND EXISTS(SELECT 1 FROM physical_recovery_intent i WHERE i.binding_uuid=NEW.binding_uuid AND i.source_completion_uuid=NEW.receipt_uuid AND i.pod_uuid=NEW.pod_uuid AND i.predecessor_attempt_uuid=NEW.attempt_uuid AND i.predecessor_footprint_uuid=NEW.footprint_uuid AND i.expected_domain_version=NEW.after_domain_version AND (i.expected_owner_uuid <=> NEW.owner_uuid) AND i.prepare_operation_uuid=NEW.consumed_by_operation_uuid AND i.phase='PREPARED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='completion one-time exact rearm consumption'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('REARM'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_14_bd BEFORE DELETE ON fp_lifecycle_completion FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_15_bi BEFORE INSERT ON physical_footprint_transfer FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.attempt_uuid AND f.footprint_uuid=NEW.from_footprint_uuid AND f.seal_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='sealed exact footprint missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_recovery_footprint f WHERE f.binding_uuid=NEW.binding_uuid AND f.pod_uuid=NEW.pod_uuid AND f.attempt_uuid=NEW.attempt_uuid AND f.footprint_uuid=NEW.to_footprint_uuid AND f.seal_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='sealed exact footprint missing'; END IF;
 IF NOT ((NEW.phase='MOVE_PREPARED' AND NEW.transfer_revision=0 AND NEW.commit_operation_uuid IS NULL AND NEW.cleanup_operation_uuid IS NULL AND EXISTS(SELECT 1 FROM physical_recovery_locator l WHERE l.binding_uuid=NEW.binding_uuid AND l.pod_uuid=NEW.pod_uuid AND l.current_attempt_uuid=NEW.attempt_uuid AND l.current_footprint_uuid=NEW.from_footprint_uuid AND l.domain_version=NEW.expected_domain_version)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='move captures exact current from footprint'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_15_bu BEFORE UPDATE ON physical_footprint_transfer FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=NEW.pod_uuid AND r.reservation_state='HELD' AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact POD reservation missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.move_uuid <=> NEW.move_uuid) AND (OLD.pod_uuid <=> NEW.pod_uuid) AND (OLD.attempt_uuid <=> NEW.attempt_uuid) AND (OLD.from_footprint_uuid <=> NEW.from_footprint_uuid) AND (OLD.to_footprint_uuid <=> NEW.to_footprint_uuid) AND (OLD.expected_domain_version <=> NEW.expected_domain_version) AND (OLD.target_domain_version <=> NEW.target_domain_version) AND (CAST(OLD.from_world_name AS BINARY) <=> CAST(NEW.from_world_name AS BINARY)) AND (OLD.from_pos_x <=> NEW.from_pos_x) AND (OLD.from_pos_y <=> NEW.from_pos_y) AND (OLD.from_pos_z <=> NEW.from_pos_z) AND (OLD.from_yaw <=> NEW.from_yaw) AND (OLD.from_pitch <=> NEW.from_pitch) AND (OLD.from_dimension_id <=> NEW.from_dimension_id) AND (CAST(OLD.to_world_name AS BINARY) <=> CAST(NEW.to_world_name AS BINARY)) AND (OLD.to_pos_x <=> NEW.to_pos_x) AND (OLD.to_pos_y <=> NEW.to_pos_y) AND (OLD.to_pos_z <=> NEW.to_pos_z) AND (OLD.to_yaw <=> NEW.to_yaw) AND (OLD.to_pitch <=> NEW.to_pitch) AND (OLD.to_dimension_id <=> NEW.to_dimension_id) AND (OLD.prepare_operation_uuid <=> NEW.prepare_operation_uuid) AND (OLD.move_plan_bytes <=> NEW.move_plan_bytes) AND (OLD.move_plan_sha256 <=> NEW.move_plan_sha256)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.transfer_revision=OLD.transfer_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT ((OLD.commit_operation_uuid IS NULL OR (OLD.commit_operation_uuid <=> NEW.commit_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixed phase operation rewritten'; END IF;
 IF NOT ((OLD.cleanup_operation_uuid IS NULL OR (OLD.cleanup_operation_uuid <=> NEW.cleanup_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixed phase operation rewritten'; END IF;
 IF NOT (((OLD.phase='MOVE_PREPARED' AND NEW.phase='MOVE_PREPARED' AND OLD.commit_operation_uuid IS NULL AND NEW.commit_operation_uuid IS NOT NULL) OR (OLD.phase='MOVE_PREPARED' AND NEW.phase IN ('MOVE_COMMITTED_CLEANUP_PENDING','MOVE_REMEDIATION_PENDING')) OR (OLD.phase='MOVE_COMMITTED_CLEANUP_PENDING' AND NEW.phase IN ('MOVE_COMPLETED','MOVE_REMEDIATION_PENDING'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='move fixed phase topology; no terminal overwrite'; END IF;
 IF NOT ((NEW.phase NOT IN ('MOVE_COMMITTED_CLEANUP_PENDING','MOVE_COMPLETED') OR EXISTS(SELECT 1 FROM physical_recovery_locator l WHERE l.binding_uuid=NEW.binding_uuid AND l.pod_uuid=NEW.pod_uuid AND l.current_attempt_uuid=NEW.attempt_uuid AND l.current_footprint_uuid=NEW.to_footprint_uuid AND l.domain_version=NEW.target_domain_version)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='move commit must publish exact to pointer'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_15_bd BEFORE DELETE ON physical_footprint_transfer FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_16_bi BEFORE INSERT ON physical_source_unit FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((NEW.unit_state='DRAFT' AND NEW.unit_revision=0 AND NEW.current_epoch IS NULL AND NEW.current_forest_uuid IS NULL) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='source unit DRAFT precedes first forest'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_CAPTURE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='SOURCE_UNIT' AND r.resource_unit_uuid=NEW.unit_uuid AND r.reservation_state='HELD' AND o.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact source-unit resource reservation required'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_16_bu BEFORE UPDATE ON physical_source_unit FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.unit_uuid <=> NEW.unit_uuid) AND (CAST(OLD.world_identity AS BINARY) <=> CAST(NEW.world_identity AS BINARY)) AND (CAST(OLD.save_identity AS BINARY) <=> CAST(NEW.save_identity AS BINARY)) AND (CAST(OLD.loader_identity AS BINARY) <=> CAST(NEW.loader_identity AS BINARY)) AND (OLD.catalog_generation <=> NEW.catalog_generation) AND (OLD.read_generation <=> NEW.read_generation) AND (OLD.source_observation_bytes <=> NEW.source_observation_bytes) AND (OLD.source_observation_sha256 <=> NEW.source_observation_sha256) AND (OLD.created_operation_uuid <=> NEW.created_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.unit_revision=OLD.unit_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=NEW.binding_uuid AND f.unit_uuid=NEW.unit_uuid AND f.forest_uuid=NEW.current_forest_uuid AND f.source_epoch=NEW.current_epoch AND f.forest_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='unit pointer must name SEALED exact forest'; END IF;
 IF NOT (((OLD.unit_state='DRAFT' AND NEW.unit_state='SEALED' AND NEW.current_epoch>=0) OR (OLD.unit_state='SEALED' AND NEW.unit_state='SEALED' AND NEW.current_epoch=OLD.current_epoch+1 AND EXISTS(SELECT 1 FROM physical_source_transfer t WHERE t.binding_uuid=NEW.binding_uuid AND t.unit_uuid=NEW.unit_uuid AND t.old_epoch=OLD.current_epoch AND t.old_forest_uuid=OLD.current_forest_uuid AND t.target_epoch=NEW.current_epoch AND t.target_forest_uuid=NEW.current_forest_uuid AND t.phase='TARGETS_DURABLE' AND t.cutover_operation_uuid=NEW.last_mutation_operation_uuid))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='unit CAS requires predeclared transfer scope'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_CAPTURE','SOURCE_CUTOVER'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='SOURCE_UNIT' AND r.resource_unit_uuid=NEW.unit_uuid AND r.reservation_state='HELD' AND o.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact source-unit resource reservation required'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_16_bd BEFORE DELETE ON physical_source_unit FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_17_bi BEFORE INSERT ON physical_source_forest FOR EACH ROW
BEGIN
 DECLARE ix_bytes LONGBLOB;
 DECLARE ix_sha VARBINARY(32);
 DECLARE ix_forest_sha VARBINARY(32);
 DECLARE ix_members BIGINT;
 DECLARE ix_forest_octets BIGINT;
 DECLARE ix_target VARBINARY(16);
 DECLARE ix_hit BOOLEAN DEFAULT FALSE;
 DECLARE ix_length BIGINT;
 DECLARE ix_count_decimal DECIMAL(20,0);
 DECLARE ix_count BIGINT UNSIGNED;
 DECLARE ix_position BIGINT UNSIGNED;
 DECLARE ix_item VARBINARY(16);
 DECLARE ix_previous VARBINARY(16);
 DECLARE ix_max_members BIGINT;
 DECLARE ix_max_packet BIGINT;
 DECLARE ix_max_retained BIGINT;
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.created_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((NEW.forest_state='DRAFT') <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forest starts immutable DRAFT'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='SOURCE_UNIT' AND r.resource_unit_uuid=NEW.unit_uuid AND r.reservation_state='HELD' AND o.operation_uuid=NEW.created_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forest captured under exact reserved source unit'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.created_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_CAPTURE','SOURCE_PREPARE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NEW.fp_pod_uuid_index_bytes IS NOT NULL OR NEW.fp_pod_uuid_index_sha256 IS NOT NULL THEN
  SET ix_bytes=NEW.fp_pod_uuid_index_bytes; SET ix_sha=NEW.fp_pod_uuid_index_sha256; SET ix_forest_sha=NEW.forest_sha256;
  SET ix_members=NEW.member_count; SET ix_forest_octets=OCTET_LENGTH(NEW.forest_bytes); SET ix_target=NULL;
 SET ix_max_members=(SELECT max_members FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=NEW.binding_uuid);
 SET ix_max_packet=(SELECT max_sql_packet_bytes FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=NEW.binding_uuid);
 SET ix_max_retained=(SELECT max_retained_bytes FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=NEW.binding_uuid);
 IF NOT ((ix_bytes IS NOT NULL AND ix_sha IS NOT NULL AND OCTET_LENGTH(ix_sha)=32 AND ix_forest_sha IS NOT NULL AND OCTET_LENGTH(ix_forest_sha)=32) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index pair or forest hash missing'; END IF;
 IF NOT ((ix_max_members>0 AND ix_max_packet>0 AND ix_max_retained>0 AND ix_members>0 AND ix_members<=ix_max_members AND ix_forest_octets>0) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index requires exact bounded source profile'; END IF;
 SET ix_length=OCTET_LENGTH(ix_bytes);
 IF NOT ((ix_length>=74 AND ix_length<=ix_max_packet AND CAST(ix_length AS DECIMAL(20,0))+CAST(ix_forest_octets AS DECIMAL(20,0))<=ix_max_retained) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index bytes exceed packet or retained budget'; END IF;
 IF NOT ((ix_sha=UNHEX(SHA2(ix_bytes,256))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index hash mismatch'; END IF;
 IF NOT ((SUBSTRING(ix_bytes,1,4)=UNHEX('0000001E') AND SUBSTRING(ix_bytes,5,30)=UNHEX('46505F464F524553545F504F445F555549445F494E4445585F42455F5631')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index fixed tag mismatch'; END IF;
 IF NOT ((SUBSTRING(ix_bytes,35,32)=ix_forest_sha) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index original forest hash mismatch'; END IF;
 IF NOT ((ORD(SUBSTRING(ix_bytes,67,1))<128) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index count outside nonnegative signed range'; END IF;
 SET ix_count_decimal=CAST(CONV(HEX(SUBSTRING(ix_bytes,67,8)),16,10) AS DECIMAL(20,0));
 IF NOT ((MOD(ix_length-74,16)=0 AND ix_count_decimal=FLOOR((ix_length-74)/16)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index count or exact length mismatch'; END IF;
 IF NOT ((ix_count_decimal<=CAST(ix_members AS DECIMAL(20,0))*3) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index exceeds raw fixed actual identity bound'; END IF;
 SET ix_count=CAST(ix_count_decimal AS UNSIGNED);
 SET ix_position=0; SET ix_previous=NULL; SET ix_hit=FALSE;
 WHILE ix_position<ix_count DO
  SET ix_item=SUBSTRING(ix_bytes,75+ix_position*16,16);
 IF NOT ((OCTET_LENGTH(ix_item)=16 AND (ix_previous IS NULL OR CAST(ix_item AS BINARY)>CAST(ix_previous AS BINARY))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index UUID width sort or duplicate invalid'; END IF;
  IF ix_target IS NOT NULL AND ix_item=ix_target THEN SET ix_hit=TRUE; END IF;
  SET ix_previous=ix_item; SET ix_position=ix_position+1;
 END WHILE;
 END IF;
 IF NOT ((NEW.forest_state<>'SEALED' OR (NEW.fp_pod_uuid_index_bytes IS NOT NULL AND NEW.fp_pod_uuid_index_sha256 IS NOT NULL)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SEALED forest requires complete original FP index'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_17_bu BEFORE UPDATE ON physical_source_forest FOR EACH ROW
BEGIN
 DECLARE ix_bytes LONGBLOB;
 DECLARE ix_sha VARBINARY(32);
 DECLARE ix_forest_sha VARBINARY(32);
 DECLARE ix_members BIGINT;
 DECLARE ix_forest_octets BIGINT;
 DECLARE ix_target VARBINARY(16);
 DECLARE ix_hit BOOLEAN DEFAULT FALSE;
 DECLARE ix_length BIGINT;
 DECLARE ix_count_decimal DECIMAL(20,0);
 DECLARE ix_count BIGINT UNSIGNED;
 DECLARE ix_position BIGINT UNSIGNED;
 DECLARE ix_item VARBINARY(16);
 DECLARE ix_previous VARBINARY(16);
 DECLARE ix_max_members BIGINT;
 DECLARE ix_max_packet BIGINT;
 DECLARE ix_max_retained BIGINT;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.unit_uuid <=> NEW.unit_uuid) AND (OLD.forest_uuid <=> NEW.forest_uuid) AND (OLD.source_epoch <=> NEW.source_epoch) AND (CAST(OLD.forest_codec AS BINARY) <=> CAST(NEW.forest_codec AS BINARY)) AND (OLD.forest_bytes <=> NEW.forest_bytes) AND (OLD.forest_sha256 <=> NEW.forest_sha256) AND (OLD.member_count <=> NEW.member_count) AND (OLD.root_count <=> NEW.root_count) AND (OLD.carrier_count <=> NEW.carrier_count) AND (OLD.retained_bytes <=> NEW.retained_bytes) AND (OLD.created_operation_uuid <=> NEW.created_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.created_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((OLD.forest_state='DRAFT' AND NEW.forest_state='SEALED' AND EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND NEW.member_count<=m.max_members AND NEW.retained_bytes<=m.max_retained_bytes AND OCTET_LENGTH(NEW.forest_bytes)<=m.max_sql_packet_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forest seal budgets; typed codec/root/carrier membership requires issuer validation'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='SOURCE_UNIT' AND r.resource_unit_uuid=NEW.unit_uuid AND r.reservation_state='HELD' AND o.operation_uuid=NEW.created_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forest captured under exact reserved source unit'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.created_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_CAPTURE','SOURCE_PREPARE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT (((OLD.fp_pod_uuid_index_bytes <=> NEW.fp_pod_uuid_index_bytes) AND (OLD.fp_pod_uuid_index_sha256 <=> NEW.fp_pod_uuid_index_sha256)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='original FP membership index immutable'; END IF;
 IF NEW.fp_pod_uuid_index_bytes IS NOT NULL OR NEW.fp_pod_uuid_index_sha256 IS NOT NULL THEN
  SET ix_bytes=NEW.fp_pod_uuid_index_bytes; SET ix_sha=NEW.fp_pod_uuid_index_sha256; SET ix_forest_sha=NEW.forest_sha256;
  SET ix_members=NEW.member_count; SET ix_forest_octets=OCTET_LENGTH(NEW.forest_bytes); SET ix_target=NULL;
 SET ix_max_members=(SELECT max_members FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=NEW.binding_uuid);
 SET ix_max_packet=(SELECT max_sql_packet_bytes FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=NEW.binding_uuid);
 SET ix_max_retained=(SELECT max_retained_bytes FROM fp_schema_manifest WHERE singleton_fixed1=1 AND expected_binding_uuid=NEW.binding_uuid);
 IF NOT ((ix_bytes IS NOT NULL AND ix_sha IS NOT NULL AND OCTET_LENGTH(ix_sha)=32 AND ix_forest_sha IS NOT NULL AND OCTET_LENGTH(ix_forest_sha)=32) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index pair or forest hash missing'; END IF;
 IF NOT ((ix_max_members>0 AND ix_max_packet>0 AND ix_max_retained>0 AND ix_members>0 AND ix_members<=ix_max_members AND ix_forest_octets>0) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index requires exact bounded source profile'; END IF;
 SET ix_length=OCTET_LENGTH(ix_bytes);
 IF NOT ((ix_length>=74 AND ix_length<=ix_max_packet AND CAST(ix_length AS DECIMAL(20,0))+CAST(ix_forest_octets AS DECIMAL(20,0))<=ix_max_retained) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index bytes exceed packet or retained budget'; END IF;
 IF NOT ((ix_sha=UNHEX(SHA2(ix_bytes,256))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index hash mismatch'; END IF;
 IF NOT ((SUBSTRING(ix_bytes,1,4)=UNHEX('0000001E') AND SUBSTRING(ix_bytes,5,30)=UNHEX('46505F464F524553545F504F445F555549445F494E4445585F42455F5631')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index fixed tag mismatch'; END IF;
 IF NOT ((SUBSTRING(ix_bytes,35,32)=ix_forest_sha) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index original forest hash mismatch'; END IF;
 IF NOT ((ORD(SUBSTRING(ix_bytes,67,1))<128) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index count outside nonnegative signed range'; END IF;
 SET ix_count_decimal=CAST(CONV(HEX(SUBSTRING(ix_bytes,67,8)),16,10) AS DECIMAL(20,0));
 IF NOT ((MOD(ix_length-74,16)=0 AND ix_count_decimal=FLOOR((ix_length-74)/16)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index count or exact length mismatch'; END IF;
 IF NOT ((ix_count_decimal<=CAST(ix_members AS DECIMAL(20,0))*3) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index exceeds raw fixed actual identity bound'; END IF;
 SET ix_count=CAST(ix_count_decimal AS UNSIGNED);
 SET ix_position=0; SET ix_previous=NULL; SET ix_hit=FALSE;
 WHILE ix_position<ix_count DO
  SET ix_item=SUBSTRING(ix_bytes,75+ix_position*16,16);
 IF NOT ((OCTET_LENGTH(ix_item)=16 AND (ix_previous IS NULL OR CAST(ix_item AS BINARY)>CAST(ix_previous AS BINARY))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='FP index UUID width sort or duplicate invalid'; END IF;
  IF ix_target IS NOT NULL AND ix_item=ix_target THEN SET ix_hit=TRUE; END IF;
  SET ix_previous=ix_item; SET ix_position=ix_position+1;
 END WHILE;
 END IF;
 IF NOT ((NEW.forest_state<>'SEALED' OR (NEW.fp_pod_uuid_index_bytes IS NOT NULL AND NEW.fp_pod_uuid_index_sha256 IS NOT NULL)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SEALED forest requires complete original FP index'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_17_bd BEFORE DELETE ON physical_source_forest FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_18_bi BEFORE INSERT ON physical_source_transfer FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((NEW.phase='SOURCE_TRANSFER_PREPARED' AND NEW.transfer_revision=0 AND NEW.cutover_operation_uuid IS NULL AND NEW.retire_operation_uuid IS NULL AND EXISTS(SELECT 1 FROM physical_source_unit u JOIN physical_source_forest f ON f.binding_uuid=u.binding_uuid AND f.unit_uuid=u.unit_uuid WHERE u.binding_uuid=NEW.binding_uuid AND u.unit_uuid=NEW.unit_uuid AND u.current_epoch=NEW.old_epoch AND u.current_forest_uuid=NEW.old_forest_uuid AND f.forest_uuid=NEW.target_forest_uuid AND f.source_epoch=NEW.target_epoch AND f.forest_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='transfer retains actual old and SEALED target'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_PREPARE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='SOURCE_UNIT' AND r.resource_unit_uuid=NEW.unit_uuid AND r.reservation_state='HELD' AND o.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact source-unit resource reservation required'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_18_bu BEFORE UPDATE ON physical_source_transfer FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.transfer_uuid <=> NEW.transfer_uuid) AND (OLD.unit_uuid <=> NEW.unit_uuid) AND (OLD.old_epoch <=> NEW.old_epoch) AND (OLD.old_forest_uuid <=> NEW.old_forest_uuid) AND (OLD.target_epoch <=> NEW.target_epoch) AND (OLD.target_forest_uuid <=> NEW.target_forest_uuid) AND (OLD.transfer_plan_bytes <=> NEW.transfer_plan_bytes) AND (OLD.transfer_plan_sha256 <=> NEW.transfer_plan_sha256) AND (OLD.prepare_operation_uuid <=> NEW.prepare_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.transfer_revision=OLD.transfer_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT ((OLD.cutover_operation_uuid IS NULL OR (OLD.cutover_operation_uuid <=> NEW.cutover_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixed phase operation rewritten'; END IF;
 IF NOT ((OLD.retire_operation_uuid IS NULL OR (OLD.retire_operation_uuid <=> NEW.retire_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixed phase operation rewritten'; END IF;
 IF NOT (((OLD.phase='SOURCE_TRANSFER_PREPARED' AND NEW.phase IN ('TARGETS_DURABLE','REMEDIATION_REQUIRED')) OR (OLD.phase='TARGETS_DURABLE' AND NEW.phase='TARGETS_DURABLE' AND OLD.cutover_operation_uuid IS NULL AND NEW.cutover_operation_uuid IS NOT NULL) OR (OLD.phase='TARGETS_DURABLE' AND NEW.phase IN ('CUTOVER_COMMITTED_RETIRE_PENDING','REMEDIATION_REQUIRED')) OR (OLD.phase='CUTOVER_COMMITTED_RETIRE_PENDING' AND NEW.phase IN ('COMPLETED','REMEDIATION_REQUIRED'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='source transfer progression only'; END IF;
 IF NOT ((NEW.phase<>'TARGETS_DURABLE' OR (NOT EXISTS(SELECT 1 FROM physical_source_obligation o LEFT JOIN physical_source_write w ON w.binding_uuid=o.binding_uuid AND w.write_uuid=o.write_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.unit_uuid=NEW.unit_uuid AND o.forest_uuid=NEW.target_forest_uuid AND o.purpose='TARGET_STAGED' AND (w.write_uuid IS NULL OR w.write_state<>'DURABLE')) AND (SELECT COUNT(*) FROM physical_source_obligation o WHERE o.binding_uuid=NEW.binding_uuid AND o.unit_uuid=NEW.unit_uuid AND o.forest_uuid=NEW.target_forest_uuid AND o.purpose='TARGET_STAGED')=(SELECT carrier_count FROM physical_source_forest f WHERE f.binding_uuid=NEW.binding_uuid AND f.unit_uuid=NEW.unit_uuid AND f.forest_uuid=NEW.target_forest_uuid))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='all target carrier obligations require actual durable write records'; END IF;
 IF NOT ((NEW.phase NOT IN ('CUTOVER_COMMITTED_RETIRE_PENDING','COMPLETED') OR (EXISTS(SELECT 1 FROM physical_source_unit u WHERE u.binding_uuid=NEW.binding_uuid AND u.unit_uuid=NEW.unit_uuid AND u.current_epoch=NEW.target_epoch AND u.current_forest_uuid=NEW.target_forest_uuid) AND NOT EXISTS(SELECT 1 FROM physical_source_obligation o WHERE o.binding_uuid=NEW.binding_uuid AND o.unit_uuid=NEW.unit_uuid AND ((o.forest_uuid=NEW.old_forest_uuid AND o.purpose IN ('OLD_HOLD','CURRENT_HOLD')) OR (o.forest_uuid=NEW.target_forest_uuid AND o.purpose='TARGET_STAGED')) AND o.obligation_state<>'DISCHARGED'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='cutover actual pointer and prior holds must agree'; END IF;
 IF NOT ((NEW.phase<>'COMPLETED' OR NOT EXISTS(SELECT 1 FROM physical_source_obligation o WHERE o.binding_uuid=NEW.binding_uuid AND o.unit_uuid=NEW.unit_uuid AND o.forest_uuid=NEW.old_forest_uuid AND o.purpose='OLD_RETIRE' AND o.obligation_state<>'DISCHARGED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='completed transfer retains actual retirement evidence'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_PREPARE','SOURCE_WRITE_RESULT','SOURCE_CUTOVER','SOURCE_RETIRE','SOURCE_REMEDIATE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((NEW.phase NOT IN ('CUTOVER_COMMITTED_RETIRE_PENDING','COMPLETED') OR ((SELECT COUNT(*) FROM physical_source_obligation o WHERE o.binding_uuid=NEW.binding_uuid AND o.unit_uuid=NEW.unit_uuid AND o.forest_uuid=NEW.old_forest_uuid AND o.purpose='OLD_RETIRE')=(SELECT carrier_count FROM physical_source_forest f WHERE f.binding_uuid=NEW.binding_uuid AND f.unit_uuid=NEW.unit_uuid AND f.forest_uuid=NEW.old_forest_uuid) AND (SELECT COUNT(*) FROM physical_source_obligation o WHERE o.binding_uuid=NEW.binding_uuid AND o.unit_uuid=NEW.unit_uuid AND o.forest_uuid=NEW.target_forest_uuid AND o.purpose='CURRENT_HOLD' AND o.obligation_state='OPEN')=(SELECT carrier_count FROM physical_source_forest f WHERE f.binding_uuid=NEW.binding_uuid AND f.unit_uuid=NEW.unit_uuid AND f.forest_uuid=NEW.target_forest_uuid))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='cutover appends complete old retirement and target current holds'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='SOURCE_UNIT' AND r.resource_unit_uuid=NEW.unit_uuid AND r.reservation_state='HELD' AND o.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='exact source-unit resource reservation required'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_18_bd BEFORE DELETE ON physical_source_transfer FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_19_bi BEFORE INSERT ON physical_source_write FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((NEW.write_state='PREPARED' AND NEW.write_revision=0 AND EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='CARRIER' AND r.resource_key_hash=NEW.carrier_key_hash AND r.resource_key=NEW.carrier_key AND r.reservation_state='HELD' AND r.workflow_uuid=NEW.workflow_uuid AND r.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='native write requires exact held carrier workflow'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND m.limit_profile_sha256=NEW.limit_profile_sha256 AND NEW.payload_length<=m.max_chunk_bytes AND NEW.undo_length<=m.max_chunk_bytes AND NEW.payload_length<=m.max_native_artifact_bytes AND NEW.undo_length<=m.max_native_artifact_bytes AND OCTET_LENGTH(NEW.member_coverage_bytes)<=m.max_sql_packet_bytes AND OCTET_LENGTH(NEW.obligation_revisions_bytes)<=m.max_sql_packet_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='explicit actual limit profile required'; END IF;
 IF NOT ((NOT EXISTS(SELECT 1 FROM physical_source_write w WHERE w.binding_uuid=NEW.binding_uuid AND w.carrier_key_hash=NEW.carrier_key_hash AND w.write_generation>=NEW.write_generation)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='generation cannot reuse older prepared/result write'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_WRITE_PREPARE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_19_bu BEFORE UPDATE ON physical_source_write FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.write_uuid <=> NEW.write_uuid) AND (OLD.workflow_uuid <=> NEW.workflow_uuid) AND (CAST(OLD.resource_kind AS BINARY) <=> CAST(NEW.resource_kind AS BINARY)) AND (OLD.carrier_key_hash <=> NEW.carrier_key_hash) AND (OLD.carrier_key <=> NEW.carrier_key) AND (CAST(OLD.save_identity AS BINARY) <=> CAST(NEW.save_identity AS BINARY)) AND (CAST(OLD.loader_identity AS BINARY) <=> CAST(NEW.loader_identity AS BINARY)) AND (CAST(OLD.region_identity AS BINARY) <=> CAST(NEW.region_identity AS BINARY)) AND (OLD.slot_x <=> NEW.slot_x) AND (OLD.slot_z <=> NEW.slot_z) AND (OLD.write_generation <=> NEW.write_generation) AND (OLD.payload_descriptor_bytes <=> NEW.payload_descriptor_bytes) AND (OLD.payload_descriptor_sha256 <=> NEW.payload_descriptor_sha256) AND (OLD.payload_length <=> NEW.payload_length) AND (OLD.payload_sha256 <=> NEW.payload_sha256) AND (CAST(OLD.payload_artifact_identity AS BINARY) <=> CAST(NEW.payload_artifact_identity AS BINARY)) AND (CAST(OLD.undo_kind AS BINARY) <=> CAST(NEW.undo_kind AS BINARY)) AND (OLD.undo_descriptor_bytes <=> NEW.undo_descriptor_bytes) AND (OLD.undo_descriptor_sha256 <=> NEW.undo_descriptor_sha256) AND (OLD.undo_length <=> NEW.undo_length) AND (OLD.undo_content_sha256 <=> NEW.undo_content_sha256) AND (CAST(OLD.undo_artifact_identity AS BINARY) <=> CAST(NEW.undo_artifact_identity AS BINARY)) AND (OLD.member_coverage_bytes <=> NEW.member_coverage_bytes) AND (OLD.member_coverage_sha256 <=> NEW.member_coverage_sha256) AND (OLD.obligation_revisions_bytes <=> NEW.obligation_revisions_bytes) AND (OLD.obligation_revisions_sha256 <=> NEW.obligation_revisions_sha256) AND (OLD.limit_profile_sha256 <=> NEW.limit_profile_sha256) AND (CAST(OLD.receipt_codec AS BINARY) <=> CAST(NEW.receipt_codec AS BINARY)) AND (OLD.prepared_operation_uuid <=> NEW.prepared_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.write_revision=OLD.write_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT (((OLD.write_state='PREPARED' AND NEW.write_state IN ('DURABLE','NO_PUT','SUPERSEDED','FAILED_NEEDS_RECONCILIATION')) OR (OLD.write_state='FAILED_NEEDS_RECONCILIATION' AND NEW.write_state='DURABLE')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='native outcome cannot be blindly retried or terminally rewritten'; END IF;
 IF NOT ((OLD.failure_diagnostic_identity IS NULL OR (OLD.failure_diagnostic_identity <=> NEW.failure_diagnostic_identity)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='native original diagnostic retained'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_WRITE_RESULT','SOURCE_WRITE_RECONCILE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_19_bd BEFORE DELETE ON physical_source_write FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_20_bi BEFORE INSERT ON physical_source_obligation FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((NEW.obligation_state='OPEN' AND NEW.obligation_revision=0 AND EXISTS(SELECT 1 FROM physical_source_forest f WHERE f.binding_uuid=NEW.binding_uuid AND f.unit_uuid=NEW.unit_uuid AND f.forest_uuid=NEW.forest_uuid AND f.forest_state='SEALED')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='source obligation begins against exact full SEALED forest'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='CARRIER' AND r.resource_key_hash=NEW.carrier_key_hash AND r.resource_key=NEW.carrier_key)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='full carrier identity cannot alias its hash'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_CAPTURE','SOURCE_PREPARE','SOURCE_WRITE_PREPARE','SOURCE_CUTOVER'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='CARRIER' AND r.resource_key_hash=NEW.carrier_key_hash AND r.resource_key=NEW.carrier_key AND r.reservation_state='HELD' AND o.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='carrier reservation must retain exact full identity and workflow'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_20_bu BEFORE UPDATE ON physical_source_obligation FOR EACH ROW
BEGIN
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='normal format3 admission missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.unit_uuid <=> NEW.unit_uuid) AND (OLD.forest_uuid <=> NEW.forest_uuid) AND (CAST(OLD.resource_kind AS BINARY) <=> CAST(NEW.resource_kind AS BINARY)) AND (OLD.carrier_key_hash <=> NEW.carrier_key_hash) AND (OLD.carrier_key <=> NEW.carrier_key) AND (CAST(OLD.purpose AS BINARY) <=> CAST(NEW.purpose AS BINARY)) AND (OLD.created_operation_uuid <=> NEW.created_operation_uuid) AND (OLD.write_uuid <=> NEW.write_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='immutable identity or original history changed'; END IF;
 IF NOT ((NEW.obligation_revision=OLD.obligation_revision+1) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revision must increment exactly once'; END IF;
 IF NOT ((OLD.obligation_state='OPEN' AND NEW.obligation_state='DISCHARGED') <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='source obligation purpose/history immutable; discharge once'; END IF;
 IF NOT (((NEW.purpose IN ('OLD_HOLD','TARGET_STAGED') AND NEW.discharge_kind='CUTOVER' AND EXISTS(SELECT 1 FROM physical_source_transfer t JOIN physical_source_unit u ON u.binding_uuid=t.binding_uuid AND u.unit_uuid=t.unit_uuid WHERE t.binding_uuid=NEW.binding_uuid AND t.unit_uuid=NEW.unit_uuid AND t.cutover_operation_uuid=NEW.discharged_operation_uuid AND t.phase='TARGETS_DURABLE' AND u.current_epoch=t.target_epoch AND u.current_forest_uuid=t.target_forest_uuid AND ((NEW.forest_uuid=t.old_forest_uuid AND NEW.purpose IN ('OLD_HOLD','CURRENT_HOLD')) OR (NEW.forest_uuid=t.target_forest_uuid AND NEW.purpose='TARGET_STAGED')))) OR (NEW.purpose='CURRENT_HOLD' AND NEW.discharge_kind='SUCCESSOR_CUTOVER' AND EXISTS(SELECT 1 FROM physical_source_transfer t JOIN physical_source_unit u ON u.binding_uuid=t.binding_uuid AND u.unit_uuid=t.unit_uuid WHERE t.binding_uuid=NEW.binding_uuid AND t.unit_uuid=NEW.unit_uuid AND t.cutover_operation_uuid=NEW.discharged_operation_uuid AND t.phase='TARGETS_DURABLE' AND u.current_epoch=t.target_epoch AND u.current_forest_uuid=t.target_forest_uuid AND ((NEW.forest_uuid=t.old_forest_uuid AND NEW.purpose IN ('OLD_HOLD','CURRENT_HOLD')) OR (NEW.forest_uuid=t.target_forest_uuid AND NEW.purpose='TARGET_STAGED')))) OR (NEW.purpose='OLD_RETIRE' AND NEW.discharge_kind='WORLD_RETIREMENT' AND EXISTS(SELECT 1 FROM physical_source_write w WHERE w.binding_uuid=NEW.binding_uuid AND w.write_uuid=NEW.write_uuid AND w.write_state='DURABLE')) OR (NEW.purpose='UNDO' AND NEW.discharge_kind='SAFE_UNDO' AND EXISTS(SELECT 1 FROM physical_source_write w WHERE w.binding_uuid=NEW.binding_uuid AND w.write_uuid=NEW.write_uuid AND w.write_state IN ('DURABLE','NO_PUT','SUPERSEDED')))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='purpose-specific discharge; no unknown/lease/absence shortcut'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6) AND o.step IN ('SOURCE_CUTOVER','SOURCE_RETIRE','SOURCE_WRITE_RECONCILE','SOURCE_REMEDIATE'))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='current transaction operation scope missing'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_reservation r JOIN fp_recovery_operation o ON o.binding_uuid=r.binding_uuid AND o.workflow_uuid=r.workflow_uuid WHERE r.binding_uuid=NEW.binding_uuid AND r.resource_kind='CARRIER' AND r.resource_key_hash=NEW.carrier_key_hash AND r.resource_key=NEW.carrier_key AND r.reservation_state='HELD' AND o.operation_uuid=NEW.last_mutation_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='carrier reservation must retain exact full identity and workflow'; END IF;
END$$

CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_20_bd BEFORE DELETE ON physical_source_obligation FOR EACH ROW
BEGIN
 IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='history DELETE forbidden'; END IF;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_21_bi BEFORE INSERT ON fp_script_catalog FOR EACH ROW
BEGIN
  IF NOT ((NEW.catalog_revision=0 AND NEW.next_version=1 AND OCTET_LENGTH(NEW.snapshot_bytes)=0 AND NEW.last_mutation_operation_uuid IS NULL AND EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN binding_bootstrap_intent i ON i.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND m.import_state='NONE' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='LEGACY_UNCERTIFIED' AND i.stage='PREPARED' AND i.binding_format_version=2 AND i.provisioning_uuid=m.provisioning_uuid) AND NOT EXISTS(SELECT 1 FROM fp_recovery_operation)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='script catalog requires exact B1 prepared genesis'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND (COALESCE(OCTET_LENGTH(NEW.snapshot_bytes),0))<=m.max_sql_packet_bytes AND (COALESCE(OCTET_LENGTH(NEW.snapshot_bytes),0))<=m.max_retained_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='catalog exceeds manifest envelope'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_21_bu BEFORE UPDATE ON fp_script_catalog FOR EACH ROW
BEGIN
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='catalog normal admission missing'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='SCRIPT_PUBLICATION' AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='catalog original operation scope mismatch'; END IF;
  IF NOT (((NEW.binding_uuid<=>OLD.binding_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='catalog identity immutable'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_script_operation p WHERE p.binding_uuid=NEW.binding_uuid AND p.operation_uuid=NEW.last_mutation_operation_uuid AND p.result_code='APPLIED' AND p.expected_catalog_revision=OLD.catalog_revision AND p.after_catalog_revision=NEW.catalog_revision AND p.before_bytes=OLD.snapshot_bytes AND p.before_sha256=OLD.snapshot_sha256 AND p.after_bytes=NEW.snapshot_bytes AND p.after_sha256=NEW.snapshot_sha256 AND ((p.request_kind='SUBMIT' AND OLD.next_version IS NOT NULL AND p.version=OLD.next_version AND NEW.next_version<=>IF(OLD.next_version=9223372036854775807,NULL,OLD.next_version+1) AND EXISTS(SELECT 1 FROM fp_script_candidate d WHERE d.binding_uuid=p.binding_uuid AND d.behavior_id=p.behavior_id AND d.version=p.version AND d.source_sha256=p.source_sha256 AND d.submission_operation_uuid=p.operation_uuid)) OR (p.request_kind IN ('APPROVE','REVOKE') AND (NEW.next_version<=>OLD.next_version) AND EXISTS(SELECT 1 FROM fp_script_version v WHERE v.binding_uuid=p.binding_uuid AND v.behavior_id=p.behavior_id AND v.version=p.version AND v.source_sha256=p.source_sha256 AND v.compilation_sha256=p.compilation_sha256 AND v.last_mutation_operation_uuid=p.operation_uuid AND ((p.request_kind='APPROVE' AND v.status='APPROVED' AND v.approval_catalog_revision=NEW.catalog_revision) OR (p.request_kind='REVOKE' AND v.status='REVOKED' AND v.revocation_catalog_revision=NEW.catalog_revision))))))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='catalog transition does not match publication effect'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND (COALESCE(OCTET_LENGTH(NEW.snapshot_bytes),0))<=m.max_sql_packet_bytes AND (COALESCE(OCTET_LENGTH(NEW.snapshot_bytes),0))<=m.max_retained_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='catalog exceeds manifest envelope'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_21_bd BEFORE DELETE ON fp_script_catalog FOR EACH ROW
BEGIN
  IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='script catalog deletion forbidden'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_22_bi BEFORE INSERT ON fp_script_candidate FOR EACH ROW
BEGIN
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='candidate normal admission missing'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='SCRIPT_PUBLICATION' AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='candidate original operation scope mismatch'; END IF;
  IF NOT ((NEW.check_status='PENDING' AND NEW.check_revision=0 AND NEW.last_mutation_operation_uuid=NEW.submission_operation_uuid) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='candidate must begin pending'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_script_operation p JOIN fp_script_catalog c ON c.binding_uuid=p.binding_uuid WHERE p.binding_uuid=NEW.binding_uuid AND p.operation_uuid=NEW.last_mutation_operation_uuid AND p.request_kind='SUBMIT' AND p.result_code='APPLIED' AND p.behavior_id=NEW.behavior_id AND p.version=NEW.version AND p.source_sha256=NEW.source_sha256 AND p.expected_catalog_revision=c.catalog_revision AND p.before_bytes=c.snapshot_bytes AND p.before_sha256=c.snapshot_sha256 AND (p.compilation_sha256 IS NULL AND c.next_version=NEW.version))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='candidate submit intent mismatch'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND (COALESCE(OCTET_LENGTH(NEW.source_bytes),0)+COALESCE(OCTET_LENGTH(NEW.check_evidence_bytes),0)+COALESCE(OCTET_LENGTH(NEW.diagnostics_bytes),0))<=m.max_sql_packet_bytes AND (COALESCE(OCTET_LENGTH(NEW.source_bytes),0)+COALESCE(OCTET_LENGTH(NEW.check_evidence_bytes),0)+COALESCE(OCTET_LENGTH(NEW.diagnostics_bytes),0))<=m.max_retained_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='candidate exceeds manifest envelope'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_22_bu BEFORE UPDATE ON fp_script_candidate FOR EACH ROW
BEGIN
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='check normal admission missing'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='SCRIPT_PUBLICATION' AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='check original operation scope mismatch'; END IF;
  IF NOT (((NEW.binding_uuid<=>OLD.binding_uuid) AND (NEW.behavior_id<=>OLD.behavior_id) AND (NEW.version<=>OLD.version) AND (NEW.source_bytes<=>OLD.source_bytes) AND (NEW.source_sha256<=>OLD.source_sha256) AND (NEW.submitted_by<=>OLD.submitted_by) AND (NEW.submission_operation_uuid<=>OLD.submission_operation_uuid)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='submitted source identity immutable'; END IF;
  IF NOT ((NEW.last_mutation_operation_uuid<>OLD.last_mutation_operation_uuid AND OLD.check_revision<9223372036854775807 AND NEW.check_revision=OLD.check_revision+1 AND NEW.check_status IN ('VALID','INVALID')) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='check revision transition invalid'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_script_operation p JOIN fp_script_catalog c ON c.binding_uuid=p.binding_uuid WHERE p.binding_uuid=NEW.binding_uuid AND p.operation_uuid=NEW.last_mutation_operation_uuid AND p.request_kind='CHECK' AND p.result_code='APPLIED' AND p.behavior_id=NEW.behavior_id AND p.version=NEW.version AND p.source_sha256=NEW.source_sha256 AND p.expected_catalog_revision=c.catalog_revision AND p.before_bytes=c.snapshot_bytes AND p.before_sha256=c.snapshot_sha256 AND ((NEW.compilation_sha256<=>p.compilation_sha256)))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='candidate check intent mismatch'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND (COALESCE(OCTET_LENGTH(NEW.source_bytes),0)+COALESCE(OCTET_LENGTH(NEW.check_evidence_bytes),0)+COALESCE(OCTET_LENGTH(NEW.diagnostics_bytes),0))<=m.max_sql_packet_bytes AND (COALESCE(OCTET_LENGTH(NEW.source_bytes),0)+COALESCE(OCTET_LENGTH(NEW.check_evidence_bytes),0)+COALESCE(OCTET_LENGTH(NEW.diagnostics_bytes),0))<=m.max_retained_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='candidate exceeds manifest envelope'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_22_bd BEFORE DELETE ON fp_script_candidate FOR EACH ROW
BEGIN
  IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='candidate history deletion forbidden'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_23_bi BEFORE INSERT ON fp_script_version FOR EACH ROW
BEGIN
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='approval normal admission missing'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='SCRIPT_PUBLICATION' AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='approval original operation scope mismatch'; END IF;
  IF NOT ((NEW.status='APPROVED' AND NEW.last_mutation_operation_uuid=NEW.approval_operation_uuid) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='version must begin approved'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_script_candidate d WHERE d.binding_uuid=NEW.binding_uuid AND d.behavior_id=NEW.behavior_id AND d.version=NEW.version AND d.check_status='VALID' AND d.check_revision=NEW.approved_check_revision AND d.source_sha256=NEW.source_sha256 AND d.compilation_sha256=NEW.compilation_sha256 AND d.compiler_version=NEW.compiler_version AND d.schema_version=NEW.schema_version AND d.schema_sha256=NEW.schema_sha256)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='approval requires exact current valid candidate'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_script_operation p JOIN fp_script_catalog c ON c.binding_uuid=p.binding_uuid WHERE p.binding_uuid=NEW.binding_uuid AND p.operation_uuid=NEW.last_mutation_operation_uuid AND p.request_kind='APPROVE' AND p.result_code='APPLIED' AND p.behavior_id=NEW.behavior_id AND p.version=NEW.version AND p.source_sha256=NEW.source_sha256 AND p.expected_catalog_revision=c.catalog_revision AND p.before_bytes=c.snapshot_bytes AND p.before_sha256=c.snapshot_sha256 AND (p.compilation_sha256=NEW.compilation_sha256 AND p.after_catalog_revision=NEW.approval_catalog_revision))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='approval intent mismatch'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND (COALESCE(OCTET_LENGTH(NEW.compilation_bytes),0)+COALESCE(OCTET_LENGTH(NEW.layout_config_bytes),0))<=m.max_sql_packet_bytes AND (COALESCE(OCTET_LENGTH(NEW.compilation_bytes),0)+COALESCE(OCTET_LENGTH(NEW.layout_config_bytes),0))<=m.max_retained_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='version exceeds manifest envelope'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_23_bu BEFORE UPDATE ON fp_script_version FOR EACH ROW
BEGIN
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revocation normal admission missing'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='SCRIPT_PUBLICATION' AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revocation original operation scope mismatch'; END IF;
  IF NOT (((NEW.binding_uuid<=>OLD.binding_uuid) AND (NEW.behavior_id<=>OLD.behavior_id) AND (NEW.version<=>OLD.version) AND (NEW.source_sha256<=>OLD.source_sha256) AND (NEW.compilation_bytes<=>OLD.compilation_bytes) AND (NEW.compilation_sha256<=>OLD.compilation_sha256) AND (NEW.compiler_version<=>OLD.compiler_version) AND (NEW.schema_version<=>OLD.schema_version) AND (NEW.schema_sha256<=>OLD.schema_sha256) AND (NEW.layout_config_bytes<=>OLD.layout_config_bytes) AND (NEW.layout_config_sha256<=>OLD.layout_config_sha256) AND (NEW.approved_by<=>OLD.approved_by) AND (NEW.approval_operation_uuid<=>OLD.approval_operation_uuid) AND (NEW.approved_check_revision<=>OLD.approved_check_revision) AND (NEW.approval_catalog_revision<=>OLD.approval_catalog_revision)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='approved artifact identity immutable'; END IF;
  IF NOT ((OLD.status='APPROVED' AND NEW.status='REVOKED' AND NEW.last_mutation_operation_uuid=NEW.revocation_operation_uuid) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='only approved to revoked transition allowed'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_script_operation p JOIN fp_script_catalog c ON c.binding_uuid=p.binding_uuid WHERE p.binding_uuid=NEW.binding_uuid AND p.operation_uuid=NEW.last_mutation_operation_uuid AND p.request_kind='REVOKE' AND p.result_code='APPLIED' AND p.behavior_id=NEW.behavior_id AND p.version=NEW.version AND p.source_sha256=NEW.source_sha256 AND p.expected_catalog_revision=c.catalog_revision AND p.before_bytes=c.snapshot_bytes AND p.before_sha256=c.snapshot_sha256 AND (p.compilation_sha256=NEW.compilation_sha256 AND p.after_catalog_revision=NEW.revocation_catalog_revision))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='revocation intent mismatch'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND (COALESCE(OCTET_LENGTH(NEW.compilation_bytes),0)+COALESCE(OCTET_LENGTH(NEW.layout_config_bytes),0))<=m.max_sql_packet_bytes AND (COALESCE(OCTET_LENGTH(NEW.compilation_bytes),0)+COALESCE(OCTET_LENGTH(NEW.layout_config_bytes),0))<=m.max_retained_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='version exceeds manifest envelope'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_23_bd BEFORE DELETE ON fp_script_version FOR EACH ROW
BEGIN
  IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='approved history deletion forbidden'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_24_bi BEFORE INSERT ON fp_script_operation FOR EACH ROW
BEGIN
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid WHERE m.singleton_fixed1=1 AND m.format_version=4 AND m.bootstrap_state='READY' AND b.binding_uuid=NEW.binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='script operation normal admission missing'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_runtime_writer w ON w.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.operation_uuid AND o.step='SCRIPT_PUBLICATION' AND o.operation_state='RESERVED' AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND o.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='script original operation scope mismatch'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_recovery_operation o JOIN fp_script_catalog c ON c.binding_uuid=o.binding_uuid WHERE o.binding_uuid=NEW.binding_uuid AND o.operation_uuid=NEW.operation_uuid AND o.request_identity_bytes=NEW.request_bytes AND o.request_identity_sha256=NEW.request_sha256 AND c.catalog_revision=NEW.expected_catalog_revision AND c.snapshot_bytes=NEW.before_bytes AND c.snapshot_sha256=NEW.before_sha256 AND ((NEW.result_code='REJECTED') OR (NEW.request_kind='SUBMIT' AND c.next_version=NEW.version) OR (NEW.request_kind='CHECK' AND EXISTS(SELECT 1 FROM fp_script_candidate d WHERE d.binding_uuid=NEW.binding_uuid AND d.behavior_id=NEW.behavior_id AND d.version=NEW.version AND d.source_sha256=NEW.source_sha256)) OR (NEW.request_kind='APPROVE' AND EXISTS(SELECT 1 FROM fp_script_candidate d WHERE d.binding_uuid=NEW.binding_uuid AND d.behavior_id=NEW.behavior_id AND d.version=NEW.version AND d.source_sha256=NEW.source_sha256 AND d.check_status='VALID' AND d.compilation_sha256=NEW.compilation_sha256)) OR (NEW.request_kind='REVOKE' AND EXISTS(SELECT 1 FROM fp_script_version v WHERE v.binding_uuid=NEW.binding_uuid AND v.behavior_id=NEW.behavior_id AND v.version=NEW.version AND v.source_sha256=NEW.source_sha256 AND v.compilation_sha256=NEW.compilation_sha256 AND v.status='APPROVED'))))) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='script request or catalog preimage mismatch'; END IF;
  IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m WHERE m.singleton_fixed1=1 AND (COALESCE(OCTET_LENGTH(NEW.request_bytes),0)+COALESCE(OCTET_LENGTH(NEW.before_bytes),0)+COALESCE(OCTET_LENGTH(NEW.after_bytes),0)+COALESCE(OCTET_LENGTH(NEW.receipt_bytes),0))<=m.max_sql_packet_bytes AND (COALESCE(OCTET_LENGTH(NEW.request_bytes),0)+COALESCE(OCTET_LENGTH(NEW.before_bytes),0)+COALESCE(OCTET_LENGTH(NEW.after_bytes),0)+COALESCE(OCTET_LENGTH(NEW.receipt_bytes),0))<=m.max_retained_bytes)) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='script operation exceeds manifest envelope'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_24_bu BEFORE UPDATE ON fp_script_operation FOR EACH ROW
BEGIN
  IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='script operation immutable'; END IF;
END$$
CREATE DEFINER='fp_s4_guard_definer'@'localhost' TRIGGER fp_s4_24_bd BEFORE DELETE ON fp_script_operation FOR EACH ROW
BEGIN
  IF NOT ((FALSE) <=> TRUE) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='script receipt history deletion forbidden'; END IF;
END$$
DELIMITER ;

-- SOURCE v4: complete actual 20-table/368-column/60-trigger capture, same18sets.
-- New guard/column definitions require independent newly approved pack; no old digest reuse.
-- FORMAT3 new 18-result-set privileged metadata profile; SOURCE ONLY.
-- Complete static schema including all20relations and all60guards. No prior baseline/hash/capability reuse.
SET SESSION sql_mode = 'STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin;
DELIMITER $$
CREATE DEFINER='fp_s4_audit_definer'@'localhost'
PROCEDURE `fp_script_publication1`.`fp_audit_schema_script_publication1`()
LANGUAGE SQL
SQL SECURITY DEFINER
READS SQL DATA
COMMENT 'FP script-publication1 exact24/72 metadata audit; SELECT-only body'
BEGIN
  -- RS01 HEADER: cardinality exactly1. USER() is connected caller, CURRENT_USER() is definer.
  SELECT @@server_uuid AS server_uuid, @@version AS server_version,
    CONNECTION_ID() AS connection_id, CURRENT_USER() AS audit_definer,
    USER() AS connected_caller, CURRENT_ROLE() AS effective_roles,
    DATABASE() AS routine_default_schema, UTC_TIMESTAMP(6) AS observed_db_time,
    'fp_script_publication1' AS audited_schema,
    m.singleton_fixed1, m.backend, m.format_version, m.ddl_sha256, m.provisioning_uuid,
    m.bootstrap_state, m.expected_server_uuid, m.expected_binding_uuid,
    m.import_state, m.import_operation_uuid, m.import_manifest_sha256,
    m.import_snapshot_sha256, m.import_receipt_sha256, m.guard_manifest_sha256,
    m.configuration_sha256, m.source_provenance_kind, m.source_provenance_bytes, m.source_provenance_sha256, m.limit_profile_encoding, m.limit_profile_bytes, m.limit_profile_sha256, m.max_region_bytes, m.max_chunk_bytes, m.max_decompressed_bytes, m.max_nbt_depth, m.max_members, m.max_retained_bytes, m.max_sql_packet_bytes, m.max_native_artifact_bytes
  FROM `fp_script_publication1`.`fp_schema_manifest` AS m WHERE m.singleton_fixed1=1;

  -- RS02 TABLES: complete fixed-schema set, unexpected tables/views are visible and rejected.
  -- Approximate row counts/timestamps/cardinality are not static-definition identity.
  SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, VERSION, ROW_FORMAT,
    TABLE_COLLATION, CREATE_OPTIONS, TABLE_COMMENT
  FROM INFORMATION_SCHEMA.TABLES
  WHERE CAST(TABLE_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
  ORDER BY CAST(TABLE_NAME AS BINARY);

  -- RS03 COLUMNS: full column definitions, including generated expression and exact collation.
  SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT,
    IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,
    NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME,
    COLLATION_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, COLUMN_COMMENT, GENERATION_EXPRESSION
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE CAST(TABLE_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
  ORDER BY CAST(TABLE_NAME AS BINARY), ORDINAL_POSITION;

  -- RS04 STATISTICS: complete index key parts; no approximate CARDINALITY in identity.
  SELECT TABLE_SCHEMA, TABLE_NAME, NON_UNIQUE, INDEX_SCHEMA, INDEX_NAME,
    SEQ_IN_INDEX, COLUMN_NAME, COLLATION, SUB_PART, PACKED, NULLABLE,
    INDEX_TYPE, COMMENT, INDEX_COMMENT, IS_VISIBLE, EXPRESSION
  FROM INFORMATION_SCHEMA.STATISTICS
  WHERE CAST(TABLE_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
  ORDER BY CAST(TABLE_NAME AS BINARY), CAST(INDEX_NAME AS BINARY), SEQ_IN_INDEX;

  -- RS05 TABLE_CONSTRAINTS: PK/unique/FK/CHECK identities and CHECK enforcement.
  SELECT CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME,
    TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_TYPE, ENFORCED
  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
  WHERE CAST(TABLE_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
  ORDER BY CAST(TABLE_NAME AS BINARY), CAST(CONSTRAINT_NAME AS BINARY), CONSTRAINT_TYPE;

  -- RS06 KEY_COLUMN_USAGE: complete ordered constrained/referenced column tuples.
  SELECT CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_CATALOG,
    TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT,
    REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
  WHERE CAST(TABLE_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
  ORDER BY CAST(TABLE_NAME AS BINARY), CAST(CONSTRAINT_NAME AS BINARY), ORDINAL_POSITION;

  -- RS07 REFERENTIAL_CONSTRAINTS: exact parent key and referential actions.
  SELECT CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME,
    UNIQUE_CONSTRAINT_CATALOG, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME,
    MATCH_OPTION, UPDATE_RULE, DELETE_RULE, TABLE_NAME, REFERENCED_TABLE_NAME
  FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
  WHERE CAST(CONSTRAINT_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
  ORDER BY CAST(TABLE_NAME AS BINARY), CAST(CONSTRAINT_NAME AS BINARY);

  -- RS08 CHECK_CONSTRAINTS: full expressions, not just names/counts.
  SELECT CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, CHECK_CLAUSE,
    OCTET_LENGTH(CHECK_CLAUSE) AS clause_octets,
    UNHEX(SHA2(CAST(CHECK_CLAUSE AS BINARY),256)) AS clause_sha256
  FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
  WHERE CAST(CONSTRAINT_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
  ORDER BY CAST(CONSTRAINT_NAME AS BINARY);

  -- RS09 TRIGGERS: whole schema, not expected-name IN filtering that hides extra objects.
  SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE,
    EVENT_MANIPULATION, ACTION_TIMING, ACTION_ORDER, ACTION_ORIENTATION,
    ACTION_STATEMENT, SQL_MODE, DEFINER, CHARACTER_SET_CLIENT,
    COLLATION_CONNECTION, DATABASE_COLLATION,
    OCTET_LENGTH(ACTION_STATEMENT) AS statement_octets,
    UNHEX(SHA2(CAST(ACTION_STATEMENT AS BINARY),256)) AS statement_sha256
  FROM INFORMATION_SCHEMA.TRIGGERS
  WHERE CAST(TRIGGER_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
  ORDER BY CAST(TRIGGER_NAME AS BINARY);

  -- RS10 required auditself only: exactly one complete definition/security/definer row.
  -- ROUTINE_DEFINITION NULL is failure; READS label is not proof that body is read-only.
  SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION,
    SECURITY_TYPE, SQL_DATA_ACCESS, SQL_MODE, DEFINER, CHARACTER_SET_CLIENT,
    COLLATION_CONNECTION, DATABASE_COLLATION, IS_DETERMINISTIC,
    DATA_TYPE, DTD_IDENTIFIER, ROUTINE_COMMENT,
    OCTET_LENGTH(ROUTINE_DEFINITION) AS definition_octets,
    UNHEX(SHA2(CAST(ROUTINE_DEFINITION AS BINARY),256)) AS definition_sha256
  FROM INFORMATION_SCHEMA.ROUTINES
  WHERE CAST(ROUTINE_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
    AND CAST(ROUTINE_NAME AS BINARY)=CAST('fp_audit_schema_script_publication1' AS BINARY)
    AND ROUTINE_TYPE='PROCEDURE'
  ORDER BY CAST(ROUTINE_NAME AS BINARY), ROUTINE_TYPE;

  -- RS11 required auditself parameters only: zero rows with singleton RS10 proves no arguments.
  SELECT SPECIFIC_CATALOG, SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION,
    PARAMETER_MODE, PARAMETER_NAME, DATA_TYPE, DTD_IDENTIFIER,
    CHARACTER_SET_NAME, COLLATION_NAME
  FROM INFORMATION_SCHEMA.PARAMETERS
  WHERE CAST(SPECIFIC_SCHEMA AS BINARY)=CAST('fp_script_publication1' AS BINARY)
    AND CAST(SPECIFIC_NAME AS BINARY)=CAST('fp_audit_schema_script_publication1' AS BINARY)
  ORDER BY CAST(SPECIFIC_NAME AS BINARY), ORDINAL_POSITION;

  -- RS12 BINDING: current scope only, no certification mutation or new identity.
  SELECT singleton_fixed1, binding_uuid, cutover_state, format_version
  FROM `fp_script_publication1`.`physical_recovery_binding` ORDER BY singleton_fixed1;

  -- RS13 WRITER: DB server UUID is not game boot UUID. Receipt values are metadata identity.
  SELECT singleton_fixed1, binding_uuid, writer_epoch, owner_boot_uuid,
    lease_until, last_commit_operation_uuid, last_commit_fingerprint
  FROM `fp_script_publication1`.`fp_runtime_writer` ORDER BY singleton_fixed1;

  -- RS14 FOOTER: must match RS01 entire persisted manifest/instance/schema/identity tuple.
  -- observed_db_time may advance; all stored identities and security/caller context must remain exact.
  SELECT @@server_uuid AS server_uuid, @@version AS server_version,
    CONNECTION_ID() AS connection_id, CURRENT_USER() AS audit_definer,
    USER() AS connected_caller, CURRENT_ROLE() AS effective_roles,
    DATABASE() AS routine_default_schema, UTC_TIMESTAMP(6) AS observed_db_time,
    'fp_script_publication1' AS audited_schema,
    m.singleton_fixed1, m.backend, m.format_version, m.ddl_sha256, m.provisioning_uuid,
    m.bootstrap_state, m.expected_server_uuid, m.expected_binding_uuid,
    m.import_state, m.import_operation_uuid, m.import_manifest_sha256,
    m.import_snapshot_sha256, m.import_receipt_sha256, m.guard_manifest_sha256,
    m.configuration_sha256, m.source_provenance_kind, m.source_provenance_bytes, m.source_provenance_sha256, m.limit_profile_encoding, m.limit_profile_bytes, m.limit_profile_sha256, m.max_region_bytes, m.max_chunk_bytes, m.max_decompressed_bytes, m.max_nbt_depth, m.max_members, m.max_retained_bytes, m.max_sql_packet_bytes, m.max_native_artifact_bytes
  FROM `fp_script_publication1`.`fp_schema_manifest` AS m WHERE m.singleton_fixed1=1;

  -- RS15..18: effective direct privileges visible for this locked audit definer ONLY.
  -- Caller effective grants/roles require the outer same-connection SHOW GRANTS envelope.
  SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PRIVILEGE_TYPE,IS_GRANTABLE
   FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES
   WHERE TABLE_SCHEMA='fp_script_publication1' AND GRANTEE='''fp_s4_audit_definer''@''localhost'''
   ORDER BY CAST(TABLE_NAME AS BINARY),CAST(PRIVILEGE_TYPE AS BINARY);
  SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,PRIVILEGE_TYPE,IS_GRANTABLE
   FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES
   WHERE TABLE_SCHEMA='fp_script_publication1' AND GRANTEE='''fp_s4_audit_definer''@''localhost'''
   ORDER BY CAST(TABLE_NAME AS BINARY),CAST(COLUMN_NAME AS BINARY),CAST(PRIVILEGE_TYPE AS BINARY);
  SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,PRIVILEGE_TYPE,IS_GRANTABLE
   FROM INFORMATION_SCHEMA.SCHEMA_PRIVILEGES
   WHERE TABLE_SCHEMA='fp_script_publication1' AND GRANTEE='''fp_s4_audit_definer''@''localhost'''
   ORDER BY CAST(PRIVILEGE_TYPE AS BINARY);
  SELECT GRANTEE,TABLE_CATALOG,PRIVILEGE_TYPE,IS_GRANTABLE
   FROM INFORMATION_SCHEMA.USER_PRIVILEGES WHERE GRANTEE='''fp_s4_audit_definer''@''localhost'''
   ORDER BY CAST(PRIVILEGE_TYPE AS BINARY);
END$$
DELIMITER ;

-- SOURCE ONLY: locked principals; execute grants only through approved SEC controller.
-- No global privilege, history DELETE, runtime DDL, manifest UPDATE or schema-wide EXECUTE.
GRANT SELECT ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (last_commit_fingerprint,last_commit_operation_uuid,lease_until,owner_boot_uuid,writer_epoch) ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (cutover_state) ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (commit_witness_bytes,commit_witness_sha256,operation_state,original_diagnostic_identity,original_diagnostic_sha256,result_at,settled_by_operation_uuid) ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (dimension_id,is_active,last_mutation_operation_uuid,last_updated,operating_mode,owner_uuid,pitch,pos_x,pos_y,pos_z,state_version,world_name,yaw) ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`player_entitlement` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (marker_ack_sha256,stage) ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`binding_bootstrap_intent` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (last_mutation_operation_uuid,operation_uuid,reservation_revision,reservation_state,workflow_uuid) ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (seal_state) ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_footprint_chunk` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (activation_operation_uuid,cleanup_operation_uuid,cleanup_proof_bytes,cleanup_proof_sha256,committed_at,committed_dimension_id,committed_domain_version,committed_pitch,committed_pos_x,committed_pos_y,committed_pos_z,committed_world_name,committed_yaw,intent_revision,last_mutation_operation_uuid,phase) ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (current_attempt_uuid,current_footprint_uuid,domain_version,last_mutation_operation_uuid,locator_revision,locator_state,reason_code,target_dimension_id) ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_locator` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (attempt_retirement_operation_uuid,last_mutation_operation_uuid,obligation_revision,obligation_state,verification_proof_bytes,verification_proof_sha256,verified_operation_uuid) ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_recovery_obligation` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (completion_revision,consumed_by_operation_uuid,last_mutation_operation_uuid) ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (cleanup_operation_uuid,cleanup_proof_bytes,cleanup_proof_sha256,commit_operation_uuid,last_mutation_operation_uuid,phase,transfer_revision) ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_footprint_transfer` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (current_epoch,current_forest_uuid,last_mutation_operation_uuid,unit_revision,unit_state) ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (forest_state) ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (cutover_operation_uuid,last_mutation_operation_uuid,phase,retire_operation_uuid,retirement_proof_bytes,retirement_proof_sha256,target_receipts_bytes,target_receipts_sha256,transfer_revision) ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_transfer` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (durable_receipt_bytes,durable_receipt_sha256,failure_diagnostic_identity,failure_diagnostic_sha256,last_mutation_operation_uuid,resolved_operation_uuid,write_revision,write_state) ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_importer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_guard_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_runtime'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (discharge_kind,discharge_proof_bytes,discharge_proof_sha256,discharged_operation_uuid,last_mutation_operation_uuid,obligation_revision,obligation_state) ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_provisioner'@'localhost';
GRANT INDEX ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_provisioner'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_importer'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`physical_source_obligation` TO 'fp_s4_importer'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`fp_lifecycle_completion` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`fp_recovery_reservation` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`physical_recovery_binding` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`physical_recovery_footprint` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`physical_source_forest` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_provisioner'@'localhost';
GRANT REFERENCES ON `fp_script_publication1`.`physical_source_write` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.* TO 'fp_s4_audit_definer'@'localhost';
GRANT TRIGGER ON `fp_script_publication1`.* TO 'fp_s4_audit_definer'@'localhost';
GRANT ALTER ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_provisioner'@'localhost';
GRANT ALTER ON `fp_script_publication1`.`physical_recovery_intent` TO 'fp_s4_provisioner'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_provisioner'@'localhost';
GRANT INSERT ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_provisioner'@'localhost';
GRANT UPDATE (bootstrap_state,guard_manifest_sha256,import_manifest_sha256,import_state,import_operation_uuid,import_snapshot_sha256,import_receipt_sha256) ON `fp_script_publication1`.`fp_schema_manifest` TO 'fp_s4_provisioner'@'localhost';
GRANT UPDATE (last_commit_fingerprint,last_commit_operation_uuid,lease_until,owner_boot_uuid,writer_epoch) ON `fp_script_publication1`.`fp_runtime_writer` TO 'fp_s4_importer'@'localhost';
GRANT UPDATE (current_epoch,current_forest_uuid,last_mutation_operation_uuid,unit_revision,unit_state) ON `fp_script_publication1`.`physical_source_unit` TO 'fp_s4_importer'@'localhost';
GRANT UPDATE (commit_witness_bytes,commit_witness_sha256,operation_state,original_diagnostic_identity,original_diagnostic_sha256,result_at,settled_by_operation_uuid) ON `fp_script_publication1`.`fp_recovery_operation` TO 'fp_s4_importer'@'localhost';
GRANT LOCK TABLES ON `fp_script_publication1`.* TO 'fp_s4_runtime'@'localhost';
GRANT CREATE ROUTINE ON `fp_script_publication1`.* TO 'fp_s4_provisioner'@'localhost';
GRANT EXECUTE ON PROCEDURE `fp_script_publication1`.`fp_audit_schema_script_publication1` TO 'fp_s4_audit_definer'@'localhost';
GRANT EXECUTE ON PROCEDURE `fp_script_publication1`.`fp_audit_schema_script_publication1` TO 'fp_s4_runtime'@'localhost';

-- v4 sole new DELETE privilege; no DELETE on other19relations.
GRANT DELETE ON `fp_script_publication1`.`floating_pod_state` TO 'fp_s4_runtime'@'localhost';

-- Exact additional ScriptPublication1 grants; no new DELETE or operation UPDATE.
GRANT SELECT,TRIGGER ON `fp_script_publication1`.`fp_script_catalog` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT,CREATE,INDEX,TRIGGER ON `fp_script_publication1`.`fp_script_catalog` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_script_catalog` TO 'fp_s4_importer'@'localhost';
GRANT SELECT,INSERT ON `fp_script_publication1`.`fp_script_catalog` TO 'fp_s4_runtime'@'localhost';
GRANT SELECT,TRIGGER ON `fp_script_publication1`.`fp_script_candidate` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT,CREATE,INDEX,TRIGGER ON `fp_script_publication1`.`fp_script_candidate` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_script_candidate` TO 'fp_s4_importer'@'localhost';
GRANT SELECT,INSERT ON `fp_script_publication1`.`fp_script_candidate` TO 'fp_s4_runtime'@'localhost';
GRANT SELECT,TRIGGER ON `fp_script_publication1`.`fp_script_version` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT,CREATE,INDEX,TRIGGER ON `fp_script_publication1`.`fp_script_version` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_script_version` TO 'fp_s4_importer'@'localhost';
GRANT SELECT,INSERT ON `fp_script_publication1`.`fp_script_version` TO 'fp_s4_runtime'@'localhost';
GRANT SELECT,TRIGGER ON `fp_script_publication1`.`fp_script_operation` TO 'fp_s4_guard_definer'@'localhost';
GRANT SELECT,CREATE,INDEX,TRIGGER ON `fp_script_publication1`.`fp_script_operation` TO 'fp_s4_provisioner'@'localhost';
GRANT SELECT ON `fp_script_publication1`.`fp_script_operation` TO 'fp_s4_importer'@'localhost';
GRANT SELECT,INSERT ON `fp_script_publication1`.`fp_script_operation` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (catalog_revision,snapshot_bytes,snapshot_sha256,next_version,last_mutation_operation_uuid) ON `fp_script_publication1`.`fp_script_catalog` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (check_evidence_bytes,check_evidence_sha256,check_revision,check_status,compilation_sha256,compiler_version,diagnostics_bytes,diagnostics_sha256,last_mutation_operation_uuid,loader_artifact_sha256,loader_id,schema_sha256,schema_version) ON `fp_script_publication1`.`fp_script_candidate` TO 'fp_s4_runtime'@'localhost';
GRANT UPDATE (status,revoked_by,revocation_operation_uuid,revocation_catalog_revision,last_mutation_operation_uuid) ON `fp_script_publication1`.`fp_script_version` TO 'fp_s4_runtime'@'localhost';
