package com.wi.floatingpod.server;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.List;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import com.wi.floatingpod.server.DbFirstBootstrapRecordCodec.OwnerData;
import com.wi.floatingpod.server.DbFirstBootstrapRecordCodec.WitnessRef;
import com.wi.floatingpod.server.DbFirstStartupRetirementCodec.*;
import static com.wi.floatingpod.server.DbFirstStartupRetirementCodec.POST_SERVICE_AUTOSAVE_CRASH_UNCHANGED_BUSINESS_DATA;
import static com.wi.floatingpod.server.DbFirstStartupRetirementCodec.COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE;
import static com.wi.floatingpod.server.DbFirstStartupRetirementCodec.COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE;
import static com.wi.floatingpod.server.DbFirstStartupRetirementCodec.BOOT_CENSUS_CLAIM_FAILURE_UNCHANGED_BUSINESS_DATA;
import static com.wi.floatingpod.server.DbFirstStartupRetirementCodec.BOOT_RECOVERY_PUBLICATION_FAILURE_UNCHANGED_BUSINESS_DATA;

class DbFirstStartupRetirementCodecTest {
    @Test void activeStopV4PinsExactPodActivationEpochAndChainWithoutChangingV3() throws Exception {
        var r=request(); var create=UUID.randomUUID(); var preparation=UUID.randomUUID();
        var active=new ActiveChainIdentity(UUID.randomUUID(),UUID.randomUUID(),12L,"56".repeat(32));
        var value=new RequestData(r.operationUuid(),r.failedBoot(),r.bootstrapOperationUuid(),r.serverUuid(),
            r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),r.recipeSha256(),
            r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),r.sourceArchiveSha256(),
            r.crashEvidenceSha256(),4,DbFirstStartupRetirementCodec.COMMITTED_ACTIVE_POD_STOP_SEALING_FAILURE,
            "34".repeat(32),java.util.Optional.of(create),java.util.Optional.of(preparation),java.util.Optional.of(active));
        byte[] bytes=DbFirstStartupRetirementCodec.encodeRequest(value);
        assertEquals(4,ByteBuffer.wrap(bytes).getInt(8));
        assertEquals(value,DbFirstStartupRetirementCodec.decodeRequest(bytes));
        assertArrayEquals(bytes,DbFirstStartupRetirementCodec.encodeRequest(DbFirstStartupRetirementCodec.decodeRequest(bytes)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),
            r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),
            r.sourceArchiveSha256(),r.crashEvidenceSha256(),4,DbFirstStartupRetirementCodec.COMMITTED_ACTIVE_POD_STOP_SEALING_FAILURE,
            "34".repeat(32),java.util.Optional.of(create),java.util.Optional.of(preparation)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),
            r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),
            r.sourceArchiveSha256(),r.crashEvidenceSha256(),3,COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE,
            "34".repeat(32),java.util.Optional.of(create),java.util.Optional.of(preparation),java.util.Optional.of(active)));
        assertThrows(IllegalArgumentException.class,()->new ActiveChainIdentity(active.podUuid(),active.activationOperationUuid(),0L,active.chainSha256()));
    }
    @Test void committedNewPreparationActivationV5PinsFourDistinctIdentitiesAndKeepsV4Framing() throws Exception {
        var r=request();
        var create=UUID.randomUUID();var preparation=UUID.randomUUID();var activation=UUID.randomUUID();var pod=UUID.randomUUID();
        var active=new ActiveChainIdentity(pod,activation,14L,"78".repeat(32));
        var value=new RequestData(r.operationUuid(),r.failedBoot(),r.bootstrapOperationUuid(),r.serverUuid(),
            r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),r.recipeSha256(),
            r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),r.sourceArchiveSha256(),
            r.crashEvidenceSha256(),5,COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE,
            "34".repeat(32),java.util.Optional.of(create),java.util.Optional.of(preparation),java.util.Optional.of(active));
        byte[] bytes=DbFirstStartupRetirementCodec.encodeRequest(value);
        assertEquals(5,ByteBuffer.wrap(bytes).getInt(8));
        assertEquals(value,DbFirstStartupRetirementCodec.decodeRequest(bytes));
        assertArrayEquals(bytes,DbFirstStartupRetirementCodec.encodeRequest(DbFirstStartupRetirementCodec.decodeRequest(bytes)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),4,
            COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE,"34".repeat(32),
            java.util.Optional.of(create),java.util.Optional.of(preparation),java.util.Optional.of(active)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),5,
            DbFirstStartupRetirementCodec.COMMITTED_ACTIVE_POD_STOP_SEALING_FAILURE,"34".repeat(32),
            java.util.Optional.of(create),java.util.Optional.of(preparation),java.util.Optional.of(active)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),5,
            COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE,"34".repeat(32),
            java.util.Optional.of(create),java.util.Optional.of(create),java.util.Optional.of(active)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),5,
            COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE,"34".repeat(32),
            java.util.Optional.of(create),java.util.Optional.of(preparation),
            java.util.Optional.of(new ActiveChainIdentity(pod,create,14L,"78".repeat(32)))));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),5,
            COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE,"34".repeat(32),
            java.util.Optional.of(create),java.util.Optional.of(preparation),
            java.util.Optional.of(new ActiveChainIdentity(create,activation,14L,"78".repeat(32)))));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),5,
            COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE,"34".repeat(32),
            java.util.Optional.empty(),java.util.Optional.of(preparation),java.util.Optional.of(active)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),5,
            COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE,"34".repeat(32),
            java.util.Optional.of(create),java.util.Optional.of(preparation)));
    }
    @Test void committedPreparationV3PreservesBothDistinctOriginalIdentities() throws Exception {
        var r=request(); var create=UUID.randomUUID(); var preparation=UUID.randomUUID();
        var v3=new RequestData(r.operationUuid(),r.failedBoot(),r.bootstrapOperationUuid(),r.serverUuid(),
            r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),r.recipeSha256(),
            r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),r.sourceArchiveSha256(),
            r.crashEvidenceSha256(),3,COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE,"34".repeat(32),
            java.util.Optional.of(create),java.util.Optional.of(preparation));
        byte[] bytes=DbFirstStartupRetirementCodec.encodeRequest(v3);
        assertEquals(3,ByteBuffer.wrap(bytes).getInt(8));
        assertEquals(v3,DbFirstStartupRetirementCodec.decodeRequest(bytes));
        assertEquals(create,v3.failedCreateOperationUuid().orElseThrow());
        assertEquals(preparation,v3.preparationOperationUuid().orElseThrow());
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),2,
            COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE,"34".repeat(32)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),3,
            COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE,"34".repeat(32),
            java.util.Optional.of(create),java.util.Optional.of(create)));
    }
    private static final String SHA = "12".repeat(32);
    private static final UUID OLD = UUID.fromString("80132ac7-aad2-4fe8-8d72-6551cdebf101");
    private static final UUID CURRENT = UUID.fromString("80132ac7-aad2-4fe8-8d72-6551cdebf102");
    private static OwnerData owner(UUID boot) { return new OwnerData(boot, 1, 1234, 134383853563132270L, SHA); }
    private static WitnessRef witness() {
        return new WitnessRef("FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000001", SHA, 128);
    }
    private static RequestData request() {
        return new RequestData(UUID.randomUUID(), OLD, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            UUID.randomUUID(), "fp_test", SHA, SHA, SHA, SHA, SHA, SHA, SHA, SHA);
    }
    private static IncidentData incident() {
        return new IncidentData(request(), SHA, owner(OLD), owner(CURRENT), 2L, witness(), witness(), witness());
    }
    private static IntentData intent() {
        return new IntentData(incident(), SHA, owner(CURRENT), "fpb1:" + "A".repeat(43), 42,
            UUID.randomUUID(), LocalDateTime.of(2026, 11, 5, 21, 0), witness(), witness());
    }
    @Test void actualCodecsRoundTripAllIndependentRecordsAndBothOutcomes() throws Exception {
        var request = request();
        assertEquals(request, DbFirstStartupRetirementCodec.decodeRequest(DbFirstStartupRetirementCodec.encodeRequest(request)));
        var incident = incident();
        assertEquals(incident, DbFirstStartupRetirementCodec.decodeIncident(DbFirstStartupRetirementCodec.encodeIncident(incident)));
        var intent = intent();
        assertEquals(intent, DbFirstStartupRetirementCodec.decodeIntent(DbFirstStartupRetirementCodec.encodeIntent(intent)));
        for (boolean applied : new boolean[]{true, false}) {
            var settled = new SettlementData(intent, SHA, owner(CURRENT), intent.barrier(), 43,
                UUID.randomUUID(), intent.databaseNow().plusSeconds(1), witness(), applied);
            assertEquals(settled, DbFirstStartupRetirementCodec.decodeSettlement(DbFirstStartupRetirementCodec.encodeSettlement(settled)));
        }
    }
    @Test void refusesCorruptionTruncationTrailingBytesOversizeAndOtherDomains() throws Exception {
        byte[] request = DbFirstStartupRetirementCodec.encodeRequest(request());
        byte[] changed = request.clone(); changed[25] ^= 1;
        assertThrows(IOException.class, () -> DbFirstStartupRetirementCodec.decodeRequest(changed));
        assertThrows(IOException.class, () -> DbFirstStartupRetirementCodec.decodeRequest(Arrays.copyOf(request, request.length - 1)));
        assertThrows(IOException.class, () -> DbFirstStartupRetirementCodec.decodeRequest(Arrays.copyOf(request, request.length + 1)));
        assertThrows(IOException.class, () -> DbFirstStartupRetirementCodec.decodeRequest(new byte[4097]));
        assertThrows(IOException.class, () -> DbFirstStartupRetirementCodec.decodeIncident(request));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(DbFirstStartupRetirementCodec.encodeIntent(intent())));
    }
    @Test void neverAcceptsOldProcessAsExecutorOrDifferentSourceAsIncident() {
        var incident = incident();
        assertThrows(IllegalArgumentException.class, () -> new IntentData(incident, SHA, owner(OLD),
            "fpb1:" + "A".repeat(43), 42, CURRENT, LocalDateTime.of(2026, 11, 5, 21, 0), witness(), witness()));
        var foreign = new OwnerData(CURRENT, 1, 1234, 134383853563132270L, "34".repeat(32));
        assertThrows(IllegalArgumentException.class, () -> new IncidentData(request(), SHA, owner(OLD), foreign,
            2L, witness(), witness(), witness()));
    }
    @Test void preservesLegacyBytesAndLinksV2OnlyToExactPreviousAppliedReceipt() throws Exception {
        var root = intent();
        byte[] legacy = DbFirstStartupRetirementCodec.encodeIntent(root);
        assertEquals(1, ByteBuffer.wrap(legacy).getInt(8));
        assertArrayEquals(legacy, DbFirstStartupRetirementCodec.encodeIntent(DbFirstStartupRetirementCodec.decodeIntent(legacy)));
        var r = root.incident().request();
        String predecessor = "34".repeat(32);
        var nextRequest = new RequestData(UUID.randomUUID(), CURRENT, r.bootstrapOperationUuid(), r.serverUuid(),
            r.bindingUuid(), r.provisioningUuid(), r.schema(), r.configurationSha256(), r.recipeSha256(),
            r.sourceSha256(), r.ownerSha256(), r.completeSha256(), r.failedArtifactSha256(), r.sourceArchiveSha256(),
            r.crashEvidenceSha256(), 2, "FORGE_EARLY_MISSING_SEC_PERMISSIONS_BEFORE_RUNTIME", predecessor);
        var nextOwner = owner(UUID.fromString("80132ac7-aad2-4fe8-8d72-6551cdebf103"));
        var previous = new WitnessRef(witness().fileId(), predecessor, 128);
        var incident = new IncidentData(nextRequest, SHA, owner(CURRENT), nextOwner, 3L,
            witness(), witness(), witness(), previous);
        var next = new IntentData(incident, SHA, nextOwner, root.barrier(), 44,
            UUID.randomUUID(), root.databaseNow().plusMinutes(1), witness(), witness());
        byte[] encoded = DbFirstStartupRetirementCodec.encodeIntent(next);
        assertEquals(2, ByteBuffer.wrap(encoded).getInt(8));
        assertEquals(next, DbFirstStartupRetirementCodec.decodeIntent(encoded));
        assertEquals(root, DbFirstStartupRetirementCodec.successor(List.of(next, root), SHA).orElseThrow());
        assertEquals(next, DbFirstStartupRetirementCodec.successor(List.of(next), predecessor).orElseThrow());
        assertTrue(DbFirstStartupRetirementCodec.successor(List.of(next), "56".repeat(32)).isEmpty());
        DbFirstStartupRetirementCodec.requireUniqueIdentities(List.of(root, next));
        assertThrows(IllegalArgumentException.class, () -> DbFirstStartupRetirementCodec.successor(List.of(root, root), SHA));
        assertThrows(IllegalArgumentException.class, () -> DbFirstStartupRetirementCodec.requireUniqueIdentities(List.of(next, next)));
        assertThrows(IllegalArgumentException.class, () -> new IncidentData(nextRequest, SHA, owner(CURRENT), nextOwner,
            3L, witness(), witness(), witness(), witness()));
    }
    @Test void postServiceAutosaveIncidentHasItsOwnExplicitKindAndCannotBecomeLegacyPreService() throws Exception {
        var r=request();String predecessor="34".repeat(32);
        var post=new RequestData(r.operationUuid(),r.failedBoot(),r.bootstrapOperationUuid(),r.serverUuid(),
            r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),r.recipeSha256(),
            r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),r.sourceArchiveSha256(),
            r.crashEvidenceSha256(),2,POST_SERVICE_AUTOSAVE_CRASH_UNCHANGED_BUSINESS_DATA,predecessor);
        assertEquals(post,DbFirstStartupRetirementCodec.decodeRequest(DbFirstStartupRetirementCodec.encodeRequest(post)));
        assertEquals(POST_SERVICE_AUTOSAVE_CRASH_UNCHANGED_BUSINESS_DATA,post.failureKind());
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),1,
            POST_SERVICE_AUTOSAVE_CRASH_UNCHANGED_BUSINESS_DATA,r.completeSha256()));
    }
    @Test void mainReadPendingCrashKeepsItsOwnKindAndChainedPredecessor() throws Exception {
        var r=request();
        var value=new RequestData(r.operationUuid(),r.failedBoot(),r.bootstrapOperationUuid(),r.serverUuid(),
            r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),r.recipeSha256(),
            r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),r.sourceArchiveSha256(),
            r.crashEvidenceSha256(),2,"POST_SERVICE_MAIN_READ_PENDING_CRASH_UNCHANGED_BUSINESS_DATA","34".repeat(32));
        assertEquals(value,DbFirstStartupRetirementCodec.decodeRequest(DbFirstStartupRetirementCodec.encodeRequest(value)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),r.bootstrapOperationUuid(),r.serverUuid(),
            r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),
            r.completeSha256(),r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),1,value.failureKind(),r.completeSha256()));
    }

    @Test void bootCensusClaimFailureIsFormat2AndCannotUseLegacyFormat1() throws Exception {
        var r=request();
        var value=new RequestData(r.operationUuid(),r.failedBoot(),r.bootstrapOperationUuid(),r.serverUuid(),
            r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),r.recipeSha256(),
            r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),r.sourceArchiveSha256(),
            r.crashEvidenceSha256(),2,BOOT_CENSUS_CLAIM_FAILURE_UNCHANGED_BUSINESS_DATA,"34".repeat(32));
        byte[] bytes=DbFirstStartupRetirementCodec.encodeRequest(value);
        assertEquals(2,ByteBuffer.wrap(bytes).getInt(8));
        assertEquals(value,DbFirstStartupRetirementCodec.decodeRequest(bytes));
        assertArrayEquals(bytes,DbFirstStartupRetirementCodec.encodeRequest(DbFirstStartupRetirementCodec.decodeRequest(bytes)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),1,
            BOOT_CENSUS_CLAIM_FAILURE_UNCHANGED_BUSINESS_DATA,r.completeSha256()));
    }

    @Test void bootRecoveryPublicationFailureIsFormat2AndCannotUseLegacyFormat1() throws Exception {
        var r=request();
        var value=new RequestData(r.operationUuid(),r.failedBoot(),r.bootstrapOperationUuid(),r.serverUuid(),
            r.bindingUuid(),r.provisioningUuid(),r.schema(),r.configurationSha256(),r.recipeSha256(),
            r.sourceSha256(),r.ownerSha256(),r.completeSha256(),r.failedArtifactSha256(),r.sourceArchiveSha256(),
            r.crashEvidenceSha256(),2,BOOT_RECOVERY_PUBLICATION_FAILURE_UNCHANGED_BUSINESS_DATA,"34".repeat(32));
        byte[] bytes=DbFirstStartupRetirementCodec.encodeRequest(value);
        assertEquals(2,ByteBuffer.wrap(bytes).getInt(8));
        assertEquals(BOOT_RECOVERY_PUBLICATION_FAILURE_UNCHANGED_BUSINESS_DATA,value.failureKind());
        assertEquals(value,DbFirstStartupRetirementCodec.decodeRequest(bytes));
        assertArrayEquals(bytes,DbFirstStartupRetirementCodec.encodeRequest(DbFirstStartupRetirementCodec.decodeRequest(bytes)));
        assertThrows(IllegalArgumentException.class,()->new RequestData(r.operationUuid(),r.failedBoot(),
            r.bootstrapOperationUuid(),r.serverUuid(),r.bindingUuid(),r.provisioningUuid(),r.schema(),
            r.configurationSha256(),r.recipeSha256(),r.sourceSha256(),r.ownerSha256(),r.completeSha256(),
            r.failedArtifactSha256(),r.sourceArchiveSha256(),r.crashEvidenceSha256(),1,
            BOOT_RECOVERY_PUBLICATION_FAILURE_UNCHANGED_BUSINESS_DATA,r.completeSha256()));
    }

}
