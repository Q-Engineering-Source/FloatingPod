package com.wi.floatingpod.server;

import java.io.IOException;
import java.nio.file.Path;
import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.floatingpod.storage.mysql.MySqlFormat3ReadBudget;
import com.wi.floatingpod.storage.mysql.MySqlDeploymentReceiptFull26RestoreOwnerTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** DATA边界测试及显式隔离child的真实schema5 native盘点；不签SQL或安装许可。 */
public class WindowsB1V6WitnessRecordStoreTest {
    /** 真实schema5 source的进程寿命fixture；不提供普通close，也不签SQL或terminal许可。 */
    public static final class NativeSourceFixture {
        // 原child强持有完整或partial责任直到实际进程退出；不因helper返回或失败而丢对象。
        private static NativeSourceFixture retained;
        private final MySqlFormat3ReadBudget root;
        public WindowsMarkerIo io;
        public WindowsWorldProvisioner provisioner;
        public WindowsWorldProvisioner.DeploymentReceiptWorld world;
        public WindowsB1SourceAnchorStore anchors;
        public WindowsB1SourceAnchorStore.ControlledAnchorLocation location;
        public WindowsB1SourceAnchorStore.DurableB1SourceAnchor anchor;
        public WindowsWorldProvisioner.DeploymentReceiptIoScope source;
        public WindowsB1Full26WitnessStore artifacts;
        public WindowsB1V6WitnessRecordStore records;
        public Throwable failure;
        private NativeSourceFixture(MySqlFormat3ReadBudget root) {
            this.root=java.util.Objects.requireNonNull(root);
        }
        private void initialize(Path metadataPath,MySqlRuntimeMetadata.DeploymentReceiptStatic metadata,
                Path worldPath,WindowsWorldProvisionReceiptDataCodec.PlanData plan,
                WindowsWorldProvisioner.BootIdentity boot) throws IOException {
            try {
                io=new WindowsMarkerIo();
                provisioner=new WindowsWorldProvisioner(io);
                anchors=new WindowsB1SourceAnchorStore(io);
                var configuration=WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(metadata);
                location=anchors.captureLocation(metadataPath,configuration,plan.creationOperationUuid());
                world=provisioner.createFreshDeploymentReceipt(worldPath,plan,boot);
                anchor=anchors.publishCreatedDeploymentReceiptSource(location,world);
                source=provisioner.bindDeploymentReceiptSource(world,anchors,location,anchor);
                artifacts=new WindowsB1Full26WitnessStore(source,root);
                records=new WindowsB1V6WitnessRecordStore(source,artifacts,root);
            } catch(IOException|RuntimeException|Error cause) {
                failure=cause;
                throw cause;
            }
        }
        public static synchronized NativeSourceFixture create(Path metadataPath,
                MySqlRuntimeMetadata.DeploymentReceiptStatic metadata,Path worldPath,
                WindowsWorldProvisionReceiptDataCodec.PlanData plan,WindowsWorldProvisioner.BootIdentity boot,
                MySqlFormat3ReadBudget root) throws IOException {
            if(retained!=null) throw new IllegalStateException("one original native source per actual fixture child",retained.failure);
            retained=new NativeSourceFixture(root);
            retained.initialize(metadataPath,metadata,worldPath,plan,boot);
            return retained;
        }
    }
    @Test @Tag("b1-schema5-native")
    void actualSchema5SourceCustodyProducesPreGenesisInventory(@TempDir Path directory) throws Exception {
        try(var child=RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.schema5.child.java"),
                System.getProperty("fp.b1.schema5.child.classpath"),NativeChild.class,directory)) {
            assertTrue(child.finish("fresh-inventory").contains("SCHEMA5_PRE_GENESIS=true"));
        }
    }

    @Test @Tag("b1-schema5-native")
    void actualSchema5Full26BodyAndGenesisIntentReceiveNativeAcknowledgements(@TempDir Path directory) throws Exception {
        try(var child=RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.schema5.child.java"),
                System.getProperty("fp.b1.schema5.child.classpath"),NativeChild.class,directory)) {
            assertTrue(child.finish("fresh-intent").contains("SCHEMA5_GENESIS_INTENT_ACK=true"));
        }
    }
    @Test @Tag("b1-schema5-native")
    void actualSchema5ReservationEntryFailuresRetainOriginalCauseAndFees(@TempDir Path directory) throws Exception {
        try(var child=RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.schema5.child.java"),
                System.getProperty("fp.b1.schema5.child.classpath"),NativeChild.class,directory)) {
            assertTrue(child.finish("reservation-entry-failures").contains("SCHEMA5_RESERVATION_FAILURES=true"));
        }
    }
    @Test @Tag("b1-schema5-native")
    void actualSchema5HealthyReservationBindsOriginalTokenAndProtectsPublicationPeak(@TempDir Path directory) throws Exception {
        try(var child=RetainedNativeFixtureSupervisor.start(System.getProperty("fp.b1.schema5.child.java"),
                System.getProperty("fp.b1.schema5.child.classpath"),NativeChild.class,directory)) {
            assertTrue(child.finish("healthy-reservation-prefix").contains("SCHEMA5_HEALTHY_RESERVATION_PREFIX=true"));
        }
    }
    public static final class NativeChild {
        private static final MySqlDeploymentReceiptFull26RestoreOwnerTest RUNNER=new MySqlDeploymentReceiptFull26RestoreOwnerTest();
        public static void main(String[] args) throws Exception {
            if(args.length!=4 || !args[0].equals("retained-supervisor"))
                throw new IllegalArgumentException("exact retained native supervisor arguments required");
            var directory=Path.of(args[1]);
            RetainedNativeFixtureSupervisor.signal(directory,".retained-supervisor-ready");
            String mode=RetainedNativeFixtureSupervisor.nextMode();
            switch(mode) {
                case "fresh-inventory" -> {
                    RUNNER.exerciseFreshNativeStoreInventory(directory);
                    System.out.println("SCHEMA5_PRE_GENESIS=true");
                }
                case "fresh-intent" -> {
                    RUNNER.exerciseFreshNativeIntent(directory);
                    System.out.println("SCHEMA5_GENESIS_INTENT_ACK=true");
                }
                case "reservation-entry-failures" -> {
                    RUNNER.exerciseNativeReservationEntryFailures(directory);
                    System.out.println("SCHEMA5_RESERVATION_FAILURES=true");
                }
                case "healthy-reservation-prefix" -> {
                    RUNNER.exerciseNativeHealthyConfirmationReservation(directory);
                    System.out.println("SCHEMA5_HEALTHY_RESERVATION_PREFIX=true");
                }
                default -> throw new IllegalArgumentException("unsupported schema5 native fixture mode");
            }
        }
    }
    private static final UUID ATTEMPT=UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c799");
    private static final byte[] SHA=HexFormat.of().parseHex("ab".repeat(32));
    private static final long MAXIMUM=1024L*1024L;

    @Test void acceptsExactCanonicalSchema5InventoryNamesAndMeasuredBounds()throws Exception {
        WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(B1ResumeRecordV6.SLOT_0,1L,0x80,MAXIMUM);
        WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(B1ResumeRecordV6.SLOT_1,B1ResumeRecordV6.MAX_RECORD_BYTES,0x20,MAXIMUM);
        WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(WindowsB1V6WitnessRecordStore.historyName(12L,SHA),
            B1ResumeRecordV6.MAX_RECORD_BYTES,0x80,MAXIMUM);
        WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(B1Full26WitnessReference.basename(ATTEMPT,SHA),MAXIMUM,0x80,MAXIMUM);
    }

    @Test void rejectsTemporaryLegacyUnknownAndNoncanonicalInventoryResponsibilities()throws Exception {
        String body=B1Full26WitnessReference.basename(ATTEMPT,SHA);
        String history=WindowsB1V6WitnessRecordStore.historyName(12L,SHA);
        for(String name:new String[]{B1ResumeRecordV6.TEMPORARY,body+".tmp",".floatingpod-b1-resume-v5.0",
                ".floatingpod-b1-full20-v1-unknown.bin",".floatingpod-b1-unknown.bin",
                history.replace("history-12-","history-012-"),history.replace("ab","AB"),
                body.replace(ATTEMPT.toString(),ATTEMPT.toString().toUpperCase(java.util.Locale.ROOT)),
                body.replace(ATTEMPT.toString(),new UUID(0L,0L).toString()),
                ".floatingpod-b1-resume-v6-history--1-"+"ab".repeat(32)+".bin"}){
            assertThrows(IOException.class,()->WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(name,1L,0x80,MAXIMUM),name);
        }
    }

    @Test void rejectsDirectoryReparseEmptyAndOversizedPhysicalEntriesBeforeOpen()throws Exception {
        String body=B1Full26WitnessReference.basename(ATTEMPT,SHA);
        for(int attributes:new int[]{0x10,0x400,0x410})
            assertThrows(IOException.class,()->WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(body,1L,attributes,MAXIMUM));
        for(long length:new long[]{0L,-1L,MAXIMUM+1L})
            assertThrows(IOException.class,()->WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(body,length,0x80,MAXIMUM));
        assertThrows(IOException.class,()->WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(body,
            (long)Integer.MAX_VALUE+1L,0x80,Long.MAX_VALUE));
        assertThrows(IOException.class,()->WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(B1ResumeRecordV6.SLOT_0,
            (long)B1ResumeRecordV6.MAX_RECORD_BYTES+1L,0x80,MAXIMUM));
        assertThrows(IllegalArgumentException.class,()->WindowsB1V6WitnessRecordStore.requireValidDiscoveryEntry(body,1L,0x80,0L));
    }
}
