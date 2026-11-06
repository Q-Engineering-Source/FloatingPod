package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 通过实际 encoder/decoder 验证 FirstBinding6 owner 与 phase DATA wire format。 */
class DbFirstBootstrapRecordCodecTest {
    private static final String SOURCE_SHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final String OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
    private static final String ID_1 = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000001";
    private static final String ID_2 = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000002";
    private static final String ID_3 = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000003";
    private static final String ID_4 = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000004";
    private static final String ID_5 = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000005";
    private static final String NAMESPACE = "fpb1:" + "A".repeat(43);
    private static final UUID BOOT = UUID.fromString("74b00f82-f777-4b09-8b21-7fbc11c68317");
    private static final UUID OPERATION = UUID.fromString("e61da3d6-e2dc-4ae0-a702-20fd3877c143");
    private static final UUID CONNECTION_GENERATION = UUID.fromString("226140b4-3fac-4dce-bb0a-fca215cbdb22");
    private static final LocalDateTime DATABASE_NOW = LocalDateTime.of(2026, 11, 5, 12, 34, 56, 123456000);

    private static DbFirstBootstrapRecordCodec.OwnerData owner() {
        return new DbFirstBootstrapRecordCodec.OwnerData(BOOT, 3, 1234, 132537600000000000L, SOURCE_SHA);
    }

    private static DbFirstBootstrapRecordCodec.WitnessRef before() {
        return new DbFirstBootstrapRecordCodec.WitnessRef(ID_1, SOURCE_SHA, 8192);
    }

    private static DbFirstBootstrapRecordCodec.WitnessRef target() {
        return new DbFirstBootstrapRecordCodec.WitnessRef(ID_2, OTHER_SHA, 16384);
    }

    private static DbFirstBootstrapRecordCodec.MarkerData marker(boolean oldMarkerPresent) {
        return new DbFirstBootstrapRecordCodec.MarkerData(ID_3, OTHER_SHA, oldMarkerPresent,
            oldMarkerPresent ? ID_4 : null);
    }

    private static DbFirstBootstrapRecordCodec.CutoverIntentData cutoverIntent() {
        return new DbFirstBootstrapRecordCodec.CutoverIntentData(SOURCE_SHA, owner(), OPERATION,
            UUID.fromString("9916a2b5-b4d7-4e38-ae16-b32d35b94f81"));
    }

    private static DbFirstBootstrapRecordCodec.SettlementData settlement(
            DbFirstBootstrapRecordCodec.Outcome outcome) throws Exception {
        var intent = record(DbFirstBootstrapRecordCodec.Phase.CERTIFY, DbFirstBootstrapRecordCodec.Kind.INTENT);
        byte[] intentBytes = DbFirstBootstrapRecordCodec.encodeRecord(intent);
        var observer = new DbFirstBootstrapRecordCodec.OwnerData(
            UUID.fromString("81020c17-dd8a-4acd-a768-6689921b3eb0"), 4, 2345, 133537600000000000L, SOURCE_SHA);
        return new DbFirstBootstrapRecordCodec.SettlementData(intent,
            java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(intentBytes)),
            observer, NAMESPACE, 43, DATABASE_NOW.plusSeconds(1),
            UUID.fromString("5126aed0-18b1-4fd4-8cbb-719ffcf3928b"),
            new DbFirstBootstrapRecordCodec.WitnessRef(ID_5, OTHER_SHA, 32768), outcome);
    }

    private static DbFirstBootstrapRecordCodec.RecordData record(
            DbFirstBootstrapRecordCodec.Phase phase, DbFirstBootstrapRecordCodec.Kind kind) {
        return record(phase, kind, phase == DbFirstBootstrapRecordCodec.Phase.PREPARE ? null : marker(true));
    }

    private static DbFirstBootstrapRecordCodec.RecordData record(
            DbFirstBootstrapRecordCodec.Phase phase, DbFirstBootstrapRecordCodec.Kind kind,
            DbFirstBootstrapRecordCodec.MarkerData marker) {
        return new DbFirstBootstrapRecordCodec.RecordData(phase, kind, OPERATION, SOURCE_SHA, owner(), NAMESPACE,
            42, DATABASE_NOW, CONNECTION_GENERATION, before(), target(),
            kind == DbFirstBootstrapRecordCodec.Kind.CONFIRMED ? OTHER_SHA : null,
            marker);
    }

    @Test void independentOwnerCodecRoundTripsAndRejectsPhaseDomain() throws Exception {
        var expected = owner();
        byte[] encoded = DbFirstBootstrapRecordCodec.encodeOwner(expected);
        var decoded = DbFirstBootstrapRecordCodec.decodeOwner(encoded);
        assertEquals(expected, decoded);
        assertArrayEquals(encoded, DbFirstBootstrapRecordCodec.encodeOwner(decoded));
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(body(encoded)),
            Arrays.copyOfRange(encoded, encoded.length - 32, encoded.length));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(encoded));

        byte[] badDigest = encoded.clone();
        badDigest[badDigest.length - 1] ^= 1;
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeOwner(badDigest));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeOwner(Arrays.copyOf(encoded, encoded.length - 1)));

        byte[] unknownVersion = body(encoded);
        unknownVersion[11] = 2;
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeOwner(reseal(unknownVersion)));

        byte[] oldMagic = body(encoded);
        ByteBuffer.wrap(oldMagic).putLong(0, 0x4650423152535634L);
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeOwner(reseal(oldMagic)));

        byte[] wrongDomain = body(encoded);
        wrongDomain[20] ^= 1;
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeOwner(reseal(wrongDomain)));
    }

    @Test void allPhaseKindCombinationsRoundTripWithFixedSchema6Full27Selector() throws Exception {
        for (var phase : DbFirstBootstrapRecordCodec.Phase.values()) {
            for (var kind : DbFirstBootstrapRecordCodec.Kind.values()) {
                var expected = record(phase, kind);
                byte[] encoded = DbFirstBootstrapRecordCodec.encodeRecord(expected);
                var decoded = DbFirstBootstrapRecordCodec.decodeRecord(encoded);
                assertEquals(expected, decoded);
                assertTrue(encoded.length <= DbFirstBootstrapRecordCodec.MAX_RECORD_BYTES);
                assertArrayEquals(encoded, DbFirstBootstrapRecordCodec.encodeRecord(decoded));
                assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(body(encoded)),
                    Arrays.copyOfRange(encoded, encoded.length - 32, encoded.length));
                assertEquals(6, decoded.schemaFormat());
                assertEquals("FULL27", decoded.snapshotSelector());
            }
        }
        var explicitAbsentPreviousMarker = record(DbFirstBootstrapRecordCodec.Phase.CERTIFY,
            DbFirstBootstrapRecordCodec.Kind.INTENT, marker(false));
        assertEquals(explicitAbsentPreviousMarker, DbFirstBootstrapRecordCodec.decodeRecord(
            DbFirstBootstrapRecordCodec.encodeRecord(explicitAbsentPreviousMarker)));
        var handoffWithoutMarker = record(DbFirstBootstrapRecordCodec.Phase.HANDOFF,
            DbFirstBootstrapRecordCodec.Kind.CONFIRMED, null);
        assertEquals(handoffWithoutMarker, DbFirstBootstrapRecordCodec.decodeRecord(
            DbFirstBootstrapRecordCodec.encodeRecord(handoffWithoutMarker)));
        assertThrows(IOException.class,
            () -> DbFirstBootstrapRecordCodec.decodeOwner(DbFirstBootstrapRecordCodec.encodeRecord(
                record(DbFirstBootstrapRecordCodec.Phase.PREPARE, DbFirstBootstrapRecordCodec.Kind.INTENT))));
    }

    @Test void markerCutoverIntentAndConfirmationHaveIndependentDomainsAndExactIntentReference() throws Exception {
        var intent = cutoverIntent();
        byte[] intentBytes = DbFirstBootstrapRecordCodec.encodeCutoverIntent(intent);
        assertEquals(intent, DbFirstBootstrapRecordCodec.decodeCutoverIntent(intentBytes));
        assertArrayEquals(intentBytes, DbFirstBootstrapRecordCodec.encodeCutoverIntent(
            DbFirstBootstrapRecordCodec.decodeCutoverIntent(intentBytes)));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeOwner(intentBytes));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(intentBytes));

        String intentSha = java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(intentBytes));
        var confirmation = new DbFirstBootstrapRecordCodec.CutoverConfirmedData(intent, intentSha, marker(false));
        byte[] confirmedBytes = DbFirstBootstrapRecordCodec.encodeCutoverConfirmed(confirmation);
        assertEquals(confirmation, DbFirstBootstrapRecordCodec.decodeCutoverConfirmed(confirmedBytes));
        assertArrayEquals(confirmedBytes, DbFirstBootstrapRecordCodec.encodeCutoverConfirmed(
            DbFirstBootstrapRecordCodec.decodeCutoverConfirmed(confirmedBytes)));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeCutoverIntent(confirmedBytes));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.encodeCutoverConfirmed(
            new DbFirstBootstrapRecordCodec.CutoverConfirmedData(intent, OTHER_SHA, marker(true))));

        byte[] changedIntentSha = body(confirmedBytes);
        changedIntentSha[cutoverIntentShaOffset(changedIntentSha)] ^= 1;
        assertThrows(IOException.class,
            () -> DbFirstBootstrapRecordCodec.decodeCutoverConfirmed(reseal(changedIntentSha)));
    }

    @Test void cutoverIntentRejectsWrongOwnerSourceOperationAndNonV4Binding() {
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.CutoverIntentData(OTHER_SHA, owner(), OPERATION,
                UUID.fromString("9916a2b5-b4d7-4e38-ae16-b32d35b94f81")));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.CutoverIntentData(SOURCE_SHA, owner(), new UUID(0, 0),
                UUID.fromString("9916a2b5-b4d7-4e38-ae16-b32d35b94f81")));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.CutoverIntentData(SOURCE_SHA, owner(), OPERATION,
                UUID.fromString("9916a2b5-b4d7-3e38-ae16-b32d35b94f81")));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.CutoverIntentData(SOURCE_SHA, owner(), OPERATION,
                UUID.fromString("9916a2b5-b4d7-4e38-ce16-b32d35b94f81")));
        assertThrows(NullPointerException.class,
            () -> new DbFirstBootstrapRecordCodec.CutoverConfirmedData(cutoverIntent(), SOURCE_SHA, null));
    }

    @Test void settlementsBindExactIntentToNewReadbackIdentityAndOnlyExplicitOutcomes() throws Exception {
        for (var outcome : DbFirstBootstrapRecordCodec.Outcome.values()) {
            var expected = settlement(outcome);
            byte[] encoded = DbFirstBootstrapRecordCodec.encodeSettlement(expected);
            assertEquals(expected, DbFirstBootstrapRecordCodec.decodeSettlement(encoded));
            assertArrayEquals(encoded, DbFirstBootstrapRecordCodec.encodeSettlement(
                DbFirstBootstrapRecordCodec.decodeSettlement(encoded)));
            assertTrue(encoded.length <= DbFirstBootstrapRecordCodec.MAX_RECORD_BYTES);
            assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(encoded));
        }
        assertArrayEquals(new DbFirstBootstrapRecordCodec.Outcome[]{
            DbFirstBootstrapRecordCodec.Outcome.APPLIED, DbFirstBootstrapRecordCodec.Outcome.NOT_APPLIED
        }, DbFirstBootstrapRecordCodec.Outcome.values());

        var good = settlement(DbFirstBootstrapRecordCodec.Outcome.APPLIED);
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.encodeSettlement(
            new DbFirstBootstrapRecordCodec.SettlementData(good.intent(), OTHER_SHA, good.observer(),
                good.sessionBarrierNamespace(), good.actualConnectionId(), good.databaseNow(),
                good.connectionGeneration(), good.actualFull27(), good.outcome())));

        byte[] changedIntentSha = body(DbFirstBootstrapRecordCodec.encodeSettlement(good));
        changedIntentSha[settlementIntentShaOffset(changedIntentSha)] ^= 1;
        assertThrows(IOException.class,
            () -> DbFirstBootstrapRecordCodec.decodeSettlement(reseal(changedIntentSha)));
    }

    @Test void settlementRejectsOldConfirmedKindCrossSourceAndInvalidDatabasePrecision() throws Exception {
        var confirmed = record(DbFirstBootstrapRecordCodec.Phase.CERTIFY, DbFirstBootstrapRecordCodec.Kind.CONFIRMED);
        byte[] confirmedBytes = DbFirstBootstrapRecordCodec.encodeRecord(confirmed);
        var otherSourceObserver = new DbFirstBootstrapRecordCodec.OwnerData(
            UUID.fromString("81020c17-dd8a-4acd-a768-6689921b3eb0"), 4, 2345, 133537600000000000L, OTHER_SHA);
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.SettlementData(confirmed,
                java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(confirmedBytes)),
                owner(), NAMESPACE, 43, DATABASE_NOW.plusSeconds(1),
                UUID.fromString("5126aed0-18b1-4fd4-8cbb-719ffcf3928b"),
                new DbFirstBootstrapRecordCodec.WitnessRef(ID_5, OTHER_SHA, 32768),
                DbFirstBootstrapRecordCodec.Outcome.APPLIED));

        var good = settlement(DbFirstBootstrapRecordCodec.Outcome.NOT_APPLIED);
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.SettlementData(good.intent(), good.intentSha256(), otherSourceObserver,
                good.sessionBarrierNamespace(), good.actualConnectionId(), good.databaseNow(),
                good.connectionGeneration(), good.actualFull27(), good.outcome()));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.SettlementData(good.intent(), good.intentSha256(), good.observer(),
                good.sessionBarrierNamespace(), good.actualConnectionId(), DATABASE_NOW.plusNanos(1),
                good.connectionGeneration(), good.actualFull27(), good.outcome()));
    }

    @Test void dataRecordsRejectInvalidIdentityHashesLengthsAndPhaseShape() {
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.OwnerData(new UUID(0, 0), 1, 1234, 132537600000000000L, SOURCE_SHA));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.OwnerData(BOOT, 0, 1234, 132537600000000000L, SOURCE_SHA));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.OwnerData(BOOT, 1, 0, 132537600000000000L, SOURCE_SHA));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.OwnerData(BOOT, 1, 1234, 132537600000000000L, SOURCE_SHA.toUpperCase()));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.WitnessRef("C:\\snapshot", SOURCE_SHA, 1));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.WitnessRef(ID_1, SOURCE_SHA.toUpperCase(), 1));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.WitnessRef(ID_1, SOURCE_SHA, 0));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.MarkerData(ID_3, OTHER_SHA, false, ID_4));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.MarkerData(ID_3, OTHER_SHA.toUpperCase(), false, null));

        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.RecordData(DbFirstBootstrapRecordCodec.Phase.PREPARE,
                DbFirstBootstrapRecordCodec.Kind.INTENT, OPERATION, SOURCE_SHA, owner(), NAMESPACE, 42,
                DATABASE_NOW, CONNECTION_GENERATION, before(), target(), OTHER_SHA, null));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.RecordData(DbFirstBootstrapRecordCodec.Phase.PREPARE,
                DbFirstBootstrapRecordCodec.Kind.CONFIRMED, OPERATION, SOURCE_SHA, owner(), NAMESPACE, 42,
                DATABASE_NOW, CONNECTION_GENERATION, before(), target(), null, null));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.RecordData(DbFirstBootstrapRecordCodec.Phase.CERTIFY,
                DbFirstBootstrapRecordCodec.Kind.INTENT, OPERATION, SOURCE_SHA, owner(), NAMESPACE, 42,
                DATABASE_NOW, CONNECTION_GENERATION, before(), target(), null, null));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.RecordData(DbFirstBootstrapRecordCodec.Phase.PREPARE,
                DbFirstBootstrapRecordCodec.Kind.INTENT, OPERATION, SOURCE_SHA, owner(), "wrong-namespace", 42,
                DATABASE_NOW, CONNECTION_GENERATION, before(), target(), null, null));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.RecordData(DbFirstBootstrapRecordCodec.Phase.PREPARE,
                DbFirstBootstrapRecordCodec.Kind.INTENT, OPERATION, OTHER_SHA, owner(), NAMESPACE, 42,
                DATABASE_NOW, CONNECTION_GENERATION, before(), target(), null, null));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.RecordData(DbFirstBootstrapRecordCodec.Phase.PREPARE,
                DbFirstBootstrapRecordCodec.Kind.INTENT, OPERATION, SOURCE_SHA, owner(), NAMESPACE, 42,
                LocalDateTime.of(999, 12, 31, 23, 59), CONNECTION_GENERATION, before(), target(), null, null));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstBootstrapRecordCodec.RecordData(DbFirstBootstrapRecordCodec.Phase.PREPARE,
                DbFirstBootstrapRecordCodec.Kind.INTENT, OPERATION, SOURCE_SHA, owner(), NAMESPACE, 42,
                LocalDateTime.of(2026, 11, 5, 12, 34, 56, 123456789), CONNECTION_GENERATION, before(), target(), null, null));
    }

    @Test void decodersRejectCorruptDigestTruncationOversizeUnknownVersionAndOldDomain() throws Exception {
        byte[] good = DbFirstBootstrapRecordCodec.encodeRecord(
            record(DbFirstBootstrapRecordCodec.Phase.PREPARE, DbFirstBootstrapRecordCodec.Kind.INTENT));
        byte[] badDigest = good.clone();
        badDigest[badDigest.length - 1] ^= 1;
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(badDigest));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(Arrays.copyOf(good, good.length - 1)));
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(
            new byte[DbFirstBootstrapRecordCodec.MAX_RECORD_BYTES + 1]));

        byte[] unknownVersion = body(good);
        unknownVersion[11] = 2;
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(unknownVersion)));

        byte[] oldMagic = body(good);
        ByteBuffer.wrap(oldMagic).putLong(0, 0x4650423152535634L);
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(oldMagic)));

        byte[] oldDomain = body(good);
        oldDomain[20] ^= 1;
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(oldDomain)));
    }

    @Test void decoderRejectsBadLengthSelectorEnumNoncanonicalBooleanAndTrailingFields() throws Exception {
        byte[] good = DbFirstBootstrapRecordCodec.encodeRecord(
            record(DbFirstBootstrapRecordCodec.Phase.CERTIFY, DbFirstBootstrapRecordCodec.Kind.CONFIRMED));

        byte[] badLength = body(good);
        ByteBuffer.wrap(badLength).putInt(12, ByteBuffer.wrap(badLength).getInt(12) + 1);
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(badLength)));

        byte[] badSelector = body(good);
        badSelector[selectorOffset()] = (byte) 'X';
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(badSelector)));

        byte[] badBoolean = body(good);
        int markerTag = markerTagOffset(badBoolean);
        badBoolean[markerTag] = 2;
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(badBoolean)));

        byte[] completeBody = body(good);
        byte[] trailing = Arrays.copyOf(completeBody, completeBody.length + 1);
        trailing[completeBody.length] = 0;
        ByteBuffer.wrap(trailing).putInt(12, ByteBuffer.wrap(trailing).getInt(12) + 1);
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(trailing)));

        byte[] unknownPhase = body(good);
        unknownPhase[phaseOffset()] = (byte) 'X';
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(unknownPhase)));

        byte[] wrongDbYear = body(good);
        ByteBuffer.wrap(wrongDbYear).putInt(databaseNowOffset(wrongDbYear), 999);
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(wrongDbYear)));

        byte[] wrongDbPrecision = body(good);
        ByteBuffer.wrap(wrongDbPrecision).putInt(databaseNowOffset(wrongDbPrecision) + 9, 123456789);
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(wrongDbPrecision)));
    }

    @Test void decoderRejectsMalformedUtf8AndOversizedTextFields() throws Exception {
        byte[] good = DbFirstBootstrapRecordCodec.encodeRecord(
            record(DbFirstBootstrapRecordCodec.Phase.PREPARE, DbFirstBootstrapRecordCodec.Kind.INTENT));

        byte[] malformedUtf8 = body(good);
        malformedUtf8[namespaceOffset()] = (byte) 0xc3;
        IOException utf8Failure = assertThrows(IOException.class,
            () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(malformedUtf8)));
        assertInstanceOf(CharacterCodingException.class, utf8Failure.getCause());

        byte[] oversizedText = body(good);
        ByteBuffer.wrap(oversizedText).putInt(16, 4097);
        assertThrows(IOException.class, () -> DbFirstBootstrapRecordCodec.decodeRecord(reseal(oversizedText)));
    }

    private static int selectorOffset() throws IOException {
        byte[] payload = DbFirstBootstrapRecordCodec.encodeRecord(
            record(DbFirstBootstrapRecordCodec.Phase.PREPARE, DbFirstBootstrapRecordCodec.Kind.INTENT));
        try (var in = new DataInputStream(new ByteArrayInputStream(body(payload)))) {
            in.skipNBytes(16);
            readText(in);
            in.readInt();
            int length = in.readInt();
            if (length <= 0 || length > in.available()) throw new IOException("Invalid test selector field");
            return body(payload).length - in.available();
        }
    }

    private static int phaseOffset() throws IOException {
        byte[] payload = DbFirstBootstrapRecordCodec.encodeRecord(
            record(DbFirstBootstrapRecordCodec.Phase.PREPARE, DbFirstBootstrapRecordCodec.Kind.INTENT));
        try (var in = new DataInputStream(new ByteArrayInputStream(body(payload)))) {
            in.skipNBytes(16);
            readText(in);
            in.readInt();
            readText(in);
            int length = in.readInt();
            if (length <= 0 || length > in.available()) throw new IOException("Invalid test phase field");
            return body(payload).length - in.available();
        }
    }

    private static int namespaceOffset() throws IOException {
        byte[] payload = DbFirstBootstrapRecordCodec.encodeRecord(
            record(DbFirstBootstrapRecordCodec.Phase.PREPARE, DbFirstBootstrapRecordCodec.Kind.INTENT));
        try (var in = new DataInputStream(new ByteArrayInputStream(body(payload)))) {
            in.skipNBytes(16);
            readText(in);
            in.readInt();
            readText(in);
            readText(in);
            readText(in);
            in.skipNBytes(16 + 32);
            in.skipNBytes(16 + 8 + 8 + 8 + 32);
            return body(payload).length - in.available() + 4;
        }
    }

    private static int databaseNowOffset(byte[] prefix) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(prefix))) {
            in.skipNBytes(16);
            readText(in);
            in.readInt();
            readText(in);
            readText(in);
            readText(in);
            in.skipNBytes(16 + 32);
            in.skipNBytes(16 + 8 + 8 + 8 + 32);
            readText(in);
            in.skipNBytes(8);
            return prefix.length - in.available();
        }
    }

    private static int markerTagOffset(byte[] prefix) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(prefix))) {
            in.skipNBytes(16);
            readText(in);
            in.readInt();
            readText(in);
            readText(in);
            readText(in);
            in.skipNBytes(16 + 32);
            in.skipNBytes(16 + 8 + 8 + 8 + 32);
            readText(in);
            in.skipNBytes(8 + 13 + 16);
            skipWitness(in);
            skipWitness(in);
            if (in.readUnsignedByte() != 1) throw new IOException("Test fixture requires confirmed intent");
            in.skipNBytes(32);
            return prefix.length - in.available();
        }
    }

    private static int cutoverIntentShaOffset(byte[] prefix) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(prefix))) {
            in.skipNBytes(16);
            readText(in);
            int length = in.readInt();
            if (length <= 0 || length > in.available()) throw new IOException("Invalid test cutover intent length");
            in.skipNBytes(length);
            return prefix.length - in.available();
        }
    }

    private static int settlementIntentShaOffset(byte[] prefix) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(prefix))) {
            in.skipNBytes(16);
            readText(in);
            int length = in.readInt();
            if (length <= 0 || length > in.available()) throw new IOException("Invalid test settlement intent length");
            in.skipNBytes(length);
            return prefix.length - in.available();
        }
    }

    private static void skipWitness(DataInputStream in) throws IOException {
        readText(in);
        in.skipNBytes(32 + 8);
    }

    private static String readText(DataInputStream in) throws IOException {
        int length = in.readInt();
        return new String(in.readNBytes(length), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] body(byte[] complete) {
        return Arrays.copyOf(complete, complete.length - 32);
    }

    private static byte[] reseal(byte[] prefix) throws Exception {
        byte[] complete = Arrays.copyOf(prefix, prefix.length + 32);
        System.arraycopy(MessageDigest.getInstance("SHA-256").digest(prefix), 0, complete, prefix.length, 32);
        return complete;
    }
}
