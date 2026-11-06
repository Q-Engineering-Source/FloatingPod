package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 通过真实 encoder/decoder 验证闭合的 FirstBinding6 来源 DATA wire format。 */
class DbFirstWorldAdoptionReceiptCodecTest {
    private static final String FILE_ID_1 = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000001";
    private static final String FILE_ID_2 = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000002";
    private static final String FILE_ID_3 = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000003";
    private static final String FILE_ID_CUSTODY = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000004";
    private static final String FILE_ID_MARKER = "FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000005";
    private static final String LOWER_SHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final UUID MARKER_V4 = UUID.fromString("a934a0b3-33d5-4eab-8b17-e61150abbf00");
    private static final UUID BOOT_ID = UUID.fromString("74b00f82-f777-4b09-8b21-7fbc11c68317");

    private static DbFirstWorldAdoptionPlanData plan() {
        return plan(UUID.fromString("55555555-5555-4555-8555-555555555555"));
    }

    private static DbFirstWorldAdoptionPlanData plan(UUID bindingUuid) {
        return new DbFirstWorldAdoptionPlanData(
            UUID.fromString("11111111-1111-4111-8111-111111111111"),
            UUID.fromString("22222222-2222-4222-8222-222222222222"),
            UUID.fromString("33333333-3333-4333-8333-333333333333"),
            UUID.fromString("44444444-4444-4444-8444-444444444444"),
            bindingUuid,
            2,
            UUID.fromString("66666666-6666-4666-8666-666666666666"),
            "floatingpod_rbss_v1",
            6,
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            UUID.fromString("77777777-7777-4777-8777-777777777777"));
    }

    private static DbFirstWorldAdoptionReceiptCodec.SourceFacts facts(
            DbFirstWorldAdoptionReceiptCodec.PreviousMarker marker) {
        return facts(plan(), marker);
    }

    private static DbFirstWorldAdoptionReceiptCodec.SourceFacts facts(DbFirstWorldAdoptionPlanData plan,
            DbFirstWorldAdoptionReceiptCodec.PreviousMarker marker) {
        return new DbFirstWorldAdoptionReceiptCodec.SourceFacts(plan, FILE_ID_1, FILE_ID_2, FILE_ID_3, FILE_ID_CUSTODY,
            marker, 1234, 132537600000000000L,
            new WindowsWorldProvisioner.BootIdentity(BOOT_ID, 7), LOWER_SHA,
            "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210");
    }

    private static DbFirstWorldAdoptionReceiptCodec.PreviousMarker absent() {
        return new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(false, null, 0, null);
    }

    private static DbFirstWorldAdoptionReceiptCodec.PreviousMarker present(int format, UUID markerUuid) {
        return new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(true, FILE_ID_MARKER, format, markerUuid);
    }

    @Test void actualCodecRoundTripsExplicitAbsentAndPresentMarkerFactsCanonically() throws Exception {
        var absent = facts(absent());
        byte[] absentBytes = DbFirstWorldAdoptionReceiptCodec.encode(absent);
        assertEquals(absent, DbFirstWorldAdoptionReceiptCodec.decode(absentBytes));
        assertArrayEquals(absentBytes, DbFirstWorldAdoptionReceiptCodec.encode(
            DbFirstWorldAdoptionReceiptCodec.decode(absentBytes)));
        assertEquals(DbFirstWorldAdoptionReceiptCodec.SOURCE_TOKEN, absent.sourceToken());
        assertTrue(absent.oldMarker().markerBytes().isEmpty());

        var marker = present(2, MARKER_V4);
        var withMarker = facts(marker);
        byte[] markerBytes = DbFirstWorldAdoptionReceiptCodec.encode(withMarker);
        assertEquals(withMarker, DbFirstWorldAdoptionReceiptCodec.decode(markerBytes));
        assertArrayEquals(markerBytes, DbFirstWorldAdoptionReceiptCodec.encode(
            DbFirstWorldAdoptionReceiptCodec.decode(markerBytes)));
        byte[] first = marker.markerBytes().orElseThrow();
        assertEquals(24, first.length);
        assertEquals(0x46505742, ByteBuffer.wrap(first).getInt());
        first[0] = 0;
        assertEquals(0x46505742, ByteBuffer.wrap(marker.markerBytes().orElseThrow()).getInt());
        assertFalse(Arrays.equals(absentBytes, markerBytes));

        var legacyMarker = present(1, UUID.fromString("74b00f82-f777-4b09-8b21-7fbc11c68317"));
        assertEquals(1, DbFirstWorldAdoptionReceiptCodec.decode(
            DbFirstWorldAdoptionReceiptCodec.encode(facts(legacyMarker))).oldMarker().format());
    }

    @Test void dataRecordsRejectAmbiguousMarkerAndNoncanonicalSourceFacts() {
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(false, FILE_ID_MARKER, 0, null));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(true, FILE_ID_MARKER, 3, MARKER_V4));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(true, FILE_ID_MARKER, 1, new UUID(0, 0)));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(true, FILE_ID_MARKER, 2,
                UUID.fromString("a934a0b3-33d5-3eab-8b17-e61150abbf00")));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(true, FILE_ID_MARKER, 2,
                UUID.fromString("a934a0b3-33d5-4eab-cb17-e61150abbf00")));

        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.SourceFacts(plan(), "C:\\world", FILE_ID_2, FILE_ID_3, FILE_ID_CUSTODY,
                absent(), 1234, 132537600000000000L, new WindowsWorldProvisioner.BootIdentity(BOOT_ID, 7), LOWER_SHA, LOWER_SHA));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.SourceFacts(plan(), FILE_ID_1, FILE_ID_2, FILE_ID_3, "C:\\guard",
                absent(), 1234, 132537600000000000L, new WindowsWorldProvisioner.BootIdentity(BOOT_ID, 7), LOWER_SHA, LOWER_SHA));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.SourceFacts(plan(), FILE_ID_1, FILE_ID_2, FILE_ID_3, FILE_ID_CUSTODY,
                absent(), 1234, 132537600000000000L, new WindowsWorldProvisioner.BootIdentity(BOOT_ID, 7),
                LOWER_SHA.toUpperCase(), LOWER_SHA));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.SourceFacts(plan(), FILE_ID_1, FILE_ID_2, FILE_ID_3, FILE_ID_CUSTODY,
                absent(), 0, 132537600000000000L, new WindowsWorldProvisioner.BootIdentity(BOOT_ID, 7), LOWER_SHA, LOWER_SHA));
        assertThrows(IllegalArgumentException.class,
            () -> new DbFirstWorldAdoptionReceiptCodec.SourceFacts(plan(), FILE_ID_1, FILE_ID_2, FILE_ID_3, FILE_ID_CUSTODY,
                absent(), 1234, 132537600000000000L,
                new WindowsWorldProvisioner.BootIdentity(new UUID(0, 0), 7), LOWER_SHA, LOWER_SHA));
        assertThrows(IllegalArgumentException.class,
            () -> facts(plan(UUID.fromString("55555555-5555-4555-c555-555555555555")), absent()));
    }

    @Test void decoderRejectsBodyDigestTruncationBoundsUnknownVersionTokenAndTrailingBytes() throws Exception {
        byte[] good = DbFirstWorldAdoptionReceiptCodec.encode(facts(absent()));
        byte[] badDigest = good.clone();
        badDigest[badDigest.length - 1] ^= 1;
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(badDigest));
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(Arrays.copyOf(good, good.length - 1)));
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(new byte[4097]));

        byte[] unknownVersion = body(good);
        unknownVersion[11] = 2;
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(unknownVersion)));

        byte[] unknownToken = body(good);
        unknownToken[16] ^= 1;
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(unknownToken)));

        byte[] completeBody = body(good);
        byte[] withTrailing = Arrays.copyOf(completeBody, completeBody.length + 1);
        withTrailing[completeBody.length] = 0;
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(withTrailing)));
    }

    @Test void decoderRejectsMalformedUtf8AndAPlanOutsideSchema6Physical2() throws Exception {
        byte[] good = DbFirstWorldAdoptionReceiptCodec.encode(facts(absent()));
        byte[] malformedUtf8 = body(good);
        int schemaOffset = schemaOffset();
        malformedUtf8[schemaOffset] = (byte) 0xc3;
        IOException utf8Failure = assertThrows(IOException.class,
            () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(malformedUtf8)));
        assertNotNull(utf8Failure.getCause());

        byte[] wrongPhysicalFormat = body(good);
        ByteBuffer.wrap(wrongPhysicalFormat).putInt(physicalFormatOffset(), 1);
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(wrongPhysicalFormat)));

        byte[] wrongSchemaFormat = body(good);
        ByteBuffer.wrap(wrongSchemaFormat).putInt(schemaOffset + "floatingpod_rbss_v1".length(), 5);
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(wrongSchemaFormat)));
    }

    @Test void decoderRejectsInvalidNativeFileIdAndZeroBootUuid() throws Exception {
        byte[] good = DbFirstWorldAdoptionReceiptCodec.encode(facts(absent()));

        byte[] badFileId = body(good);
        badFileId[selectedSaveFileIdOffset(badFileId)] = 'C';
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(badFileId)));

        byte[] badCustodyFileId = body(good);
        badCustodyFileId[custodyFileIdOffset(badCustodyFileId)] = 'C';
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(badCustodyFileId)));

        byte[] zeroBoot = body(good);
        Arrays.fill(zeroBoot, bootUuidOffset(zeroBoot), bootUuidOffset(zeroBoot) + 16, (byte) 0);
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(zeroBoot)));
    }

    @Test void decoderRejectsWrongOldMarkerProofFormatAndUuidVersion() throws Exception {
        byte[] good = DbFirstWorldAdoptionReceiptCodec.encode(facts(present(2, MARKER_V4)));
        int markerOffset = markerBytesOffset(body(good));

        byte[] wrongMagic = body(good);
        ByteBuffer.wrap(wrongMagic).putInt(markerOffset, 0x46505743);
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(wrongMagic)));

        byte[] wrongFormat = body(good);
        ByteBuffer.wrap(wrongFormat).putInt(markerOffset + 4, 3);
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(wrongFormat)));

        byte[] wrongV2Uuid = body(good);
        int versionByte = markerOffset + 8 + 6;
        wrongV2Uuid[versionByte] = (byte) ((wrongV2Uuid[versionByte] & 0x0f) | 0x30);
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(wrongV2Uuid)));

        byte[] wrongVariant = body(good);
        int variantByte = markerOffset + 8 + 8;
        wrongVariant[variantByte] = (byte) ((wrongVariant[variantByte] & 0x3f) | 0xc0);
        assertThrows(IOException.class, () -> DbFirstWorldAdoptionReceiptCodec.decode(reseal(wrongVariant)));
    }

    private static int physicalFormatOffset() {
        return 8 + 4 + 4 + DbFirstWorldAdoptionReceiptCodec.SOURCE_TOKEN.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
            + (5 * 16);
    }

    private static int schemaOffset() {
        return physicalFormatOffset() + 4 + 16 + 4;
    }

    private static int markerBytesOffset(byte[] body) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            in.readLong();
            in.readInt();
            readText(in);
            for (int i = 0; i < 5; i++) skipUuid(in);
            in.readInt();
            skipUuid(in);
            readText(in);
            in.readInt();
            in.skipNBytes(32);
            skipUuid(in);
            readText(in);
            readText(in);
            readText(in);
            readText(in);
            if (!in.readBoolean()) throw new IOException("Test fixture requires a present marker");
            readText(in);
            return body.length - in.available();
        }
    }

    private static int selectedSaveFileIdOffset(byte[] body) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            in.readLong();
            in.readInt();
            readText(in);
            for (int i = 0; i < 5; i++) skipUuid(in);
            in.readInt();
            skipUuid(in);
            readText(in);
            in.readInt();
            in.skipNBytes(32);
            skipUuid(in);
            int fieldOffset = body.length - in.available();
            return fieldOffset + 4;
        }
    }

    private static int custodyFileIdOffset(byte[] body) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            in.readLong();
            in.readInt();
            readText(in);
            for (int i = 0; i < 5; i++) skipUuid(in);
            in.readInt();
            skipUuid(in);
            readText(in);
            in.readInt();
            in.skipNBytes(32);
            skipUuid(in);
            readText(in);
            readText(in);
            readText(in);
            int fieldOffset = body.length - in.available();
            return fieldOffset + 4;
        }
    }

    private static int bootUuidOffset(byte[] body) throws IOException {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            in.readLong();
            in.readInt();
            readText(in);
            for (int i = 0; i < 5; i++) skipUuid(in);
            in.readInt();
            skipUuid(in);
            readText(in);
            in.readInt();
            in.skipNBytes(32);
            skipUuid(in);
            readText(in);
            readText(in);
            readText(in);
            readText(in);
            if (in.readBoolean()) {
                readText(in);
                in.skipNBytes(24);
            }
            in.skipNBytes(16);
            return body.length - in.available();
        }
    }

    private static String readText(DataInputStream in) throws IOException {
        int length = in.readInt();
        return new String(in.readNBytes(length), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void skipUuid(DataInputStream in) throws IOException {
        in.readLong();
        in.readLong();
    }

    private static byte[] body(byte[] complete) {
        return Arrays.copyOf(complete, complete.length - 32);
    }

    private static byte[] reseal(byte[] body) throws Exception {
        byte[] complete = Arrays.copyOf(body, body.length + 32);
        System.arraycopy(MessageDigest.getInstance("SHA-256").digest(body), 0, complete, body.length, 32);
        return complete;
    }
}
