package com.wi.floatingpod.entity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;
import net.minecraft.nbt.NBTSizeTracker;
import org.junit.jupiter.api.Test;

class PodArtifactNbtCodecTest {
    private static final UUID BINDING = UUID.fromString("a1000000-0000-4000-a000-000000000001");
    private static final UUID ATTEMPT = UUID.fromString("b2000000-0000-4000-b000-000000000002");
    private static final Set<String> FIELDS = Set.of("Format", "Binding", "Attempt", "Origin");

    @Test
    void allThreeOriginsSurviveRealBinaryNbtRoundTripAndPreserveRootData() throws IOException {
        for (PodArtifactIdentity.Origin origin : PodArtifactIdentity.Origin.values()) {
            NBTTagCompound root = new NBTTagCompound();
            root.setString("unrelated", "preserved");
            root.setLong("unrelatedLong", 91L);
            NBTTagCompound forgeCaps = new NBTTagCompound();
            forgeCaps.setByteArray("cap", new byte[]{3, 4, 9});
            root.setTag("ForgeCaps", forgeCaps);

            PodArtifactIdentity expected = new PodArtifactIdentity(BINDING, ATTEMPT, origin);
            PodArtifactNbtCodec.write(root, expected);

            NBTTagCompound writtenArtifact = root.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY);
            assertEquals(FIELDS, writtenArtifact.getKeySet());
            assertEquals(expectedWire(origin), writtenArtifact.getString("Origin"));
            NBTTagCompound decodedRoot = binaryRoundTrip(root);
            assertEquals(expected, PodArtifactNbtCodec.read(decodedRoot).orElseThrow());
            assertEquals("preserved", decodedRoot.getString("unrelated"));
            assertEquals(91L, decodedRoot.getLong("unrelatedLong"));
            assertArrayEquals(new byte[]{3, 4, 9}, decodedRoot.getCompoundTag("ForgeCaps").getByteArray("cap"));
        }
    }

    @Test
    void onlyAnAbsentRootKeyIsLegacyAbsence() {
        NBTTagCompound root = new NBTTagCompound();
        root.setString("unrelated", "retained");
        assertEquals(java.util.Optional.empty(), PodArtifactNbtCodec.read(root));

        root.setTag(PodArtifactNbtCodec.ROOT_KEY, new NBTTagCompound());
        assertThrows(IllegalArgumentException.class, () -> PodArtifactNbtCodec.read(root));

        root.setString(PodArtifactNbtCodec.ROOT_KEY, "not a compound");
        assertThrows(IllegalArgumentException.class, () -> PodArtifactNbtCodec.read(root));

        root.setTag(PodArtifactNbtCodec.ROOT_KEY, new NBTTagList());
        assertThrows(IllegalArgumentException.class, () -> PodArtifactNbtCodec.read(root));
    }

    @Test
    void everyExpectedFieldIsRequiredAndUnknownNestedFieldsAreRejected() {
        for (String missing : FIELDS) {
            NBTTagCompound partial = validArtifact();
            partial.removeTag(missing);
            assertReadFailsWith(partial);
        }

        for (String extra : Set.of("owner", "modules", "domainVersion")) {
            NBTTagCompound extended = validArtifact();
            extended.setString(extra, "not part of the identity contract");
            assertReadFailsWith(extended);
        }
    }

    @Test
    void formatRequiresAnIntegerWithTheExactSupportedVersion() {
        NBTTagCompound wrongType = validArtifact();
        wrongType.setString("Format", "1");
        assertReadFailsWith(wrongType);

        NBTTagCompound byteType = validArtifact();
        byteType.setByte("Format", (byte) 1);
        assertReadFailsWith(byteType);

        NBTTagCompound shortType = validArtifact();
        shortType.setShort("Format", (short) 1);
        assertReadFailsWith(shortType);

        NBTTagCompound longType = validArtifact();
        longType.setLong("Format", 1L);
        assertReadFailsWith(longType);

        NBTTagCompound wrongVersion = validArtifact();
        wrongVersion.setInteger("Format", 2);
        assertReadFailsWith(wrongVersion);
    }

    @Test
    void uuidFieldsRequireStringTagsCanonicalTextAndPreserveParserCause() {
        for (String field : new String[]{"Binding", "Attempt"}) {
            NBTTagCompound wrongType = validArtifact();
            wrongType.setInteger(field, 1);
            assertReadFailsWith(wrongType);
        }

        NBTTagCompound shortened = validArtifact();
        shortened.setString("Binding", "1-1-1-1-1");
        assertReadFailsWith(shortened);

        NBTTagCompound uppercase = validArtifact();
        uppercase.setString("Binding", BINDING.toString().toUpperCase(Locale.ROOT));
        assertReadFailsWith(uppercase);

        NBTTagCompound whitespace = validArtifact();
        whitespace.setString("Attempt", " " + ATTEMPT);
        assertReadFailsWith(whitespace);

        NBTTagCompound invalid = validArtifact();
        invalid.setString("Attempt", "not-a-uuid");
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> PodArtifactNbtCodec.read(rootWith(invalid)));
        assertNotNull(failure.getCause());
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
    }

    @Test
    void originMustBeAStringWithOneOfTheThreeFrozenWireValues() {
        NBTTagCompound wrongType = validArtifact();
        wrongType.setInteger("Origin", 1);
        assertReadFailsWith(wrongType);

        for (String value : new String[]{"", "OWNED", "arbitrary"}) {
            NBTTagCompound unknown = validArtifact();
            unknown.setString("Origin", value);
            assertReadFailsWith(unknown);
        }
    }

    @Test
    void writeIsIdempotentForTheSameIdentityAndRejectsDifferentOrMalformedExistingTags() {
        PodArtifactIdentity identity = new PodArtifactIdentity(
                BINDING, ATTEMPT, PodArtifactIdentity.Origin.DEBUG_ITEM_CREATE);
        NBTTagCompound root = new NBTTagCompound();
        root.setInteger("unrelated", 23);
        PodArtifactNbtCodec.write(root, identity);

        NBTTagCompound beforeSameWrite = root.copy();
        PodArtifactNbtCodec.write(root, identity);
        assertEquals(beforeSameWrite, root);

        NBTTagCompound beforeDifferentWrite = root.copy();
        PodArtifactIdentity different = new PodArtifactIdentity(
                UUID.fromString("c3000000-0000-4000-a000-000000000003"),
                ATTEMPT,
                PodArtifactIdentity.Origin.DEBUG_ITEM_CREATE);
        assertThrows(IllegalArgumentException.class, () -> PodArtifactNbtCodec.write(root, different));
        assertEquals(beforeDifferentWrite, root);

        NBTTagCompound malformed = root.copy();
        malformed.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY).setString("owner", "extra");
        NBTTagCompound beforeMalformedWrite = malformed.copy();
        assertThrows(IllegalArgumentException.class, () -> PodArtifactNbtCodec.write(malformed, identity));
        assertEquals(beforeMalformedWrite, malformed);
    }

    @Test
    void nullIdentityIsRejectedWithoutChangingTheRoot() {
        NBTTagCompound root = new NBTTagCompound();
        root.setString("unrelated", "preserved");
        NBTTagCompound before = root.copy();

        assertThrows(IllegalArgumentException.class, () -> PodArtifactNbtCodec.write(root, null));
        assertEquals(before, root);
    }

    @Test
    void rootAndIdentityRecordRejectNullInputs() {
        assertThrows(NullPointerException.class, () -> PodArtifactNbtCodec.read(null));
        assertThrows(NullPointerException.class,
                () -> PodArtifactNbtCodec.write(null, new PodArtifactIdentity(
                        BINDING, ATTEMPT, PodArtifactIdentity.Origin.OWNED_RELEASE)));
        assertThrows(NullPointerException.class,
                () -> new PodArtifactIdentity(null, ATTEMPT, PodArtifactIdentity.Origin.OWNED_RELEASE));
        assertThrows(NullPointerException.class,
                () -> new PodArtifactIdentity(BINDING, null, PodArtifactIdentity.Origin.OWNED_RELEASE));
        assertThrows(NullPointerException.class,
                () -> new PodArtifactIdentity(BINDING, ATTEMPT, null));
    }

    private static String expectedWire(PodArtifactIdentity.Origin origin) {
        return switch (origin) {
            case OWNED_RELEASE -> "OWNED_RELEASE";
            case UNBOUND_ADOPTION -> "UNBOUND_ADOPTION";
            case DEBUG_ITEM_CREATE -> "DEBUG_ITEM_CREATE";
        };
    }

    private static NBTTagCompound validArtifact() {
        NBTTagCompound artifact = new NBTTagCompound();
        artifact.setInteger("Format", 1);
        artifact.setString("Binding", BINDING.toString());
        artifact.setString("Attempt", ATTEMPT.toString());
        artifact.setString("Origin", "DEBUG_ITEM_CREATE");
        return artifact;
    }

    private static NBTTagCompound rootWith(NBTTagCompound artifact) {
        NBTTagCompound root = new NBTTagCompound();
        root.setTag(PodArtifactNbtCodec.ROOT_KEY, artifact);
        return root;
    }

    private static void assertReadFailsWith(NBTTagCompound artifact) {
        assertThrows(IllegalArgumentException.class,
                () -> PodArtifactNbtCodec.read(rootWith(artifact)));
    }

    private static NBTTagCompound binaryRoundTrip(NBTTagCompound root) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            CompressedStreamTools.write(root, output);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return CompressedStreamTools.read(input, new NBTSizeTracker(8_000_000L));
        }
    }
}
