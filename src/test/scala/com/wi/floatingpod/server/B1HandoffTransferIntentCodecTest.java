package com.wi.floatingpod.server;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static com.wi.floatingpod.server.B1HandoffTransferIntentFixture.*;

@Tag("b1-two-slot-pure")
class B1HandoffTransferIntentCodecTest {
    @Test void completeInitialAndSuccessorIntentPreserveAllOriginalBytesAndRefs() throws Exception {
        for (long t : new long[] {0, 1, Long.MAX_VALUE}) {
            var original = intent(t); byte[] bytes = B1HandoffTransferIntentCodec.encode(original);
            var cold = B1HandoffTransferIntentCodec.decode(bytes, PROFILE);
            assertArrayEquals(bytes, B1HandoffTransferIntentCodec.encode(cold));
            assertEquals(original.basename(), cold.basename());
            assertArrayEquals(original.inventory().bridgeBytes(), cold.inventory().bridgeBytes());
            assertArrayEquals(original.inventory().currentBytes(), cold.inventory().currentBytes());
            assertArrayEquals(original.inventory().inactiveBytes(), cold.inventory().inactiveBytes());
            assertArrayEquals(original.beforeOwner(), cold.beforeOwner()); assertArrayEquals(original.targetOwner(), cold.targetOwner());
            assertArrayEquals(original.oldTerminal().body(), cold.oldTerminal().body());
            assertTrue(original.inventory().closure().references().get(0).exactlyEquals(cold.inventory().closure().references().get(0)));
        }
    }
    @Test void actualIntentReferenceDataDerivesSameProductionResultAndIndependentRetirement() throws Exception {
        var original = intent(0); byte[] full = B1HandoffTransferIntentCodec.encode(original);
        var ref = new B1HandoffTransferDataCodec.Reference(original.basename(), id(11), full.length,
            B1HandoffTransferDataCodec.digest(full));
        byte[] cResult = B1HandoffTransferDataCodec.encodeTargetResult(original.targetResult(ref));
        var cold = B1HandoffTransferIntentCodec.decode(full, PROFILE);
        byte[] dResult = B1HandoffTransferDataCodec.encodeTargetResult(cold.targetResult(ref));
        assertArrayEquals(cResult, dResult);
        var resultRef = new B1HandoffTransferDataCodec.Reference(cold.targetResult(ref).basename(), id(12),
            dResult.length, B1HandoffTransferDataCodec.digest(dResult));
        var retirement = new B1HandoffTransferDataCodec.RetirementData(cold.targetResult(ref), resultRef);
        byte[] retired = B1HandoffTransferDataCodec.encodeRetirement(retirement);
        assertArrayEquals(retired, B1HandoffTransferDataCodec.encodeRetirement(
            B1HandoffTransferDataCodec.decodeRetirement(retired, PROFILE)));
        var wrong = new B1HandoffTransferDataCodec.Reference(original.basename(), id(11), full.length, new byte[32]);
        assertThrows(IOException.class, () -> original.targetResult(wrong));
    }
    @Test void everyIntentCrashPrefixCompletesSameOriginalBodyWithoutRedrawingTarget() throws Exception {
        var original = intent(0); byte[] full = B1HandoffTransferIntentCodec.encode(original);
        // expected fullbody 保留同原 I；DATA prefix completion 不是 cold private issuer。
        for (int k = 0; k <= full.length; k++) {
            byte[] partial = Arrays.copyOf(full, k), saved = partial.clone();
            byte[] suffix = B1HandoffExactPrefix.artifactSuffix(full, partial, PROFILE.maxNativeArtifactBytes());
            byte[] completed = Arrays.copyOf(partial, full.length);
            System.arraycopy(suffix, 0, completed, k, suffix.length);
            assertArrayEquals(full, completed); assertArrayEquals(saved, partial);
        }
        var cold = B1HandoffTransferIntentCodec.decode(full, PROFILE);
        assertArrayEquals(original.targetOwner(), cold.targetOwner());
    }
    @Test void allTruncatedIntentBodiesTrailingAndUnknownTagsFailExplicitly() throws Exception {
        byte[] full = B1HandoffTransferIntentCodec.encode(intent(0));
        for (int k = 0; k < full.length; k++) {
            byte[] partial = Arrays.copyOf(full, k);
            assertThrows(IOException.class, () -> B1HandoffTransferIntentCodec.decode(partial, PROFILE));
        }
        assertThrows(IOException.class, () -> B1HandoffTransferIntentCodec.decode(Arrays.copyOf(full, full.length + 1), PROFILE));
        byte[] unknown = full.clone(); unknown[4] ^= 1;
        assertThrows(IOException.class, () -> B1HandoffTransferIntentCodec.decode(unknown, PROFILE));
    }
    @Test void sourceGuardIdentityForeignContainerAndMissingClosureCannotBecomeInventory() throws Exception {
        var p = parts();
        assertThrows(IOException.class, () -> inventory(p, List.of()));
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.Inventory(PROFILE,
            p.bridge.sourceAnchorSha256(), id(99), id(2), id(3), B1SourceAnchorFixture.WORLD,
            p.bridgeBytes, id(4), p.currentBytes, id(6), p.inactiveBytes, id(5), List.of(), List.of(p.artifact)));
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.Inventory(PROFILE,
            p.bridge.sourceAnchorSha256(), id(1), id(2), id(3), B1SourceAnchorFixture.WORLD,
            p.bridgeBytes, id(4), p.currentBytes, id(6), p.inactiveBytes, id(6), List.of(), List.of(p.artifact)));
        byte[] current = p.currentBytes.clone(); current[current.length - 1] ^= 1;
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.Inventory(PROFILE,
            p.bridge.sourceAnchorSha256(), id(1), id(2), id(3), B1SourceAnchorFixture.WORLD,
            p.bridgeBytes, id(4), current, id(6), p.inactiveBytes, id(5), List.of(), List.of(p.artifact)));
    }
    @Test void wrongTerminalGenerationMissingPriorAndChangedOwnerCannotFormIntent() throws Exception {
        var p = parts(); var inventory = inventory(p, List.of(p.artifact));
        var terminal = nativeArtifact(B1HandoffGenerationName.Family.TERMINAL, inventory.baseKey(), 3, -1, 7);
        var wrong = nativeArtifact(B1HandoffGenerationName.Family.TERMINAL, inventory.baseKey(), 4, -1, 7);
        byte[] b = owner(p.bridge.originalReceipt(), 100, 10), c = owner(p.bridge.originalReceipt(), 200, 20);
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.IntentData(
            new B1HandoffTransferIntentCodec.OriginalBirthPredecessorData(inventory, 3), inventory, 3, 0, wrong, null, b, c, "ORIGINAL", new byte[] {1}));
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.IntentData(
            new B1HandoffTransferIntentCodec.OriginalBirthPredecessorData(inventory, 3), inventory, 3, 1, terminal, null, b, c, "ORIGINAL", new byte[] {1}));
        byte[] sameBoot = owner(p.bridge.originalReceipt(), 200, 10);
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.IntentData(
            new B1HandoffTransferIntentCodec.OriginalBirthPredecessorData(inventory, 3), inventory, 3, 0, terminal, null, b, sameBoot, "ORIGINAL", new byte[] {1}));
        byte[] corrupted = c.clone(); corrupted[163] ^= 1;
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.IntentData(
            new B1HandoffTransferIntentCodec.OriginalBirthPredecessorData(inventory, 3), inventory, 3, 0, terminal, null, b, corrupted, "ORIGINAL", new byte[] {1}));
    }
    @Test void wrongStoredKeyAndMalformedNativePathCannotRoundTripAsOriginalIntent() throws Exception {
        var original = intent(0); byte[] bytes = B1HandoffTransferIntentCodec.encode(original);
        byte[] key = original.inventory().baseKey().getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        int start = -1;
        for (int i = 0; i <= bytes.length - key.length; i++) {
            boolean equal = true;
            for (int j = 0; j < key.length; j++) if (bytes[i + j] != key[j]) { equal = false; break; }
            if (equal) { start = i; break; }
        }
        assertTrue(start >= 0); bytes[start] = bytes[start] == 'a' ? (byte) 'b' : (byte) 'a';
        assertThrows(IOException.class, () -> B1HandoffTransferIntentCodec.decode(bytes, PROFILE));
        var p = parts();
        String malformed = new String(new char[] {(char) 0xd800});
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.Inventory(PROFILE,
            p.bridge.sourceAnchorSha256(), id(1), id(2), id(3), malformed,
            p.bridgeBytes, id(4), p.currentBytes, id(6), p.inactiveBytes, id(5), List.of(), List.of(p.artifact)));
    }
    private static B1HandoffTransferIntentCodec.Inventory advanced(Parts p, boolean wrongRetainedIdentity,
            boolean skipGeneration) throws Exception {
        var old = p.current.transition();
        long sequence = skipGeneration ? 4 : 3;
        var nextTransition = new B1ResumeRecord(sequence,
            B1HandoffTransferDataCodec.digest(B1ResumeRecordCanonical.encode(old)),
            old.kind(), old.sourceAnchorSha256(), old.originalReceipt(), old.barrierNamespace(), old.assertions());
        var next = new B1ResumeRecordV5(nextTransition, p.current.bridgeSequence(), p.current.bridgeSha256(),
            p.current.bridgeIdentity(), B1HandoffTransferDataCodec.digest(p.currentBytes), p.current.references());
        return new B1HandoffTransferIntentCodec.Inventory(PROFILE, p.bridge.sourceAnchorSha256(),
            id(1), id(2), id(3), B1SourceAnchorFixture.WORLD, p.bridgeBytes, id(4),
            B1ResumeRecordV5.encodeData(next), id(13), p.currentBytes, wrongRetainedIdentity ? id(14) : id(6),
            List.of(), List.of(p.artifact));
    }
    @Test void originalOldKeyTerminalSurvivesTrueNextV5PrefixAndSameImmutableBirthClosure() throws Exception {
        var p = parts(); var predecessor = inventory(p, List.of(p.artifact));
        var birth = new B1HandoffTransferIntentCodec.OriginalBirthPredecessorData(predecessor, 3);
        var observed = advanced(p, false, false);
        assertNotEquals(predecessor.baseKey(), observed.baseKey());
        var terminal = nativeArtifact(B1HandoffGenerationName.Family.TERMINAL, predecessor.baseKey(), 3, -1, 7);
        var transfer = new B1HandoffTransferIntentCodec.IntentData(birth, observed, 3, 0, terminal, null,
            owner(p.bridge.originalReceipt(), 100, 10), owner(p.bridge.originalReceipt(), 200, 20),
            "ORIGINAL", new byte[] {1});
        assertEquals(B1HandoffGenerationName.transfer(B1HandoffGenerationName.Family.TRANSFER,
            predecessor.baseKey(), 3, 0).basename(), transfer.basename());
        byte[] bytes = B1HandoffTransferIntentCodec.encode(transfer);
        var cold = B1HandoffTransferIntentCodec.decode(bytes, PROFILE);
        assertArrayEquals(bytes, B1HandoffTransferIntentCodec.encode(cold));
        assertEquals(predecessor.baseKey(), cold.originalBirth().baseKey());
        assertArrayEquals(p.inactiveBytes, cold.originalBirth().predecessor().inactiveBytes());
        assertArrayEquals(observed.currentBytes(), cold.inventory().currentBytes());
        var ref = new B1HandoffTransferDataCodec.Reference(cold.basename(), id(15), bytes.length,
            B1HandoffTransferDataCodec.digest(bytes));
        assertEquals(predecessor.baseKey(), cold.targetResult(ref).baseKey());
        assertEquals(3, cold.targetResult(ref).generation());
        assertArrayEquals(B1HandoffTransferIntentCodec.encodeBirthPredecessor(birth),
            B1HandoffTransferIntentCodec.encodeBirthPredecessor(cold.originalBirth()));
    }
    @Test void observedNewKeyTerminalAndForeignMissingOriginalLineageAreRejected() throws Exception {
        var p = parts(); var predecessor = inventory(p, List.of(p.artifact));
        var birth = new B1HandoffTransferIntentCodec.OriginalBirthPredecessorData(predecessor, 3);
        var observed = advanced(p, false, false);
        var wrongTerminal = nativeArtifact(B1HandoffGenerationName.Family.TERMINAL, observed.baseKey(), 3, -1, 7);
        byte[] b = owner(p.bridge.originalReceipt(), 100, 10), c = owner(p.bridge.originalReceipt(), 200, 20);
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.IntentData(
            birth, observed, 3, 0, wrongTerminal, null, b, c, "ORIGINAL", new byte[] {1}));
        var terminal = nativeArtifact(B1HandoffGenerationName.Family.TERMINAL, predecessor.baseKey(), 3, -1, 7);
        var foreign = advanced(p, true, false);
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.IntentData(
            birth, foreign, 3, 0, terminal, null, b, c, "ORIGINAL", new byte[] {1}));
        assertThrows(IOException.class, () -> advanced(p, false, true));
        byte[] fullBirth = B1HandoffTransferIntentCodec.encodeBirthPredecessor(birth);
        assertThrows(IOException.class, () -> B1HandoffTransferIntentCodec.decodeBirthPredecessor(
            Arrays.copyOf(fullBirth, fullBirth.length - 1), PROFILE));
        assertThrows(IOException.class, () -> new B1HandoffTransferIntentCodec.IntentData(
            birth, observed, 4, 0, terminal, null, b, c, "ORIGINAL", new byte[] {1}));
    }
    @Test void defensiveCopiesKeepOriginalBodyKeyAndTargetFixed() throws Exception {
        var original = intent(0); byte[] full = B1HandoffTransferIntentCodec.encode(original);
        byte[] b = original.beforeOwner(), c = original.targetOwner(), terminal = original.oldTerminal().body(),
            bridge = original.inventory().bridgeBytes(), artifact = original.inventory().closure().artifacts().get(0).body();
        for (byte[] exposed : List.of(b, c, terminal, bridge, artifact)) exposed[0] ^= 1;
        assertArrayEquals(full, B1HandoffTransferIntentCodec.encode(original));
        assertThrows(UnsupportedOperationException.class, () -> original.inventory().closure().artifacts().clear());
    }
}
