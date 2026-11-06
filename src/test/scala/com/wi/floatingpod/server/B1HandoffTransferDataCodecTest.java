package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@Tag("b1-two-slot-pure")
class B1HandoffTransferDataCodecTest {
    private static final String KEY = "abcdef0123456789".repeat(4);
    private static final SourceLimitProfile PROFILE = SourceLimitProfile.of(
        1_000_000, 1_000_000, 1_000_000, 64, 100, 10_000_000, 1_000_000, 1_000_000);
    private static final WindowsMarkerIo.FileIdentity DIR = id(1), GUARD = id(2), RECEIPT = id(3);
    private static final byte[] RECEIPT_HASH = filled(32, 19);
    private static WindowsMarkerIo.FileIdentity id(int number) { return new WindowsMarkerIo.FileIdentity(7, filled(16, number)); }
    private static byte[] filled(int size, int value) { byte[] b = new byte[size]; Arrays.fill(b, (byte) value); return b; }
    private static byte[] sha(byte[] b) throws Exception { return MessageDigest.getInstance("SHA-256").digest(b); }
    private static byte[] owner(long pid, long boot) throws Exception {
        ByteBuffer b = ByteBuffer.allocate(164).order(ByteOrder.BIG_ENDIAN);
        b.putLong(WindowsWorldProvisioner.GUARD_MAGIC).putInt(1);
        putIdentity(b, DIR); putIdentity(b, GUARD); b.put(RECEIPT_HASH);
        b.putLong(pid).putLong(133800000000000123L).putLong(0).putLong(boot).putLong(1);
        b.put(sha(Arrays.copyOf(b.array(), 132))); return b.array();
    }
    private static void putIdentity(ByteBuffer b, WindowsMarkerIo.FileIdentity id) {
        b.putLong(id.volumeSerialNumber()).put(id.fileId128());
    }
    private static B1HandoffTransferDataCodec.Reference ref(B1HandoffGenerationName.Family family, int identity) {
        String name = family == B1HandoffGenerationName.Family.TERMINAL ?
            B1HandoffGenerationName.attempt(family, KEY, 3).basename() :
            B1HandoffGenerationName.transfer(family, KEY, 3, 4).basename();
        return new B1HandoffTransferDataCodec.Reference(name, id(identity), 99, filled(32, identity));
    }
    private static B1HandoffTransferDataCodec.TargetResultData data(byte[] before, byte[] target,
            List<B1Full20WitnessReference> preserved) throws Exception {
        return new B1HandoffTransferDataCodec.TargetResultData(PROFILE, filled(32, 20), RECEIPT_HASH,
            "namespace-original", KEY, 3, 4, ref(B1HandoffGenerationName.Family.TRANSFER, 4),
            ref(B1HandoffGenerationName.Family.TERMINAL, 5), before, target, DIR, GUARD, RECEIPT,
            new B1HandoffTransferDataCodec.StateFact(0, 0, id(6), filled(32, 6)),
            new B1HandoffTransferDataCodec.StateFact(2, 0, id(7), filled(32, 7)),
            new B1HandoffTransferDataCodec.StateFact(1, 1, id(8), filled(32, 8)), preserved);
    }
    @Test void originalTargetResultRoundTripAndIndependentRetirementUseExactCompleteBody() throws Exception {
        var original = data(owner(100, 10), owner(200, 20), List.of());
        byte[] result = B1HandoffTransferDataCodec.encodeTargetResult(original);
        var decoded = B1HandoffTransferDataCodec.decodeTargetResult(result, PROFILE);
        assertArrayEquals(result, B1HandoffTransferDataCodec.encodeTargetResult(decoded));
        var resultRef = new B1HandoffTransferDataCodec.Reference(original.basename(), id(9), result.length, sha(result));
        var retirement = new B1HandoffTransferDataCodec.RetirementData(decoded, resultRef);
        byte[] retired = B1HandoffTransferDataCodec.encodeRetirement(retirement);
        assertArrayEquals(retired, B1HandoffTransferDataCodec.encodeRetirement(
            B1HandoffTransferDataCodec.decodeRetirement(retired, PROFILE)));
        assertArrayEquals(result, B1HandoffTransferDataCodec.encodeTargetResult(retirement.original()));
        assertNotEquals(original.basename(), retirement.basename());
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeTargetResult(retired, PROFILE));
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeRetirement(result, PROFILE));
    }
    @Test void successorAtEveryResultWriteBoundaryDerivesSameFullBodyAndOnlyAppendsSuffix() throws Exception {
        var cOriginal = data(owner(100, 10), owner(200, 20), List.of());
        byte[] cBody = B1HandoffTransferDataCodec.encodeTargetResult(cOriginal);
        // 真 D issuer 的输入必须仍是完整原 I；这里测试 DATA 的 byte contract，不模拟生命周期资格。
        var dOriginal = data(owner(100, 10), owner(200, 20), List.of());
        byte[] dExpected = B1HandoffTransferDataCodec.encodeTargetResult(dOriginal);
        assertArrayEquals(cBody, dExpected);
        for (int k = 0; k <= cBody.length; k++) {
            byte[] partial = Arrays.copyOf(cBody, k), saved = partial.clone();
            byte[] suffix = B1HandoffExactPrefix.artifactSuffix(dExpected, partial, PROFILE.maxNativeArtifactBytes());
            byte[] complete = Arrays.copyOf(partial, cBody.length);
            System.arraycopy(suffix, 0, complete, k, suffix.length);
            assertArrayEquals(cBody, complete); assertArrayEquals(saved, partial);
        }
    }
    @Test void strictDecodeRejectsEveryTruncationTrailingTagAndWrongProfile() throws Exception {
        byte[] result = B1HandoffTransferDataCodec.encodeTargetResult(data(owner(100, 10), owner(200, 20), List.of()));
        for (int k = 0; k < result.length; k++) {
            byte[] truncated = Arrays.copyOf(result, k);
            assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeTargetResult(truncated, PROFILE));
        }
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeTargetResult(Arrays.copyOf(result, result.length + 1), PROFILE));
        byte[] wrong = result.clone(); wrong[4] ^= 1;
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeTargetResult(wrong, PROFILE));
        var different = SourceLimitProfile.of(1_000_000, 1_000_000, 1_000_000, 65, 100, 10_000_000, 1_000_000, 1_000_000);
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeTargetResult(result, different));
    }
    @Test void actualResultReferenceMustMatchBodyNameLengthHashBeforeRetirementData() throws Exception {
        var original = data(owner(100, 10), owner(200, 20), List.of());
        byte[] body = B1HandoffTransferDataCodec.encodeTargetResult(original);
        var wrongHash = new B1HandoffTransferDataCodec.Reference(original.basename(), id(9), body.length, new byte[32]);
        assertThrows(IOException.class, () -> new B1HandoffTransferDataCodec.RetirementData(original, wrongHash));
        var wrongLength = new B1HandoffTransferDataCodec.Reference(original.basename(), id(9), body.length + 1, sha(body));
        assertThrows(IOException.class, () -> new B1HandoffTransferDataCodec.RetirementData(original, wrongLength));
        var wrongName = new B1HandoffTransferDataCodec.Reference(ref(B1HandoffGenerationName.Family.TRANSFER, 9).basename(), id(9), body.length, sha(body));
        assertThrows(IOException.class, () -> new B1HandoffTransferDataCodec.RetirementData(original, wrongName));
    }
    @Test void completeOwnersCannotChangeSourceGuardReceiptOrPretendSameBootTransfer() throws Exception {
        byte[] b = owner(100, 10), c = owner(200, 20), bad = c.clone();
        bad[20] ^= 1; System.arraycopy(sha(Arrays.copyOf(bad, 132)), 0, bad, 132, 32);
        assertThrows(IOException.class, () -> data(b, bad, List.of()));
        assertThrows(IOException.class, () -> data(b, owner(200, 10), List.of()));
        byte[] badDigest = c.clone(); badDigest[163] ^= 1;
        assertThrows(IOException.class, () -> data(b, badDigest, List.of()));
    }
    @Test void preservedRefsAreImmutableCompleteAndDuplicateNamesRejected() throws Exception {
        UUID attempt = new UUID(1, 2), operation = new UUID(3, 4);
        var reference = new B1Full20WitnessReference(B1Full20WitnessReference.basename(attempt, filled(32, 10)),
            id(10), 20, filled(32, 10), filled(32, 20), RECEIPT_HASH, PROFILE.sha256(),
            attempt, B1ResumeRecord.Phase.PREPARE, operation);
        var original = data(owner(100, 10), owner(200, 20), List.of(reference));
        byte[] encoded = B1HandoffTransferDataCodec.encodeTargetResult(original);
        byte[] exposed = original.targetOwner(); exposed[1] ^= 1;
        byte[] digest = reference.fullSha256(); digest[0] ^= 1;
        assertArrayEquals(encoded, B1HandoffTransferDataCodec.encodeTargetResult(original));
        var cold = B1HandoffTransferDataCodec.decodeTargetResult(encoded, PROFILE);
        assertTrue(reference.exactlyEquals(cold.preserved().get(0)));
        assertThrows(UnsupportedOperationException.class, () -> original.preserved().clear());
        var conflicting = new B1Full20WitnessReference(B1Full20WitnessReference.basename(attempt, filled(32, 11)),
            id(11), 20, filled(32, 11), filled(32, 20), RECEIPT_HASH, PROFILE.sha256(),
            attempt, B1ResumeRecord.Phase.PREPARE, operation);
        assertThrows(IOException.class, () -> data(owner(100, 10), owner(200, 20), List.of(reference, conflicting)));
        assertThrows(IOException.class, () -> data(owner(100, 10), owner(200, 20), List.of(reference, reference)));
        assertThrows(IllegalArgumentException.class, () -> new B1HandoffTransferDataCodec.Reference("../orphan", id(1), 1, new byte[32]));
    }
    @Test void retirementCreatePartialFullBeforeAckHasOneOriginalBodyAndStrictDecode() throws Exception {
        var original = data(owner(100, 10), owner(200, 20), List.of());
        byte[] result = B1HandoffTransferDataCodec.encodeTargetResult(original);
        var r = new B1HandoffTransferDataCodec.Reference(original.basename(), id(9), result.length, sha(result));
        byte[] retired = B1HandoffTransferDataCodec.encodeRetirement(new B1HandoffTransferDataCodec.RetirementData(original, r));
        for (int k = 0; k <= retired.length; k++) {
            byte[] partial = Arrays.copyOf(retired, k);
            byte[] suffix = B1HandoffExactPrefix.artifactSuffix(retired, partial, PROFILE.maxNativeArtifactBytes());
            byte[] complete = Arrays.copyOf(partial, retired.length);
            System.arraycopy(suffix, 0, complete, k, suffix.length); assertArrayEquals(retired, complete);
            if (k < retired.length) assertThrows(IOException.class,
                () -> B1HandoffTransferDataCodec.decodeRetirement(partial, PROFILE));
        }
        byte[] changed = retired.clone(); changed[changed.length - 5] ^= 1;
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeRetirement(changed, PROFILE));
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeRetirement(
            Arrays.copyOf(retired, retired.length + 1), PROFILE));
        byte[] changedGuard = result.clone(); changedGuard[changedGuard.length - 5] ^= 1;
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeTargetResult(changedGuard, PROFILE));
    }
    @Test void artifactWorkspaceBoundsFailBeforeFinalBodyAllocation() throws Exception {
        var original = data(owner(100, 10), owner(200, 20), List.of());
        byte[] bytes = B1HandoffTransferDataCodec.encodeTargetResult(original);
        var tiny = SourceLimitProfile.of(1, 1, 1, 1, 1, 100, 1, 100);
        assertThrows(IOException.class, () -> B1HandoffTransferDataCodec.decodeTargetResult(bytes, tiny));
        assertEquals(bytes.length * 32L + 8192, B1HandoffTransferDataCodec.workspaceBound(bytes.length));
        assertThrows(IllegalArgumentException.class, () -> B1HandoffTransferDataCodec.workspaceBound(0));
    }
}
