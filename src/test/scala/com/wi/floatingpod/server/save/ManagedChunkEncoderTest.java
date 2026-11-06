package com.wi.floatingpod.server.save;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ReadOnlyBufferException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Direct actual Deflater/Inflater and Minecraft NBT behavior; no publish, native, Mixin, or Ready claim. */
final class ManagedChunkEncoderTest {
    @Test void candidateMatchesVanillaZlibBytesAndRoundTripsTheCompleteNbtTree() throws Exception {
        NBTTagCompound root = completeNbt();
        byte[] raw = serializeNbt(root);
        ByteArrayOutputStream vanillaCompressed = new ByteArrayOutputStream();
        try (DataOutputStream vanilla = new DataOutputStream(new BufferedOutputStream(
                new DeflaterOutputStream(vanillaCompressed)))) {
            CompressedStreamTools.write(root, vanilla);
        }

        SourceLimitProfile profile = profile(100_000, 100_000, 100_000);
        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(100_000);
            CompressedStreamTools.write(root, encoder.dataOutput());
            ManagedChunkEncoder.EncodedCandidate candidate = encoder.finishEncoding();

            byte[] actualCompressed = new byte[candidate.compressedLength()];
            candidate.copyTo(actualCompressed, 0);
            assertArrayEquals(vanillaCompressed.toByteArray(), actualCompressed,
                    "default-level zlib output must match the actual vanilla stream for the same complete NBT bytes");
            assertArrayEquals(raw, inflate(actualCompressed, 100_000));
            NBTTagCompound decoded = CompressedStreamTools.read(new DataInputStream(
                    new ByteArrayInputStream(inflate(actualCompressed, 100_000))));
            assertEquals(root, decoded);
            assertEquals(raw.length, candidate.submittedInputBytes());
            assertEquals(actualCompressed.length, candidate.compressedLength());
            assertTrue(candidate.retainedCapacityBytes() <= encoder.outputBudgetBytes());
            assertTrue(candidate.compressedPayload().getFirst().isReadOnly());
            assertThrows(ReadOnlyBufferException.class, () -> candidate.compressedPayload().getFirst().put(0, (byte) 0));
            assertTrue(candidate.belongsTo(scope.context, scope.attempt));
            assertFalse(candidate.belongsTo(scope.context, scope.otherAttempt()));
            assertEquals(ManagedChunkEncoder.State.CANDIDATE, encoder.state());
        }
    }

    @Test void explicitZeroOverProfileAndUnrepresentableBudgetsFailBeforeAllocatingACompressor() {
        SourceLimitProfile profile = profile(4096, 8192, 4096);
        SourceLimitProfile arrayLimitProfile = SourceLimitProfile.of(1_048_576, (long) Integer.MAX_VALUE,
                8192, 64, 200_000, (long) Integer.MAX_VALUE, 1_048_576, 1_048_576);
        AtomicReference<Deflater> unexpected = new AtomicReference<>();
        ManagedChunkEncoder.DeflaterFactory factory = () -> {
            Deflater deflater = new Deflater();
            unexpected.set(deflater);
            return deflater;
        };
        try (SaveScope scope = new SaveScope(profile)) {
            assertThrows(IllegalArgumentException.class, () -> ManagedChunkEncoder.open(
                    scope.context, scope.attempt, profile, 0, factory, ManagedChunkEncoder.BoundedMemoryOutput::new));
            assertNull(unexpected.get());
            assertThrows(IllegalStateException.class, () -> ManagedChunkEncoder.open(
                    scope.context, scope.attempt, SourceLimitProfile.of(1_048_576, 4096, 8192,
                            64, 200_000, 4096, 1_048_576, 1_048_576),
                    4096, factory, ManagedChunkEncoder.BoundedMemoryOutput::new));
            assertThrows(IllegalStateException.class, () -> ManagedChunkEncoder.open(
                    scope.context, scope.otherAttempt(), profile, 4096,
                    factory, ManagedChunkEncoder.BoundedMemoryOutput::new));
            assertNull(unexpected.get());
        }
        SourceLimitProfile chunkCap = profile(16, 8192, 32);
        try (SaveScope scope = new SaveScope(chunkCap)) {
            assertThrows(IllegalArgumentException.class, () -> ManagedChunkEncoder.open(
                    scope.context, scope.attempt, chunkCap, 17, factory, ManagedChunkEncoder.BoundedMemoryOutput::new));
            assertNull(unexpected.get());
        }
        SourceLimitProfile retainedCap = profile(32, 8192, 16);
        try (SaveScope scope = new SaveScope(retainedCap)) {
            assertThrows(IllegalArgumentException.class, () -> ManagedChunkEncoder.open(
                    scope.context, scope.attempt, retainedCap, 17, factory, ManagedChunkEncoder.BoundedMemoryOutput::new));
            assertNull(unexpected.get());
        }
        try (SaveScope scope = new SaveScope(arrayLimitProfile)) {
            assertThrows(IllegalArgumentException.class, () -> ManagedChunkEncoder.open(
                    scope.context, scope.attempt, arrayLimitProfile, (long) Integer.MAX_VALUE,
                    factory, ManagedChunkEncoder.BoundedMemoryOutput::new));
            assertNull(unexpected.get());
        }
    }

    @Test void inputOverflowFailsBeforeForwardingAndAbortDoesNotFlushFinishOrClose() throws Exception {
        SourceLimitProfile profile = profile(1024, 8, 1024);
        TrackingDeflater deflater = new TrackingDeflater();
        TrackingMemoryOutput memory = new TrackingMemoryOutput(1024);
        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(1024, () -> deflater, ignored -> memory);
            DataOutputStream output = encoder.dataOutput();
            output.writeLong(0x0102030405060708L);
            IOException failure = assertThrows(IOException.class, () -> output.writeByte(9));
            assertSame(failure, assertThrows(IOException.class, encoder::finishEncoding));
            assertEquals(8, encoder.submittedInputBytes());
            assertEquals(ManagedChunkEncoder.State.FAILED, encoder.state());

            encoder.abort(failure);
            assertEquals(0, deflater.finishCalls);
            assertEquals(1, deflater.endCalls);
            assertEquals(0, memory.flushCalls);
            assertEquals(0, memory.closeCalls);
            assertEquals(ManagedChunkEncoder.State.ABORTED, encoder.state());
            assertThrows(IllegalStateException.class, encoder::candidate);
        }
    }

    @Test void serializerFailureAfterPartialBytesAbortsWithoutFinishingFlushingOrClosing() throws Exception {
        SourceLimitProfile profile = profile(4096, 4096, 4096);
        TrackingDeflater deflater = new TrackingDeflater();
        TrackingMemoryOutput memory = new TrackingMemoryOutput(4096);
        IOException encodeFailure = new IOException("original complete-NBT encoder failure");
        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(4096, () -> deflater, ignored -> memory);
            encoder.dataOutput().writeUTF("partial real DataOutput bytes before the serializer fails");
            encoder.abort(encodeFailure);
            assertSame(encodeFailure, encoder.primaryFailure());
            assertEquals(1, deflater.endCalls);
            assertEquals(0, deflater.finishCalls);
            assertEquals(0, memory.flushCalls);
            assertEquals(0, memory.closeCalls);
            assertEquals(ManagedChunkEncoder.State.ABORTED, encoder.state());
            assertThrows(IllegalStateException.class, encoder::candidate);
        }
    }

    @Test void explicitFlushFailureKeepsCauseAndAbortCallsOnlyDeflaterEnd() throws Exception {
        SourceLimitProfile profile = profile(4096, 4096, 4096);
        TrackingDeflater deflater = new TrackingDeflater();
        IOException flushFailure = new IOException("injected bounded-memory flush failure");
        TrackingMemoryOutput memory = new TrackingMemoryOutput(4096);
        memory.flushFailure = flushFailure;
        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(4096, () -> deflater, ignored -> memory);
            encoder.dataOutput().writeInt(0x13579bdf);
            assertSame(flushFailure, assertThrows(IOException.class, encoder::flushEncoding));
            assertEquals(ManagedChunkEncoder.State.FAILED, encoder.state());

            encoder.abort(flushFailure);
            assertEquals(1, deflater.endCalls);
            assertEquals(0, deflater.finishCalls);
            assertEquals(1, memory.flushCalls);
            assertEquals(0, memory.closeCalls);
            assertEquals(ManagedChunkEncoder.State.ABORTED, encoder.state());
        }
    }

    @Test void compressedOutputOverflowFailsWithoutTruncatingOrProducingCandidate() throws Exception {
        SourceLimitProfile profile = profile(64, 24_000, 64);
        TrackingDeflater deflater = new TrackingDeflater();
        TrackingMemoryOutput memory = new TrackingMemoryOutput(64);
        NBTTagCompound root = new NBTTagCompound();
        byte[] entropy = new byte[20_000];
        new Random(0x5eedL).nextBytes(entropy);
        root.setByteArray("Uncompressible", entropy);

        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(64, () -> deflater, ignored -> memory);
            Throwable failure;
            try {
                CompressedStreamTools.write(root, encoder.dataOutput());
                encoder.finishEncoding();
                fail("the compressed candidate must exceed the explicit 64-byte budget");
                return;
            } catch (IOException | IllegalStateException expected) {
                failure = expected;
            }
            assertEquals(ManagedChunkEncoder.State.FAILED, encoder.state());
            assertTrue(memory.count() <= 64);
            assertTrue(memory.capacity() <= 64);
            encoder.abort(failure);
            assertEquals(1, deflater.endCalls);
            assertEquals(ManagedChunkEncoder.State.ABORTED, encoder.state());
            assertThrows(IllegalStateException.class, encoder::candidate);
        }
    }

    @Test void finishFailureIsStickyAndEndsTheOwnedDeflaterExactlyOnce() throws Exception {
        SourceLimitProfile profile = profile(4096, 4096, 4096);
        TrackingDeflater deflater = new TrackingDeflater();
        TrackingMemoryOutput memory = new TrackingMemoryOutput(4096);
        IllegalStateException finishFailure = new IllegalStateException("injected Deflater.finish failure");
        deflater.finishFailure = finishFailure;
        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(4096, () -> deflater, ignored -> memory);
            encoder.dataOutput().writeUTF("complete payload before finish");
            assertSame(finishFailure, assertThrows(IllegalStateException.class, encoder::finishEncoding));
            assertEquals(1, deflater.finishCalls);
            assertEquals(1, deflater.endCalls);
            assertEquals(ManagedChunkEncoder.State.FAILED, encoder.state());
            assertEquals(0, memory.capacity(), "failed finish discards payload after Deflater cleanup succeeds");
            assertThrows(IllegalStateException.class, encoder::finishEncoding);
            encoder.abort(finishFailure);
            assertEquals(ManagedChunkEncoder.State.ABORTED, encoder.state());
            assertEquals(1, deflater.endCalls);
        }
    }

    @Test void cleanupFailurePreservesOriginalCauseAndRetainsTheActualCompressor() throws Exception {
        SourceLimitProfile profile = profile(4096, 4096, 4096);
        TrackingDeflater deflater = new TrackingDeflater();
        IllegalStateException cleanupFailure = new IllegalStateException("injected Deflater.end failure");
        deflater.endFailure = cleanupFailure;
        IOException encodeFailure = new IOException("original NBT encode failure");
        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(4096, () -> deflater, ManagedChunkEncoder.BoundedMemoryOutput::new);
            encoder.dataOutput().writeInt(123);
            IOException reported = assertThrows(IOException.class, () -> encoder.abort(encodeFailure));
            assertSame(encodeFailure, reported.getCause());
            assertTrue(containsIdentity(encodeFailure.getSuppressed(), cleanupFailure));
            ManagedChunkEncoder.RetainedCompressionResources retained = retainedResources(encodeFailure);
            assertTrue(retained.retains(deflater));
            assertEquals(1, deflater.endCalls);
            assertEquals(ManagedChunkEncoder.State.FAILED, encoder.state());
            assertThrows(IllegalStateException.class, () -> encoder.abort(encodeFailure));
            assertEquals(1, deflater.endCalls, "unknown cleanup outcome must not retry end");
        }
    }

    @Test void constructorFailureKeepsTheOriginalAndCleanupCausesWithTheUnreleasedResource() {
        SourceLimitProfile profile = profile(4096, 4096, 4096);
        IllegalArgumentException constructionFailure = new IllegalArgumentException("injected bounded buffer construction failure");
        IllegalStateException cleanupFailure = new IllegalStateException("injected constructor cleanup failure");
        TrackingDeflater deflater = new TrackingDeflater();
        deflater.endFailure = cleanupFailure;
        try (SaveScope scope = new SaveScope(profile)) {
            IllegalArgumentException reported = assertThrows(IllegalArgumentException.class, () ->
                    ManagedChunkEncoder.open(scope.context, scope.attempt, profile, 4096,
                            () -> deflater, ignored -> { throw constructionFailure; }));
            assertSame(constructionFailure, reported);
            assertTrue(containsIdentity(constructionFailure.getSuppressed(), cleanupFailure));
            assertTrue(retainedResources(constructionFailure).retains(deflater));
            assertEquals(1, deflater.endCalls);
        }
    }

    @Test void wrongAttemptWrongThreadAndDuplicateCloseAreRejectedWithoutReusingTheEncoder() throws Exception {
        SourceLimitProfile profile = profile(4096, 4096, 4096);
        TrackingDeflater deflater = new TrackingDeflater();
        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(4096, () -> deflater, ManagedChunkEncoder.BoundedMemoryOutput::new);
            ManagedSourceSaveLedger.Attempt original = scope.attempt;
            scope.context.attempt = scope.otherAttempt();
            assertThrows(IllegalStateException.class, encoder::dataOutput);
            scope.context.attempt = original;

            AtomicReference<Throwable> wrongThread = new AtomicReference<>();
            Thread thread = new Thread(() -> {
                try { encoder.finishEncoding(); }
                catch (Throwable failure) { wrongThread.set(failure); }
            }, "wrong-owner-test-thread");
            thread.start();
            thread.join(2_000);
            assertFalse(thread.isAlive(), "wrong-owner operation must return without an unbounded wait");
            assertInstanceOf(IllegalStateException.class, wrongThread.get());
            assertEquals(ManagedChunkEncoder.State.OPEN, encoder.state());

            DataOutputStream output = encoder.dataOutput();
            output.writeInt(42);
            output.close();
            assertEquals(ManagedChunkEncoder.State.CANDIDATE, encoder.state());
            assertThrows(IOException.class, output::close);
            assertThrows(IllegalStateException.class, encoder::finishEncoding);
            assertEquals(1, deflater.endCalls);
        }
    }

    @Test void deflaterEndFailureDuringFinishDoesNotReturnCandidateOrRetryCleanup() throws Exception {
        SourceLimitProfile profile = profile(4096, 4096, 4096);
        TrackingDeflater deflater = new TrackingDeflater();
        IllegalStateException cleanupFailure = new IllegalStateException("finish-path Deflater.end failure");
        deflater.endFailure = cleanupFailure;
        try (SaveScope scope = new SaveScope(profile)) {
            ManagedChunkEncoder encoder = scope.open(4096, () -> deflater, ManagedChunkEncoder.BoundedMemoryOutput::new);
            encoder.dataOutput().writeInt(7);
            assertSame(cleanupFailure, assertThrows(IllegalStateException.class, encoder::finishEncoding));
            assertTrue(retainedResources(cleanupFailure).retains(deflater));
            assertEquals(1, deflater.finishCalls);
            assertEquals(1, deflater.endCalls);
            assertEquals(ManagedChunkEncoder.State.FAILED, encoder.state());
            assertThrows(IOException.class, () -> encoder.abort(cleanupFailure));
            assertEquals(1, deflater.endCalls);
        }
    }

    private static SourceLimitProfile profile(long maxChunkBytes, long maxDecompressedBytes,
            long maxRetainedBytes) {
        return SourceLimitProfile.of(1_048_576, maxChunkBytes, maxDecompressedBytes,
                64, 200_000, maxRetainedBytes, 1_048_576, 1_048_576);
    }

    private static NBTTagCompound completeNbt() {
        NBTTagCompound root = new NBTTagCompound();
        NBTTagCompound level = new NBTTagCompound();
        root.setTag("Level", level);
        root.setString("UnrelatedModData", "must survive the complete save encoding");
        byte[] blocks = new byte[24_000];
        new Random(0x51a7L).nextBytes(blocks);
        level.setByteArray("Blocks", blocks);
        NBTTagList passengers = new NBTTagList();
        NBTTagCompound pig = new NBTTagCompound();
        pig.setString("id", "minecraft:pig");
        pig.setString("OwnerData", "retained passenger payload");
        passengers.appendTag(pig);
        level.setTag("Passengers", passengers);
        return root;
    }

    private static byte[] serializeNbt(NBTTagCompound root) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            CompressedStreamTools.write(root, output);
        }
        return bytes.toByteArray();
    }

    private static byte[] inflate(byte[] compressed, int maximumBytes) throws Exception {
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(compressed);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] chunk = new byte[1024];
            while (!inflater.finished()) {
                int count = inflater.inflate(chunk);
                if (count > 0) {
                    if (count > maximumBytes - output.size()) throw new IOException("Inflater output exceeds test budget");
                    output.write(chunk, 0, count);
                } else if (inflater.needsDictionary() || inflater.needsInput()) {
                    break;
                } else {
                    throw new IOException("Inflater made no progress on the completed candidate");
                }
            }
            if (!inflater.finished()) throw new IOException("Candidate is not a complete zlib stream");
            if (inflater.getRemaining() != 0) throw new IOException("Candidate contains bytes after the zlib end marker");
            return output.toByteArray();
        } finally {
            inflater.end();
        }
    }

    private static boolean containsIdentity(Throwable[] values, Throwable expected) {
        for (Throwable value : values) if (value == expected) return true;
        return false;
    }

    private static ManagedChunkEncoder.RetainedCompressionResources retainedResources(Throwable primary) {
        for (Throwable value : primary.getSuppressed()) {
            if (value instanceof ManagedChunkEncoder.RetainedCompressionResources retained) return retained;
        }
        fail("failure chain must retain unresolved compression resources");
        throw new AssertionError("unreachable");
    }

    private static final class SaveScope implements AutoCloseable {
        private final Object loader = new Object();
        private final SourceLimitProfile profile;
        private final ManagedSourceSaveLedger ledger;
        private final ManagedSourceSaveLedger.Attempt attempt;
        private final ManagedSaveContext context;
        private int nextSlot = 2;

        private SaveScope(SourceLimitProfile profile) {
            this.profile = profile;
            Thread installer = Thread.currentThread();
            Thread io = new Thread(() -> { }, "unused-managed-save-fixture-io");
            ledger = new ManagedSourceSaveLedger(UUID.randomUUID(), installer, loader, io, profile,
                    new ManagedSourceSaveLedger.Capacity(16, 16, 16));
            attempt = ledger.begin(loader, new ManagedSourceSaveLedger.Slot(1, 1));
            context = ManagedSaveContext.producer(ledger, attempt);
        }

        private ManagedChunkEncoder open(long budget) {
            return ManagedChunkEncoder.open(context, attempt, profile, budget);
        }

        private ManagedChunkEncoder open(long budget, ManagedChunkEncoder.DeflaterFactory deflater,
                ManagedChunkEncoder.BoundedOutputFactory output) {
            return ManagedChunkEncoder.open(context, attempt, profile, budget, deflater, output);
        }

        private ManagedSourceSaveLedger.Attempt otherAttempt() {
            return ledger.begin(loader, new ManagedSourceSaveLedger.Slot(nextSlot++, 1));
        }

        @Override public void close() {
            context.close();
        }
    }

    private static final class TrackingDeflater extends Deflater {
        private int finishCalls;
        private int endCalls;
        private RuntimeException finishFailure;
        private RuntimeException endFailure;

        private TrackingDeflater() {
            super();
        }

        @Override public void finish() {
            finishCalls++;
            super.finish();
            if (finishFailure != null) throw finishFailure;
        }

        @Override public void end() {
            endCalls++;
            super.end();
            if (endFailure != null) throw endFailure;
        }
    }

    private static final class TrackingMemoryOutput extends ManagedChunkEncoder.BoundedMemoryOutput {
        private int flushCalls;
        private int closeCalls;
        private IOException flushFailure;

        private TrackingMemoryOutput(int maximumBytes) {
            super(maximumBytes);
        }

        @Override public void flush() throws IOException {
            flushCalls++;
            if (flushFailure != null) throw flushFailure;
            super.flush();
        }

        @Override public void close() throws IOException {
            closeCalls++;
            super.close();
        }
    }
}
