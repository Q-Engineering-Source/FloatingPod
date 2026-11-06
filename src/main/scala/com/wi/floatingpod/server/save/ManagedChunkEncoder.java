package com.wi.floatingpod.server.save;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Owns one bounded vanilla-compatible zlib encoding attempt. A completed candidate is only compressed
 * bytes; this class has no RegionFile publish, durability permit, undo, or Ready capability.
 */
public final class ManagedChunkEncoder {
    private static final long MAX_JVM_ARRAY_BYTES = Integer.MAX_VALUE - 8L;

    public enum State { OPEN, FINISHING, CANDIDATE, ABORTED, FAILED }

    @FunctionalInterface
    interface DeflaterFactory {
        Deflater create();
    }

    @FunctionalInterface
    interface BoundedOutputFactory {
        BoundedMemoryOutput create(int maximumBytes);
    }

    private final ManagedSaveContext ownerScope;
    private final ManagedSourceSaveLedger.Attempt attempt;
    private final SourceLimitProfile limits;
    private final Thread ownerThread;
    private final int outputBudgetBytes;
    private Deflater compressor;
    private BoundedMemoryOutput compressedBytes;
    private DeflaterOutputStream compressorStream;
    private BufferedOutputStream bufferedEncoder;
    private final ManagedDataOutputStream dataOutput;
    private long submittedInputBytes;
    private volatile State state = State.OPEN;
    private Throwable primaryFailure;
    private Throwable endFailure;
    private boolean compressorEndAttempted;
    private boolean compressorEnded;
    private boolean abortAttempted;
    private EncodedCandidate candidate;

    private ManagedChunkEncoder(ManagedSaveContext ownerScope,
            ManagedSourceSaveLedger.Attempt attempt,
            SourceLimitProfile limits,
            int outputBudgetBytes,
            Deflater compressor,
            BoundedMemoryOutput compressedBytes,
            DeflaterOutputStream compressorStream,
            BufferedOutputStream bufferedEncoder) {
        this.ownerScope = ownerScope;
        this.attempt = attempt;
        this.limits = limits;
        this.ownerThread = Thread.currentThread();
        this.outputBudgetBytes = outputBudgetBytes;
        this.compressor = compressor;
        this.compressedBytes = compressedBytes;
        this.compressorStream = compressorStream;
        this.bufferedEncoder = bufferedEncoder;
        this.dataOutput = new ManagedDataOutputStream(new OwnerOutputStream());
    }

    /**
     * Opens one encoding attempt with an explicit per-attempt compressed-output allowance.
     * The caller must reserve the payload allowance in its retention policy; this method does
     * not reserve bytes globally. Fixed JDK buffers and native Deflater memory are outside this payload-byte cap.
     * The active scope, attempt, and shared profile must be exact objects.
     */
    public static ManagedChunkEncoder open(ManagedSaveContext ownerScope,
            ManagedSourceSaveLedger.Attempt attempt,
            SourceLimitProfile limits,
            long compressedOutputBudgetBytes) {
        return open(ownerScope, attempt, limits, compressedOutputBudgetBytes,
                Deflater::new, BoundedMemoryOutput::new);
    }

    static ManagedChunkEncoder open(ManagedSaveContext ownerScope,
            ManagedSourceSaveLedger.Attempt attempt,
            SourceLimitProfile limits,
            long compressedOutputBudgetBytes,
            DeflaterFactory deflaterFactory,
            BoundedOutputFactory outputFactory) {
        requireOpeningScope(ownerScope, attempt, limits);
        Objects.requireNonNull(deflaterFactory, "Deflater factory is required");
        Objects.requireNonNull(outputFactory, "bounded output factory is required");
        if (compressedOutputBudgetBytes <= 0) {
            throw new IllegalArgumentException("A positive per-attempt compressed-output budget is required");
        }
        if (compressedOutputBudgetBytes > limits.maxChunkBytes()) {
            throw new IllegalArgumentException("Compressed-output budget exceeds the shared maxChunkBytes limit");
        }
        if (compressedOutputBudgetBytes > limits.maxRetainedBytes()) {
            throw new IllegalArgumentException("Compressed-output budget exceeds the shared maxRetainedBytes limit");
        }
        if (compressedOutputBudgetBytes > MAX_JVM_ARRAY_BYTES) {
            throw new IllegalArgumentException("Compressed-output budget exceeds the JVM byte-array representation limit");
        }
        int outputBudget = Math.toIntExact(compressedOutputBudgetBytes);

        Deflater ownedCompressor = Objects.requireNonNull(deflaterFactory.create(),
                "Deflater factory returned null");
        BoundedMemoryOutput bounded = null;
        DeflaterOutputStream compressorOutput = null;
        BufferedOutputStream buffered = null;
        try {
            bounded = Objects.requireNonNull(outputFactory.create(outputBudget),
                    "bounded output factory returned null");
            if (bounded.maximumBytes() != outputBudget) {
                throw new IllegalStateException("Bounded output factory changed the explicit output budget");
            }
            compressorOutput = new DeflaterOutputStream(bounded, ownedCompressor);
            buffered = new BufferedOutputStream(compressorOutput);
            return new ManagedChunkEncoder(ownerScope, attempt, limits, outputBudget,
                    ownedCompressor, bounded, compressorOutput, buffered);
        } catch (Throwable primary) {
            try {
                ownedCompressor.end();
                if (bounded != null) bounded.discardAfterCompressorEnd();
            } catch (Throwable cleanup) {
                addSuppressed(primary, cleanup);
                addSuppressed(primary, new RetainedCompressionResources(
                        ownedCompressor, bounded, compressorOutput, buffered, cleanup));
            }
            throw rethrowOpeningFailure(primary);
        }
    }

    /** Returns the one DataOutput capability for this exact active scope and attempt. */
    public synchronized DataOutputStream dataOutput() {
        requireActiveScope();
        if (state != State.OPEN) throw invalidState("Encoder output is no longer writable");
        return dataOutput;
    }

    /** Flushes Java buffering without requesting Deflater SYNC_FLUSH or closing any downstream stream. */
    public synchronized void flushEncoding() throws IOException {
        requireActiveScope();
        requireOpenForWrite();
        try {
            bufferedEncoder.flush();
        } catch (Throwable failure) {
            rememberFailure(failure);
            rethrow(failure);
        }
    }

    /**
     * Completes the zlib stream, explicitly releases the owned Deflater, and returns immutable bytes.
     * This method never calls RegionFile.write or closes the output chain.
     */
    public synchronized EncodedCandidate finishEncoding() throws IOException {
        requireActiveScope();
        if (state == State.FAILED) rethrow(primaryFailure);
        if (state != State.OPEN) throw invalidState("Encoder finish is terminal and may occur only once");
        state = State.FINISHING;
        try {
            bufferedEncoder.flush();
            compressorStream.finish();
            if (!compressor.finished()) {
                throw new IOException("Deflater did not report a completed zlib stream after finish");
            }
            endCompressor();
            EncodedCandidate completed = new EncodedCandidate(ownerScope, attempt,
                    compressedBytes, submittedInputBytes);
            candidate = completed;
            compressedBytes = null;
            clearStreamChain();
            state = State.CANDIDATE;
            return completed;
        } catch (Throwable failure) {
            rememberFailure(failure);
            endAfterFailure(failure);
            state = State.FAILED;
            rethrow(failure);
            throw new AssertionError("Unreachable after rethrow");
        }
    }

    /**
     * Abandons this attempt. It never flushes, finishes, closes a stream, or publishes bytes.
     * If Deflater.end fails, the original cause and unresolved compressor stay attached to the
     * failure chain and this encoder remains permanently failed.
     */
    public synchronized void abort(Throwable cause) throws IOException {
        requireActiveScope();
        Objects.requireNonNull(cause, "Abort cause is required");
        if (state == State.CANDIDATE || state == State.ABORTED) {
            throw invalidState("A completed or aborted encoder cannot be aborted again");
        }
        if (state == State.FINISHING) throw invalidState("Encoder finish is in progress");
        if (abortAttempted) throw invalidState("Encoder abort is terminal and may occur only once");
        abortAttempted = true;
        rememberFailure(cause);

        if (endFailure != null) {
            throw cleanupFailure("Deflater cleanup previously failed; the compressor remains retained");
        }
        if (compressor != null && !compressorEndAttempted) {
            compressorEndAttempted = true;
            try {
                compressor.end();
                compressor = null;
                compressorEnded = true;
                clearStreamChain();
            } catch (Throwable cleanup) {
                endFailure = cleanup;
                addSuppressed(primaryFailure, cleanup);
                addSuppressed(primaryFailure, retainedResources(cleanup));
                state = State.FAILED;
                throw cleanupFailure("Deflater cleanup failed; the compressor remains retained");
            }
        } else if (compressor != null) {
            // end() already threw. Retrying an operation with unknown native completion is forbidden.
            throw cleanupFailure("Deflater cleanup outcome is unresolved; the compressor remains retained");
        }

        if (!compressorEnded && compressor == null) compressorEnded = true;
        clearStreamChain();
        if (compressedBytes != null) {
            compressedBytes.discardAfterCompressorEnd();
            compressedBytes = null;
        }
        state = State.ABORTED;
    }

    public synchronized EncodedCandidate candidate() {
        requireActiveScope();
        if (state != State.CANDIDATE || candidate == null) {
            throw invalidState("No completed compressed candidate is available");
        }
        return candidate;
    }

    public synchronized State state() {
        return state;
    }

    public synchronized long submittedInputBytes() {
        return submittedInputBytes;
    }

    synchronized Throwable primaryFailure() {
        requireActiveScope();
        return primaryFailure;
    }

    public int outputBudgetBytes() {
        return outputBudgetBytes;
    }

    private synchronized void writeFromOwner(int value) throws IOException {
        requireOpenForWrite();
        if (submittedInputBytes >= limits.maxDecompressedBytes()) {
            IOException failure = new IOException("Encoder input exceeds the shared maxDecompressedBytes limit");
            rememberFailure(failure);
            throw failure;
        }
        submittedInputBytes++;
        try {
            bufferedEncoder.write(value);
        } catch (Throwable failure) {
            rememberFailure(failure);
            rethrow(failure);
        }
    }

    private synchronized void writeFromOwner(byte[] bytes, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        requireOpenForWrite();
        if (length == 0) return;
        if ((long) length > limits.maxDecompressedBytes() - submittedInputBytes) {
            IOException failure = new IOException("Encoder input exceeds the shared maxDecompressedBytes limit");
            rememberFailure(failure);
            throw failure;
        }
        submittedInputBytes += length;
        try {
            bufferedEncoder.write(bytes, offset, length);
        } catch (Throwable failure) {
            rememberFailure(failure);
            rethrow(failure);
        }
    }

    private void requireOpenForWrite() throws IOException {
        requireActiveScope();
        if (state == State.FAILED) rethrow(primaryFailure);
        if (state != State.OPEN) throw invalidState("Encoder is not open for writes");
    }

    private void requireActiveScope() {
        if (Thread.currentThread() != ownerThread
                || ManagedSaveContext.current() != ownerScope
                || ownerScope.attempt != attempt
                || ownerScope.ledger.limits() != limits) {
            throw new IllegalStateException("Exact active save scope, owner thread, attempt, and shared limits are required");
        }
    }

    private static void requireOpeningScope(ManagedSaveContext ownerScope,
            ManagedSourceSaveLedger.Attempt attempt, SourceLimitProfile limits) {
        Objects.requireNonNull(ownerScope, "Active save scope is required");
        Objects.requireNonNull(attempt, "Exact save attempt is required");
        Objects.requireNonNull(limits, "Shared source limits are required");
        if (ManagedSaveContext.current() != ownerScope
                || ownerScope.attempt != attempt
                || ownerScope.ledger.limits() != limits) {
            throw new IllegalStateException("Exact active save scope, attempt, and shared limits are required");
        }
    }

    private void rememberFailure(Throwable failure) {
        if (primaryFailure == null) primaryFailure = failure;
        else if (primaryFailure != failure) addSuppressed(primaryFailure, failure);
        if (state != State.CANDIDATE && state != State.ABORTED) state = State.FAILED;
    }

    private void endCompressor() {
        if (compressor == null || compressorEndAttempted) {
            throw new IllegalStateException("Owned Deflater cleanup was already attempted");
        }
        compressorEndAttempted = true;
        try {
            compressor.end();
            compressor = null;
            compressorEnded = true;
            clearStreamChain();
        } catch (Throwable failure) {
            endFailure = failure;
            addSuppressed(failure, retainedResources(failure));
            rethrowUnchecked(failure);
        }
    }

    private void endAfterFailure(Throwable primary) {
        if (compressor == null || compressorEndAttempted) return;
        compressorEndAttempted = true;
        try {
            compressor.end();
            compressor = null;
            compressorEnded = true;
            clearStreamChain();
            if (compressedBytes != null) {
                compressedBytes.discardAfterCompressorEnd();
                compressedBytes = null;
            }
        } catch (Throwable cleanup) {
            endFailure = cleanup;
            addSuppressed(primary, cleanup);
            addSuppressed(primary, retainedResources(cleanup));
        }
    }

    private RetainedCompressionResources retainedResources(Throwable cleanup) {
        return new RetainedCompressionResources(compressor, compressedBytes,
                compressorStream, bufferedEncoder, cleanup);
    }

    private IOException cleanupFailure(String message) {
        Throwable primary = primaryFailure == null ? endFailure : primaryFailure;
        if (primary == null) primary = new IllegalStateException(message);
        return new IOException(message, primary);
    }

    private static IllegalStateException invalidState(String message) {
        return new IllegalStateException(message);
    }

    private static void addSuppressed(Throwable primary, Throwable additional) {
        if (primary != null && additional != null && primary != additional) {
            for (Throwable present : primary.getSuppressed()) if (present == additional) return;
            primary.addSuppressed(additional);
        }
    }

    private void clearStreamChain() {
        bufferedEncoder = null;
        compressorStream = null;
    }

    private static void rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException io) throw io;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        throw new IOException("Unexpected checked failure in managed chunk encoding", failure);
    }

    private static void rethrowUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        throw new IllegalStateException("Unexpected checked failure in Deflater cleanup", failure);
    }

    private static RuntimeException rethrowOpeningFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        return new IllegalStateException("Unexpected checked failure while opening managed encoder", failure);
    }

    /** Immutable compressed bytes plus lineage identity; deliberately carries no write authority. */
    public static final class EncodedCandidate {
        private final ManagedSaveContext ownerScope;
        private final ManagedSourceSaveLedger.Attempt attempt;
        private final byte[][] segments;
        private final int[] segmentLengths;
        private final int compressedLength;
        private final int retainedCapacityBytes;
        private final long submittedInputBytes;

        private EncodedCandidate(ManagedSaveContext ownerScope,
                ManagedSourceSaveLedger.Attempt attempt,
                BoundedMemoryOutput output,
                long submittedInputBytes) {
            this.ownerScope = ownerScope;
            this.attempt = attempt;
            this.segments = output.copySegmentReferences();
            this.segmentLengths = output.copySegmentLengths();
            this.compressedLength = output.count;
            this.retainedCapacityBytes = output.allocatedBytes;
            this.submittedInputBytes = submittedInputBytes;
            output.transferToCandidate();
        }

        public int compressedLength() { return compressedLength; }
        public int retainedCapacityBytes() { return retainedCapacityBytes; }
        public long submittedInputBytes() { return submittedInputBytes; }
        public boolean belongsTo(ManagedSaveContext actualOwner, ManagedSourceSaveLedger.Attempt actualAttempt) {
            return ownerScope == actualOwner && attempt == actualAttempt;
        }

        /** Read-only segment views over exact payload bytes; no mutable backing array escapes. */
        public List<ByteBuffer> compressedPayload() {
            var views = new ArrayList<ByteBuffer>(segments.length);
            for (int index = 0; index < segments.length; index++) {
                views.add(ByteBuffer.wrap(segments[index], 0, segmentLengths[index])
                        .slice().asReadOnlyBuffer());
            }
            return List.copyOf(views);
        }

        /** Copies into a caller-reserved array; the caller accounts for this additional retained allocation. */
        public void copyTo(byte[] destination, int offset) {
            Objects.checkFromIndexSize(offset, compressedLength, destination.length);
            int targetOffset = offset;
            for (int index = 0; index < segments.length; index++) {
                int length = segmentLengths[index];
                System.arraycopy(segments[index], 0, destination, targetOffset, length);
                targetOffset += length;
            }
        }
    }

    /**
     * Segmented bounded sink. It never grows by copying a prior array, so payload backing capacity
     * stays within the supplied allowance even during encoding and candidate transfer.
     */
    static class BoundedMemoryOutput extends OutputStream {
        private static final int SEGMENT_BYTES = 8192;
        private final int maximumBytes;
        private java.util.ArrayList<byte[]> segments = new java.util.ArrayList<>();
        private java.util.ArrayList<Integer> segmentLengths = new java.util.ArrayList<>();
        private byte[] current;
        private int currentCount;
        private int count;
        private int allocatedBytes;
        private boolean transferred;
        private boolean discarded;

        BoundedMemoryOutput(int maximumBytes) {
            if (maximumBytes <= 0) throw new IllegalArgumentException("Positive compressed-output allowance required");
            this.maximumBytes = maximumBytes;
        }

        final int maximumBytes() { return maximumBytes; }
        final int count() { return count; }
        final int capacity() { return allocatedBytes; }

        @Override public void write(int value) throws IOException {
            requireWritable();
            if (count == maximumBytes) throw overflow();
            ensureCurrentSegment();
            current[currentCount++] = (byte) value;
            count++;
            segmentLengths.set(segmentLengths.size() - 1, currentCount);
        }

        @Override public void write(byte[] bytes, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            requireWritable();
            if (length == 0) return;
            if (length > maximumBytes - count) throw overflow();
            int sourceOffset = offset;
            int remaining = length;
            while (remaining != 0) {
                ensureCurrentSegment();
                int copied = Math.min(remaining, current.length - currentCount);
                System.arraycopy(bytes, sourceOffset, current, currentCount, copied);
                currentCount += copied;
                count += copied;
                sourceOffset += copied;
                remaining -= copied;
                segmentLengths.set(segmentLengths.size() - 1, currentCount);
            }
        }

        @Override public void flush() throws IOException {
            requireWritable();
        }

        @Override public void close() throws IOException {
            throw new IOException("Managed compressed buffer close is forbidden; finish or abort owns it");
        }

        final void discardAfterCompressorEnd() {
            if (!transferred) {
                segments.clear();
                segmentLengths.clear();
                current = null;
                currentCount = 0;
                count = 0;
                allocatedBytes = 0;
                discarded = true;
            }
        }

        private byte[][] copySegmentReferences() {
            return segments.toArray(byte[][]::new);
        }

        private int[] copySegmentLengths() {
            int[] lengths = new int[segmentLengths.size()];
            for (int index = 0; index < lengths.length; index++) lengths[index] = segmentLengths.get(index);
            return lengths;
        }

        private void transferToCandidate() {
            transferred = true;
            segments.clear();
            segmentLengths.clear();
            current = null;
            currentCount = 0;
            count = 0;
            allocatedBytes = 0;
        }

        private void ensureCurrentSegment() throws IOException {
            requireWritable();
            if (current != null && currentCount < current.length) return;
            int remainingAllowance = maximumBytes - allocatedBytes;
            if (remainingAllowance <= 0) throw overflow();
            int segmentBytes = Math.min(SEGMENT_BYTES, remainingAllowance);
            byte[] next = new byte[segmentBytes];
            current = next;
            currentCount = 0;
            allocatedBytes += segmentBytes;
            segments.add(next);
            segmentLengths.add(0);
        }

        private IOException overflow() {
            return new IOException("Compressed output exceeds its explicit bounded-memory allowance");
        }

        private void requireWritable() throws IOException {
            if (transferred || discarded) throw new IOException("Compressed buffer is no longer owned for writing");
        }
    }

    private final class OwnerOutputStream extends OutputStream {
        @Override public void write(int value) throws IOException {
            writeFromOwner(value);
        }

        @Override public void write(byte[] bytes, int offset, int length) throws IOException {
            writeFromOwner(bytes, offset, length);
        }

        @Override public void flush() throws IOException {
            flushEncoding();
        }

        @Override public void close() throws IOException {
            throw new IOException("Internal encoder output cannot close its owned compression chain");
        }
    }

    private final class ManagedDataOutputStream extends DataOutputStream {
        private boolean closeAttempted;

        private ManagedDataOutputStream(OutputStream output) {
            super(output);
        }

        @Override public void flush() throws IOException {
            flushEncoding();
        }

        @Override public void close() throws IOException {
            synchronized (ManagedChunkEncoder.this) {
                requireActiveScope();
                if (closeAttempted) throw new IOException("Managed chunk output close was already attempted");
                closeAttempted = true;
            }
            finishEncoding();
        }
    }

    /** Strongly retains unresolved native and Java resources through the primary failure chain. */
    static final class RetainedCompressionResources extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final Deflater compressor;
        private final BoundedMemoryOutput memory;
        private final DeflaterOutputStream compressorStream;
        private final BufferedOutputStream bufferedStream;
        private final Throwable cleanupFailure;

        private RetainedCompressionResources(Deflater compressor,
                BoundedMemoryOutput memory,
                DeflaterOutputStream compressorStream,
                BufferedOutputStream bufferedStream,
                Throwable cleanupFailure) {
            super("Owned compression resources remain retained after cleanup failure");
            this.compressor = compressor;
            this.memory = memory;
            this.compressorStream = compressorStream;
            this.bufferedStream = bufferedStream;
            this.cleanupFailure = cleanupFailure;
        }

        boolean retains(Deflater actual) {
            return compressor == actual;
        }
    }
}
