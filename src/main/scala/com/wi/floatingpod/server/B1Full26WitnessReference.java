package com.wi.floatingpod.server;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/** 完整 Full26 artifact 的不可变 DATA 引用；不授予 native、SQL 或运行资格。 */
public final class B1Full26WitnessReference {
    public static final String DOMAIN = "FP_B1_FULL26_ORIGINAL_WITNESS_BE_V1";
    private final String basename;
    private final WindowsMarkerIo.FileIdentity fileIdentity;
    private final long fullLength;
    private final byte[] fullSha256, sourceAnchorSha256, originalReceiptSha256;
    private final byte[] sourceLimitProfileSha256, configurationSha256, ddlSha256;
    private final UUID attemptId, logicalOperation;
    private final B1ResumeRecord.Phase phase;

    public B1Full26WitnessReference(String basename, WindowsMarkerIo.FileIdentity fileIdentity, long fullLength,
            byte[] fullSha256, byte[] sourceAnchorSha256, byte[] originalReceiptSha256,
            byte[] sourceLimitProfileSha256, byte[] configurationSha256, byte[] ddlSha256,
            UUID attemptId, B1ResumeRecord.Phase phase, UUID logicalOperation) {
        this.fileIdentity = Objects.requireNonNull(fileIdentity);
        if (fullLength <= 0) throw new IllegalArgumentException("Positive complete Full26 artifact length required");
        this.fullLength = fullLength;
        this.fullSha256 = hash(fullSha256); this.sourceAnchorSha256 = hash(sourceAnchorSha256);
        this.originalReceiptSha256 = hash(originalReceiptSha256);
        this.sourceLimitProfileSha256 = hash(sourceLimitProfileSha256);
        this.configurationSha256 = hash(configurationSha256); this.ddlSha256 = hash(ddlSha256);
        this.attemptId = nonzero(attemptId); this.logicalOperation = nonzero(logicalOperation);
        this.phase = Objects.requireNonNull(phase);
        if (!basename(attemptId, this.fullSha256).equals(basename))
            throw new IllegalArgumentException("Exact fixed Full26 witness basename required");
        this.basename = basename;
    }
    static byte[] hash(byte[] value) {
        if (value == null || value.length != 32) throw new IllegalArgumentException("Exact Full26 SHA256 required");
        return value.clone();
    }
    private static UUID nonzero(UUID id) {
        Objects.requireNonNull(id);
        if (id.getMostSignificantBits() == 0 && id.getLeastSignificantBits() == 0)
            throw new IllegalArgumentException("Nonzero Full26 reference UUID required");
        return id;
    }
    public static String basename(UUID attempt, byte[] sha256) {
        return ".floatingpod-b1-full26-v1-" + nonzero(attempt) + "-" + HexFormat.of().formatHex(hash(sha256)) + ".bin";
    }
    public String basename() { return basename; }
    public WindowsMarkerIo.FileIdentity fileIdentity() { return fileIdentity; }
    public long fullLength() { return fullLength; }
    public byte[] fullSha256() { return fullSha256.clone(); }
    public byte[] sourceAnchorSha256() { return sourceAnchorSha256.clone(); }
    public byte[] originalReceiptSha256() { return originalReceiptSha256.clone(); }
    public byte[] sourceLimitProfileSha256() { return sourceLimitProfileSha256.clone(); }
    public byte[] configurationSha256() { return configurationSha256.clone(); }
    public byte[] ddlSha256() { return ddlSha256.clone(); }
    public UUID attemptId() { return attemptId; }
    public B1ResumeRecord.Phase phase() { return phase; }
    public UUID logicalOperation() { return logicalOperation; }
    public boolean exactlyEquals(B1Full26WitnessReference other) {
        return other != null && basename.equals(other.basename) && fileIdentity.equals(other.fileIdentity)
            && fullLength == other.fullLength && Arrays.equals(fullSha256, other.fullSha256)
            && Arrays.equals(sourceAnchorSha256, other.sourceAnchorSha256)
            && Arrays.equals(originalReceiptSha256, other.originalReceiptSha256)
            && Arrays.equals(sourceLimitProfileSha256, other.sourceLimitProfileSha256)
            && Arrays.equals(configurationSha256, other.configurationSha256) && Arrays.equals(ddlSha256, other.ddlSha256)
            && attemptId.equals(other.attemptId) && phase == other.phase && logicalOperation.equals(other.logicalOperation);
    }
    @Override public String toString() { return "B1Full26WitnessReference[DATA; no native/SQL authority]"; }
}
