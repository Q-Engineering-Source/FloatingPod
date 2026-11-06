package com.wi.floatingpod.server;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/** 完整 artifact 的磁盘 DATA 引用；公开数据不能签发 native/SQL/退休能力。 */
public final class B1Full20WitnessReference {
    public static final String DOMAIN = "FP_B1_FULL20_ORIGINAL_WITNESS_BE_V1";
    private final String basename;
    private final WindowsMarkerIo.FileIdentity fileIdentity;
    private final long fullLength;
    private final byte[] fullSha256, sourceAnchorSha256, originalReceiptSha256, profileSha256;
    private final UUID attemptId, logicalOperation;
    private final B1ResumeRecord.Phase phase;
    public B1Full20WitnessReference(String basename, WindowsMarkerIo.FileIdentity fileIdentity, long fullLength,
            byte[] fullSha256, byte[] sourceAnchorSha256, byte[] originalReceiptSha256, byte[] profileSha256,
            UUID attemptId, B1ResumeRecord.Phase phase, UUID logicalOperation) {
        this.fileIdentity=Objects.requireNonNull(fileIdentity);
        this.fullSha256=hash(fullSha256); this.sourceAnchorSha256=hash(sourceAnchorSha256);
        this.originalReceiptSha256=hash(originalReceiptSha256); this.profileSha256=hash(profileSha256);
        this.attemptId=Objects.requireNonNull(attemptId); this.phase=Objects.requireNonNull(phase);
        this.logicalOperation=Objects.requireNonNull(logicalOperation);
        if (fullLength <= 0) throw new IllegalArgumentException("Complete positive artifact length required");
        this.fullLength=fullLength;
        String exact=basename(attemptId, this.fullSha256);
        if (!exact.equals(basename)) throw new IllegalArgumentException("Exact fixed witness basename required");
        this.basename=basename;
    }
    static byte[] hash(byte[] value) {
        if (value==null || value.length!=32) throw new IllegalArgumentException("Exact witness SHA256 required");
        return value.clone();
    }
    public static String basename(UUID attempt, byte[] sha256) {
        Objects.requireNonNull(attempt);
        if (sha256==null || sha256.length!=32) throw new IllegalArgumentException("Exact artifact SHA256 required");
        return ".floatingpod-b1-full20-v1-"+attempt+"-"+HexFormat.of().formatHex(sha256)+".bin";
    }
    public String basename(){return basename;}
    public WindowsMarkerIo.FileIdentity fileIdentity(){return fileIdentity;}
    public long fullLength(){return fullLength;}
    public byte[] fullSha256(){return fullSha256.clone();}
    public byte[] sourceAnchorSha256(){return sourceAnchorSha256.clone();}
    public byte[] originalReceiptSha256(){return originalReceiptSha256.clone();}
    public byte[] profileSha256(){return profileSha256.clone();}
    public UUID attemptId(){return attemptId;}
    public B1ResumeRecord.Phase phase(){return phase;}
    public UUID logicalOperation(){return logicalOperation;}
    public boolean exactlyEquals(B1Full20WitnessReference other) {
        return other!=null && basename.equals(other.basename) && fileIdentity.equals(other.fileIdentity) &&
            fullLength==other.fullLength && Arrays.equals(fullSha256,other.fullSha256) &&
            Arrays.equals(sourceAnchorSha256,other.sourceAnchorSha256) &&
            Arrays.equals(originalReceiptSha256,other.originalReceiptSha256) &&
            Arrays.equals(profileSha256,other.profileSha256) && attemptId.equals(other.attemptId) &&
            phase==other.phase && logicalOperation.equals(other.logicalOperation);
    }
    @Override public String toString(){return "B1Full20WitnessReference[DATA; no native/SQL authority]";}
}
