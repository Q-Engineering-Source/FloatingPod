package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** 仅 DATA fixture；不构造 private native ACK/退休/JDBC/custody 资格。 */
final class B1HandoffTransferIntentFixture {
    static final SourceLimitProfile PROFILE = SourceLimitProfile.of(
        10_000_000, 10_000_000, 10_000_000, 64, 100, 100_000_000, 10_000_000, 10_000_000);
    static WindowsMarkerIo.FileIdentity id(int number) {
        byte[] b = new byte[16]; b[15] = (byte) number; return new WindowsMarkerIo.FileIdentity(0x1234, b);
    }
    static final class Parts {
        B1ResumeRecord bridge; B1ResumeRecordV5 inactive, current;
        byte[] bridgeBytes, inactiveBytes, currentBytes;
        B1Full20WitnessReference witness; B1HandoffArtifactClosure.Artifact artifact;
    }
    static Parts parts() throws Exception {
        var p = new Parts(); p.bridge = B1FreshPrepareRecordFixture.validRecord();
        p.bridgeBytes = B1ResumeRecordCanonical.encode(p.bridge);
        var s = p.bridge.assertions();
        var state = new B1ResumeRecord.FullStateAssertions(s.currentOwner(), s.previousOwner(), s.retirement(),
            s.pendingAttempt(), s.currentWriter(), s.targetWriter(), s.pendingAttempt(), s.latestSettlement(), s.marker(), null);
        // body 是完整供 closure 检查的 byte DATA；不声称它已通过 full20 semantic codec/SQL qualification。
        byte[] body = new byte[] {9, 8, 7, 6, 5};
        var a = s.pendingAttempt(); byte[] hash = B1HandoffTransferDataCodec.digest(body);
        p.witness = new B1Full20WitnessReference(B1Full20WitnessReference.basename(a.attemptId(), hash), id(9),
            body.length, hash, p.bridge.sourceAnchorSha256(), B1HandoffTransferDataCodec.digest(p.bridge.originalReceipt()),
            PROFILE.sha256(), a.attemptId(), a.phase(), a.logicalWriteUuid());
        p.artifact = new B1HandoffArtifactClosure.Artifact(p.witness, body);
        var refs = new B1ResumeRecordV5.References(p.witness, p.witness, null, null);
        var inactiveTransition = new B1ResumeRecord(1, B1HandoffTransferDataCodec.digest(p.bridgeBytes),
            B1ResumeRecord.Kind.DB_ACQUIRED, p.bridge.sourceAnchorSha256(), p.bridge.originalReceipt(), p.bridge.barrierNamespace(), state);
        p.inactive = new B1ResumeRecordV5(inactiveTransition, 0, B1HandoffTransferDataCodec.digest(p.bridgeBytes),
            id(4), B1HandoffTransferDataCodec.digest(p.bridgeBytes), refs);
        p.inactiveBytes = B1ResumeRecordV5.encodeData(p.inactive);
        var currentTransition = new B1ResumeRecord(2, B1HandoffTransferDataCodec.digest(B1ResumeRecordCanonical.encode(inactiveTransition)),
            B1ResumeRecord.Kind.DB_ACQUIRED, p.bridge.sourceAnchorSha256(), p.bridge.originalReceipt(), p.bridge.barrierNamespace(), state);
        p.current = new B1ResumeRecordV5(currentTransition, 0, B1HandoffTransferDataCodec.digest(p.bridgeBytes),
            id(4), B1HandoffTransferDataCodec.digest(p.inactiveBytes), refs);
        p.currentBytes = B1ResumeRecordV5.encodeData(p.current); return p;
    }
    static B1HandoffTransferIntentCodec.Inventory inventory(Parts p, List<B1HandoffArtifactClosure.Artifact> artifacts) throws Exception {
        return new B1HandoffTransferIntentCodec.Inventory(PROFILE, p.bridge.sourceAnchorSha256(), id(1), id(2), id(3),
            B1SourceAnchorFixture.WORLD, p.bridgeBytes, id(4), p.currentBytes, id(6), p.inactiveBytes, id(5), List.of(), artifacts);
    }
    static B1HandoffTransferIntentCodec.NativeArtifact nativeArtifact(B1HandoffGenerationName.Family family,
            String key, long g, long t, int identity) throws Exception {
        String name = family == B1HandoffGenerationName.Family.TERMINAL ?
            B1HandoffGenerationName.attempt(family, key, g).basename() :
            B1HandoffGenerationName.transfer(family, key, g, t).basename();
        byte[] body = new byte[] {1, 2, 3, (byte) identity};
        var ref = new B1HandoffTransferDataCodec.Reference(name, id(identity), body.length, B1HandoffTransferDataCodec.digest(body));
        return new B1HandoffTransferIntentCodec.NativeArtifact(ref, body);
    }
    static byte[] owner(byte[] receipt, long pid, long boot) {
        ByteBuffer b = ByteBuffer.allocate(164).order(ByteOrder.BIG_ENDIAN);
        b.putLong(WindowsWorldProvisioner.GUARD_MAGIC).putInt(1);
        for (var identity : List.of(id(1), id(2))) b.putLong(identity.volumeSerialNumber()).put(identity.fileId128());
        b.put(B1HandoffTransferDataCodec.digest(receipt)).putLong(pid).putLong(B1SourceAnchorFixture.FILETIME);
        b.putLong(0).putLong(boot).putLong(1);
        b.put(B1HandoffTransferDataCodec.digest(Arrays.copyOf(b.array(), 132))); return b.array();
    }
    static B1HandoffTransferIntentCodec.IntentData intent(long t) throws Exception {
        var p = parts(); var inventory = inventory(p, List.of(p.artifact));
        return new B1HandoffTransferIntentCodec.IntentData(new B1HandoffTransferIntentCodec.OriginalBirthPredecessorData(inventory, 3), inventory, 3, t,
            nativeArtifact(B1HandoffGenerationName.Family.TERMINAL, inventory.baseKey(), 3, -1, 7),
            t == 0 ? null : nativeArtifact(B1HandoffGenerationName.Family.TRANSFER_SUPERSESSION, inventory.baseKey(), 3, t - 1, 8),
            owner(p.bridge.originalReceipt(), 100, 10), owner(p.bridge.originalReceipt(), 200, 20),
            "ORIGINAL_RETIREMENT_DATA_PROTOCOL", new byte[] {4, 5, 6});
    }
}
