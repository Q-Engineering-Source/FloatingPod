package com.wi.floatingpod.server;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.*;

/** V5明确完整artifact引用；DATA本身不能签 native/SQL/退休。V4协议不变。 */
public final class B1ResumeRecordV5 {
    public static final String DOMAIN="FP_B1_RESUME_BINARY_BE_V5";
    public static final int MAX_RECORD_BYTES=65536;
    public record References(B1Full20WitnessReference originalPrepare,B1Full20WitnessReference pending,
            B1Full20WitnessReference settled,B1Full20WitnessReference acknowledgedCertify) { }
    private final B1ResumeRecord transition;
    private final long bridgeSequence;
    private final byte[] bridgeSha256,previousContainerSha256;
    private final WindowsMarkerIo.FileIdentity bridgeIdentity;
    private final References references;
    public B1ResumeRecordV5(B1ResumeRecord transition,long bridgeSequence,byte[] bridgeSha256,
            WindowsMarkerIo.FileIdentity bridgeIdentity,byte[] previousContainerSha256,References references) {
        this.transition=Objects.requireNonNull(transition);
        if(bridgeSequence<0||transition.sequence()<=bridgeSequence)throw new IllegalArgumentException("V5 must continue its original V4 sequence");
        this.bridgeSequence=bridgeSequence;this.bridgeSha256=B1Full20WitnessReference.hash(bridgeSha256);
        this.bridgeIdentity=Objects.requireNonNull(bridgeIdentity);
        this.previousContainerSha256=B1Full20WitnessReference.hash(previousContainerSha256);
        this.references=Objects.requireNonNull(references);validateReferences();
    }
    public B1ResumeRecord transition(){return transition;}
    public long bridgeSequence(){return bridgeSequence;}
    public byte[] bridgeSha256(){return bridgeSha256.clone();}
    public WindowsMarkerIo.FileIdentity bridgeIdentity(){return bridgeIdentity;}
    public byte[] previousContainerSha256(){return previousContainerSha256.clone();}
    public References references(){return references;}
    private void validateReferences() {
        var s=transition.assertions();
        requireRef(s.originalPrepareWitness(),references.originalPrepare());requireRef(s.pendingAttempt(),references.pending());
        requireRef(s.latestSettlement()==null?null:s.latestSettlement().settledAttempt(),references.settled());
        requireRef(s.acknowledgedCertifyWitness(),references.acknowledgedCertify());
        var identities=new HashMap<UUID,B1Full20WitnessReference>();
        for(var ref:allReferences()) {var previous=identities.putIfAbsent(ref.attemptId(),ref);
            if(previous!=null&&!previous.exactlyEquals(ref))throw new IllegalArgumentException("Same attempt has conflicting complete artifact reference");}
    }
    public List<B1Full20WitnessReference> allReferences(){
        var result=new ArrayList<B1Full20WitnessReference>(4);
        for(var r:new B1Full20WitnessReference[]{references.originalPrepare(),references.pending(),references.settled(),references.acknowledgedCertify()})
            if(r!=null)result.add(r);
        return List.copyOf(result);
    }
    private void requireRef(B1ResumeRecord.AttemptAssertions attempt,B1Full20WitnessReference ref) {
        if((attempt==null)!=(ref==null))throw new IllegalArgumentException("Every retained attempt requires its full20 reference");
        if(ref==null)return;
        if(!attempt.attemptId().equals(ref.attemptId())||attempt.phase()!=ref.phase()||!attempt.logicalWriteUuid().equals(ref.logicalOperation())||
            !Arrays.equals(transition.sourceAnchorSha256(),ref.sourceAnchorSha256())||
            !Arrays.equals(WindowsWorldProvisioner.digest(transition.originalReceipt()),ref.originalReceiptSha256()))
            throw new IllegalArgumentException("V5 reference differs from exact original attempt/source");
    }
    public static byte[] encodeData(B1ResumeRecordV5 value)throws IOException {
        byte[] transition=B1ResumeRecordCodec.encode(value.transition);
        var sink=new Bounded();try(var out=new DataOutputStream(sink)) {
            text(out,DOMAIN);out.writeLong(value.bridgeSequence);out.write(value.bridgeSha256);
            identity(out,value.bridgeIdentity);out.write(value.previousContainerSha256);
            out.writeInt(transition.length);out.write(transition);
            reference(out,value.references.originalPrepare());reference(out,value.references.pending());
            reference(out,value.references.settled());reference(out,value.references.acknowledgedCertify());
        }
        return sink.toByteArray();
    }
    public static B1ResumeRecordV5 decodeData(byte[] bytes)throws IOException {
        if(bytes==null||bytes.length==0||bytes.length>MAX_RECORD_BYTES)throw new IOException("V5 record bound");
        try(var in=new DataInputStream(new ByteArrayInputStream(bytes))) {
            if(!DOMAIN.equals(text(in,128)))throw new IOException("Unknown V5 domain");
            long bridge=in.readLong();byte[] sha=read(in,32);var identity=identity(in);byte[] previous=read(in,32);
            int n=in.readInt();if(n<=0||n>MAX_RECORD_BYTES)throw new IOException("V5 transition bound");
            var transition=B1ResumeRecordCodec.decode(read(in,n));
            var refs=new References(reference(in),reference(in),reference(in),reference(in));
            if(in.read()!=-1)throw new IOException("V5 trailing bytes");
            return new B1ResumeRecordV5(transition,bridge,sha,identity,previous,refs);
        } catch(IllegalArgumentException failure){throw new IOException("Invalid V5 DATA",failure);}
    }
    static void consecutive(B1ResumeRecordV5 older,byte[] olderBytes,B1ResumeRecordV5 newer)throws IOException {
        if(older.bridgeSequence!=newer.bridgeSequence||!older.bridgeIdentity.equals(newer.bridgeIdentity)||
            !Arrays.equals(older.bridgeSha256,newer.bridgeSha256))throw new IOException("V5 original handoff bridge changed");
        B1ResumeRecordCodec.exact(WindowsWorldProvisioner.digest(olderBytes),newer.previousContainerSha256,"V5 container predecessor changed");
        B1ResumeRecordCodec.consecutive(older.transition,B1ResumeRecordCodec.encode(older.transition),newer.transition);
        preserve(older.references.originalPrepare(),newer.references.originalPrepare());
        preserve(older.references.acknowledgedCertify(),newer.references.acknowledgedCertify());
    }
    private static void preserve(B1Full20WitnessReference old,B1Full20WitnessReference next)throws IOException {
        if(old!=null&&!old.exactlyEquals(next))throw new IOException("V5 original retained reference removed/changed");
    }
    private static void reference(DataOutputStream out,B1Full20WitnessReference ref)throws IOException {
        out.writeBoolean(ref!=null);if(ref==null)return;
        text(out,ref.basename());identity(out,ref.fileIdentity());out.writeLong(ref.fullLength());
        out.write(ref.fullSha256());out.write(ref.sourceAnchorSha256());out.write(ref.originalReceiptSha256());out.write(ref.profileSha256());
        uuid(out,ref.attemptId());out.writeInt(ref.phase().ordinal());uuid(out,ref.logicalOperation());
    }
    private static B1Full20WitnessReference reference(DataInputStream in)throws IOException {
        int present=in.readUnsignedByte();if(present==0)return null;if(present!=1)throw new IOException("V5 optional tag");
        String name=text(in,256);var id=identity(in);long length=in.readLong();
        byte[] sha=read(in,32),source=read(in,32),receipt=read(in,32),profile=read(in,32);
        UUID attempt=uuid(in);int phase=in.readInt();if(phase<0||phase>=B1ResumeRecord.Phase.values().length)throw new IOException("V5 phase");
        return new B1Full20WitnessReference(name,id,length,sha,source,receipt,profile,attempt,B1ResumeRecord.Phase.values()[phase],uuid(in));
    }
    private static void identity(DataOutputStream out,WindowsMarkerIo.FileIdentity id)throws IOException {out.writeLong(id.volumeSerialNumber());out.write(id.fileId128());}
    private static WindowsMarkerIo.FileIdentity identity(DataInputStream in)throws IOException {return new WindowsMarkerIo.FileIdentity(in.readLong(),read(in,16));}
    private static void uuid(DataOutputStream out,UUID id)throws IOException {out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}
    private static UUID uuid(DataInputStream in)throws IOException {return new UUID(in.readLong(),in.readLong());}
    private static void text(DataOutputStream out,String value)throws IOException {byte[] b=value.getBytes(StandardCharsets.UTF_8);if(b.length>256)throw new IOException("V5 text bound");out.writeInt(b.length);out.write(b);}
    private static String text(DataInputStream in,int limit)throws IOException {int n=in.readInt();if(n<=0||n>limit)throw new IOException("V5 text bound");
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(read(in,n))).toString();}
    private static byte[] read(DataInputStream in,int length)throws IOException {if(length>in.available())throw new EOFException("Truncated V5 framing");return in.readNBytes(length);}
    private static final class Bounded extends ByteArrayOutputStream {
        private void reserve(int count)throws IOException {if(count<0||count>MAX_RECORD_BYTES-size())throw new IOException("V5 complete record exceeds fixed bound");}
        @Override public synchronized void write(int value){try{reserve(1);}catch(IOException e){throw new UncheckedIOException(e);}super.write(value);}
        @Override public synchronized void write(byte[] value,int offset,int count){try{reserve(count);}catch(IOException e){throw new UncheckedIOException(e);}super.write(value,offset,count);}
    }
}
