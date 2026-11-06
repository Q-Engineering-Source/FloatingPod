package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlSessionBarrierNamespace;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import static com.wi.floatingpod.server.B1ResumeRecord.*;

/** 完整 bounded BE binary codec；只验证结构/一致性，不将 caller assertions 升格为真实 SQL proof。 */
final class B1ResumeRecordCodec {
    static final long MAGIC = 0x4650423152535634L;
    static final int MAX_BYTES = 65536;
    private enum ReceiptFamily { LEGACY, SCHEMA5_DATA }
    static byte[] encode(B1ResumeRecord r) throws IOException {return encode(r,ReceiptFamily.LEGACY);}
    static byte[] encodeSchema5Data(B1ResumeRecord r) throws IOException {return encode(r,ReceiptFamily.SCHEMA5_DATA);}
    private static byte[] encode(B1ResumeRecord r,ReceiptFamily family) throws IOException {
        validate(r,family);
        var body = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(body)) {
            text(out, "FP_B1_RESUME_BINARY_BE_V4"); out.writeLong(r.sequence()); out.write(r.previousRecordSha256());
            text(out, r.kind().name()); out.write(r.sourceAnchorSha256()); blob(out, r.originalReceipt());
            out.write(WindowsWorldProvisioner.digest(r.originalReceipt())); text(out, r.barrierNamespace()); state(out, r.assertions());
        }
        byte[] payload = body.toByteArray(); if (payload.length > MAX_BYTES - 48) throw new IOException("Full B1 state exceeds 65536 byte bound");
        var result = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(result)) {
            out.writeLong(MAGIC); out.writeInt(4); out.writeInt(payload.length); out.write(payload); out.write(WindowsWorldProvisioner.digest(payload));
        }
        return result.toByteArray();
    }
    static B1ResumeRecord decode(byte[] original) throws IOException {return decode(original,ReceiptFamily.LEGACY);}
    static B1ResumeRecord decodeSchema5Data(byte[] original) throws IOException {return decode(original,ReceiptFamily.SCHEMA5_DATA);}
    private static B1ResumeRecord decode(byte[] original,ReceiptFamily family) throws IOException {
        if (original == null || original.length < 48 || original.length > MAX_BYTES) throw new IOException("Invalid full B1 record size");
        byte[] input = original.clone();
        try (var in = new DataInputStream(new ByteArrayInputStream(input))) {
            if (in.readLong() != MAGIC || in.readInt() != 4) throw new IOException("Unknown B1 record protocol/version");
            int size = in.readInt(); if (size <= 0 || size != input.length - 48) throw new IOException("Invalid full B1 record length");
            byte[] payload = in.readNBytes(size); exact(in.readNBytes(32), WindowsWorldProvisioner.digest(payload), "B1 payload hash conflict");
            if (in.read() != -1) throw new IOException("Trailing B1 record bytes");
            B1ResumeRecord result;
            try (var fields = new DataInputStream(new ByteArrayInputStream(payload))) {
                if (!"FP_B1_RESUME_BINARY_BE_V4".equals(text(fields))) throw new IOException("Unknown B1 record payload");
                long sequence = fields.readLong(); byte[] previous = hash(fields); Kind kind = Kind.valueOf(text(fields));
                byte[] source = hash(fields), receipt = blob(fields); exact(hash(fields), WindowsWorldProvisioner.digest(receipt), "Original receipt hash conflict");
                result = new B1ResumeRecord(sequence, previous, kind, source, receipt, text(fields), state(fields));
                if (fields.read() != -1) throw new IOException("Trailing B1 record fields");
            }
            exact(input, encode(result,family), "Noncanonical full B1 record"); return result;
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException failure) { throw new IOException("Malformed complete B1 record identity", failure); }
    }
    static void validate(B1ResumeRecord r) throws IOException {validate(r,ReceiptFamily.LEGACY);}
    static void validateSchema5Data(B1ResumeRecord r) throws IOException {validate(r,ReceiptFamily.SCHEMA5_DATA);}
    private static void validate(B1ResumeRecord r,ReceiptFamily family) throws IOException {
        if(family==ReceiptFamily.LEGACY && r.kind()==Kind.PREPARE_SUCCESSOR_INTENT)
            throw new IOException("Successor PREPARE belongs exclusively to schema5 DATA/V6");
        var original=WindowsWorldProvisionReceiptDataCodec.decode(r.originalReceipt());var p=original.plan();
        if(family==ReceiptFamily.LEGACY && p.schemaFormat()!=2 && p.schemaFormat()!=3 ||
            family==ReceiptFamily.SCHEMA5_DATA && p.schemaFormat()!=5)
            throw new IOException("B1 receipt schema family differs from selected DATA entry");
        String expectedNamespace = MySqlSessionBarrierNamespace.forIdentity(p.mysqlServerUuid(), p.schema(), p.provisioningUuid(), p.bindingUuid()).lockName();
        if (!expectedNamespace.equals(r.barrierNamespace())) throw new IOException("B1 namespace differs from original fixed plan");
        FullStateAssertions s = r.assertions();
        if (!s.currentWriter().binding().equals(p.bindingUuid()) || !s.targetWriter().binding().equals(p.bindingUuid()))
            throw new IOException("Full W state differs from fixed binding");
        if (r.kind() == Kind.GENESIS && (s.pendingAttempt() == null || s.pendingAttempt().phase() != Phase.PREPARE))
            throw new IOException("Genesis requires complete first prepare planning assertions");
        if (r.kind() == Kind.ATTEMPT_CANCELLED && (s.latestSettlement() == null || s.latestSettlement().kind() != SettlementKind.CANCELLED))
            throw new IOException("Cancelled record requires complete cancelled settlement assertions");
        if ((r.kind() == Kind.NATIVE_ACK_CERTIFY_INTENT || r.kind() == Kind.CERTIFIED) && s.marker() == null)
            throw new IOException("Marker-bearing phase requires complete marker assertions");
        for (var a : new AttemptAssertions[]{s.originalPrepareWitness(), s.pendingAttempt(), s.latestSettlement()==null?null:s.latestSettlement().settledAttempt(), s.acknowledgedCertifyWitness()}) {
            if (a == null) continue;
            if(family==ReceiptFamily.SCHEMA5_DATA){requireFull26DataGraph(a.beforeGraph());requireFull26DataGraph(a.targetGraph());}
            if (!a.namespace().equals(r.barrierNamespace()) || !a.context().binding().equals(p.bindingUuid()) ||
                !a.beforeWriter().binding().equals(p.bindingUuid()) || !a.targetWriter().binding().equals(p.bindingUuid()))
                throw new IOException("Attempt source/binding namespace conflict");
            if (a.phase() == Phase.PREPARE && !a.logicalWriteUuid().equals(p.prepareWriteUuid()) ||
                a.phase() == Phase.CERTIFY && !a.logicalWriteUuid().equals(p.certifyWriteUuid()))
                throw new IOException("Fixed prepare/certify phase UUID changed");
        }
        if (r.kind() != Kind.GENESIS && r.kind() != Kind.ATTEMPT_CANCELLED &&
                r.kind()!=Kind.PREPARE_SUCCESSOR_INTENT && s.originalPrepareWitness() == null)
            throw new IOException("Post-prepare record must retain full original prepare witness");
        if(r.kind()==Kind.PREPARE_SUCCESSOR_INTENT)requireSuccessorIntent(r);
        if(family==ReceiptFamily.SCHEMA5_DATA && r.kind()==Kind.PREPARED_CONFIRMED)
            requireConfirmedPrepareState(s.latestSettlement()==null?null:s.latestSettlement().settledAttempt(),s);
        if (s.originalPrepareWitness() != null && s.originalPrepareWitness().phase() != Phase.PREPARE)
            throw new IOException("Original prepare witness must preserve prepare scope");
        if ((r.kind()==Kind.GENESIS || r.kind()==Kind.PREPARED_CONFIRMED || r.kind()==Kind.NATIVE_ACK_CERTIFY_INTENT)
                && s.acknowledgedCertifyWitness()!=null) throw new IOException("Pre-certification record cannot contain acknowledged certify witness");
        if (r.kind()==Kind.NATIVE_ACK_CERTIFY_INTENT) {
            requireCertifyWitness(s.pendingAttempt());
            exact(writerBytes(s.currentWriter()),writerBytes(s.pendingAttempt().beforeWriter()),"Certify intent current W differs from full before");
            exact(writerBytes(s.targetWriter()),writerBytes(s.pendingAttempt().targetWriter()),"Certify intent target W differs from full target");
        }
        if (s.acknowledgedCertifyWitness()!=null) requireCertifyWitness(s.acknowledgedCertifyWitness());
        if (r.kind()==Kind.CERTIFIED) {
            if(s.pendingAttempt()!=null)throw new IOException("CERTIFIED cannot retain an unresolved pending attempt");
            requireCertifyWitness(s.acknowledgedCertifyWitness());
            exact(writerBytes(s.currentWriter()),writerBytes(s.acknowledgedCertifyWitness().targetWriter()),"CERTIFIED current W differs from acknowledged target");
            exact(writerBytes(s.targetWriter()),writerBytes(s.acknowledgedCertifyWitness().targetWriter()),"CERTIFIED target W differs from acknowledged target");
        }
        if (s.marker() != null) {
            WindowsWorldProvisioner.validateIdentityText(s.marker().fileIdentity()); WindowsWorldProvisioner.validateIdentityText(s.marker().parentIdentity());
            exact(s.marker().fullMarkerBytes(), WindowsMarkerIo.markerBytes(p.bindingUuid(), p.physicalFormat()), "Marker bytes differ from fixed source binding");
        }
    }
    private static void requireFull26DataGraph(CommonGraphAssertions graph)throws IOException {
        if(!"floatingpod.mysql.b1-manifest/v1".equals(graph.manifestEncoding()) ||
            !"floatingpod.mysql.b1-binding/v1".equals(graph.bindingEncoding()) ||
            !"floatingpod.mysql.script-publication1-deployment-receipt1-repository-commitment/v1".equals(graph.repositoryEncoding()))
            throw new IOException("Schema5 B1 DATA requires both actual Full26 common graph type families");
        // This gate does not decode or qualify the complete Full26 payload; the artifact semantic consumer owns that check.
    }
    private static void requireCertifyWitness(AttemptAssertions a)throws IOException {
        if(a==null||a.phase()!=Phase.CERTIFY)throw new IOException("Complete Phase.CERTIFY witness required");
        var w=a.targetWriter();var slot=w.lastSlot();var c=a.context();
        if(slot==null||!c.boot().equals(w.ownerBoot())||c.writerEpoch()!=w.epoch()||!c.binding().equals(w.binding())
                ||!slot.boot().equals(c.boot())||slot.writerEpoch()!=c.writerEpoch()||!slot.binding().equals(c.binding())
                ||!slot.operation().equals(c.operation()))throw new IOException("Certify context and complete target W receipt differ");
        exact(slot.fingerprint(),c.originalOperationFingerprint(),"Certify target receipt fingerprint differs from original context");
    }
    private static byte[] writerBytes(WriterAssertions w)throws IOException {
        var bytes=new ByteArrayOutputStream();try(var out=new DataOutputStream(bytes)){writer(out,w);}return bytes.toByteArray();
    }
    private static void state(DataOutputStream o, FullStateAssertions s) throws IOException {
        owner(o,s.currentOwner()); optional(o,s.previousOwner(),B1ResumeRecordCodec::owner); optional(o,s.retirement(),(a,r)->{text(a,r.protocol());blob(a,r.originalEvidence());});
        optional(o,s.originalPrepareWitness(),B1ResumeRecordCodec::attempt); writer(o,s.currentWriter());writer(o,s.targetWriter());
        optional(o,s.pendingAttempt(),B1ResumeRecordCodec::attempt);
        optional(o,s.latestSettlement(),(a,r)->{text(a,r.kind().name());attempt(a,r.settledAttempt());text(a,r.proofProtocol());blob(a,r.fullOriginalEvidence());a.write(r.originalUnknownDiagnosticIdentity());});
        optional(o,s.marker(),(a,r)->{blob(a,r.fullMarkerBytes());text(a,r.fileIdentity());text(a,r.parentIdentity());text(a,r.nativeAckProtocol());a.writeLong(r.custodyGeneration());blob(a,r.originalNativeAckEvidence());});
        optional(o,s.acknowledgedCertifyWitness(),B1ResumeRecordCodec::attempt);
    }
    private static FullStateAssertions state(DataInputStream i) throws IOException {
        var current=owner(i);var previous=optional(i,B1ResumeRecordCodec::owner);
        var retired=optional(i,a->new RetirementAssertions(text(a),blob(a)));
        var prepare=optional(i,B1ResumeRecordCodec::attempt);var before=writer(i);var target=writer(i);var pending=optional(i,B1ResumeRecordCodec::attempt);
        var settlement=optional(i,a->new SettlementAssertions(SettlementKind.valueOf(text(a)),attempt(a),text(a),blob(a),hash(a)));
        var marker=optional(i,a->new MarkerAssertions(blob(a),text(a),text(a),text(a),a.readLong(),blob(a)));
        var acknowledged=optional(i,B1ResumeRecordCodec::attempt);
        return new FullStateAssertions(current,previous,retired,prepare,before,target,pending,settlement,marker,acknowledged);
    }
    private static void owner(DataOutputStream o,NativeOwnerAssertions r)throws IOException {o.writeLong(r.pid());o.writeLong(r.fileTime());uuid(o,r.boot());o.writeLong(r.generation());}
    private static NativeOwnerAssertions owner(DataInputStream i)throws IOException{return new NativeOwnerAssertions(i.readLong(),i.readLong(),uuid(i),i.readLong());}
    private static void writer(DataOutputStream o,WriterAssertions w)throws IOException {
        uuid(o,w.binding());o.writeLong(w.epoch());optional(o,w.ownerBoot(),B1ResumeRecordCodec::uuid);time(o,w.leaseUntil());
        optional(o,w.lastSlot(),(a,r)->{uuid(a,r.boot());a.writeLong(r.writerEpoch());uuid(a,r.binding());uuid(a,r.operation());a.write(r.fingerprint());});
    }
    private static WriterAssertions writer(DataInputStream i)throws IOException{return new WriterAssertions(uuid(i),i.readLong(),optional(i,B1ResumeRecordCodec::uuid),time(i),
        optional(i,a->new ReceiptSlotAssertions(uuid(a),a.readLong(),uuid(a),uuid(a),hash(a))));}
    private static void attempt(DataOutputStream o,AttemptAssertions r)throws IOException {
        uuid(o,r.attemptId());text(o,r.phase().name());uuid(o,r.logicalWriteUuid());o.writeLong(r.bootGeneration());uuid(o,r.connectionGeneration());o.writeLong(r.connectionId());text(o,r.namespace());
        operation(o,r.context());writer(o,r.beforeWriter());writer(o,r.targetWriter());graph(o,r.beforeGraph());graph(o,r.targetGraph());
    }
    private static AttemptAssertions attempt(DataInputStream i)throws IOException{return new AttemptAssertions(uuid(i),Phase.valueOf(text(i)),uuid(i),i.readLong(),uuid(i),i.readLong(),text(i),operation(i),writer(i),writer(i),graph(i),graph(i));}
    private static void operation(DataOutputStream o,OperationAssertions r)throws IOException {
        uuid(o,r.boot());o.writeLong(r.writerEpoch());uuid(o,r.binding());uuid(o,r.operation());o.writeInt(r.effects().size());
        for(var e:r.effects()){text(o,e.targetIdentity());o.writeLong(e.expectedVersion());o.write(e.desiredFingerprint());}o.write(r.originalOperationFingerprint());
    }
    private static OperationAssertions operation(DataInputStream i)throws IOException {
        UUID boot=uuid(i);long epoch=i.readLong();UUID binding=uuid(i),op=uuid(i);int count=i.readInt();
        if(count<1||count>256)throw new IOException("Invalid full effect count");var effects=new ArrayList<EffectAssertions>();
        for(int n=0;n<count;n++)effects.add(new EffectAssertions(text(i),i.readLong(),hash(i)));
        return new OperationAssertions(boot,epoch,binding,op,effects,hash(i));
    }
    private static void graph(DataOutputStream o,CommonGraphAssertions g)throws IOException {
        text(o,g.manifestEncoding());blob(o,g.manifestBytes());o.write(WindowsWorldProvisioner.digest(g.manifestBytes()));
        text(o,g.bindingEncoding());blob(o,g.bindingBytes());o.write(WindowsWorldProvisioner.digest(g.bindingBytes()));
        text(o,g.repositoryEncoding());blob(o,g.repositoryBytes());o.write(WindowsWorldProvisioner.digest(g.repositoryBytes()));
    }
    private static byte[] checkedBlob(DataInputStream i)throws IOException{byte[] b=blob(i);exact(hash(i),WindowsWorldProvisioner.digest(b),"Full graph hash conflict");return b;}
    private static CommonGraphAssertions graph(DataInputStream i)throws IOException{return new CommonGraphAssertions(text(i),checkedBlob(i),text(i),checkedBlob(i),text(i),checkedBlob(i));}
    private static void time(DataOutputStream o,LocalDateTime t)throws IOException{o.writeInt(t.getYear());o.writeByte(t.getMonthValue());o.writeByte(t.getDayOfMonth());o.writeByte(t.getHour());o.writeByte(t.getMinute());o.writeByte(t.getSecond());o.writeInt(t.getNano());}
    private static LocalDateTime time(DataInputStream i)throws IOException{return LocalDateTime.of(i.readInt(),i.readUnsignedByte(),i.readUnsignedByte(),i.readUnsignedByte(),i.readUnsignedByte(),i.readUnsignedByte(),i.readInt());}
    private interface Writer<T>{void write(DataOutputStream o,T value)throws IOException;}
    private interface Reader<T>{T read(DataInputStream i)throws IOException;}
    private static <T>void optional(DataOutputStream o,T value,Writer<T>w)throws IOException{o.writeBoolean(value!=null);if(value!=null)w.write(o,value);}
    private static <T>T optional(DataInputStream i,Reader<T>r)throws IOException{int tag=i.readUnsignedByte();if(tag>1)throw new IOException("Noncanonical nullable tag");return tag==0?null:r.read(i);}
    private static void uuid(DataOutputStream o,UUID u)throws IOException{o.writeLong(u.getMostSignificantBits());o.writeLong(u.getLeastSignificantBits());}
    private static UUID uuid(DataInputStream i)throws IOException{return new UUID(i.readLong(),i.readLong());}
    private static byte[] hash(DataInputStream i)throws IOException{byte[] b=i.readNBytes(32);if(b.length!=32)throw new EOFException("Truncated fixed hash");return b;}
    private static void blob(DataOutputStream o,byte[] b)throws IOException{o.writeInt(b.length);o.write(b);}
    private static byte[] blob(DataInputStream i)throws IOException{int n=i.readInt();if(n<1||n>65536||n>i.available())throw new IOException("Invalid complete field length");return i.readNBytes(n);}
    private static void text(DataOutputStream o,String s)throws IOException {
        ByteBuffer b=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(s));
        if(b.remaining()>4096)throw new IOException("B1 UTF8 text exceeds bound");byte[] bytes=new byte[b.remaining()];b.get(bytes);blob(o,bytes);
    }
    private static String text(DataInputStream i)throws IOException{byte[] b=blob(i);if(b.length>4096)throw new IOException("B1 text exceeds bound");return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(b)).toString();}
    static void exact(byte[] a,byte[] b,String m)throws IOException{if(!Arrays.equals(a,b))throw new IOException(m);}
    static byte[] attemptBytes(AttemptAssertions a)throws IOException {
        var bytes=new ByteArrayOutputStream();try(var out=new DataOutputStream(bytes)){attempt(out,a);}return bytes.toByteArray();
    }
    static void consecutive(B1ResumeRecord older,byte[] olderBytes,B1ResumeRecord newer)throws IOException {
        validate(older);validate(newer);
        exact(encode(older),olderBytes,"Legacy predecessor differs from original encoded record");
        consecutiveCommon(older,olderBytes,newer);
    }
    static void consecutiveSchema5Data(B1ResumeRecord older,byte[] olderBytes,B1ResumeRecord newer)throws IOException {
        validateSchema5Data(older);validateSchema5Data(newer);
        exact(encodeSchema5Data(older),olderBytes,"Schema5 predecessor differs from original encoded record");
        consecutiveCommon(older,olderBytes,newer);
        if(newer.kind()==Kind.PREPARED_CONFIRMED){
            if(older.kind()!=Kind.GENESIS && older.kind()!=Kind.PREPARE_SUCCESSOR_INTENT)
                throw new IOException("Schema5 PREPARED confirmation must follow its actual PREPARE intent");
            requireConfirmedPrepareState(older.assertions().pendingAttempt(),newer.assertions());
        }
        if(newer.kind()==Kind.PREPARE_SUCCESSOR_INTENT){
            var old=older.assertions();var next=newer.assertions();var cancelled=old.latestSettlement();
            if(older.kind()!=Kind.ATTEMPT_CANCELLED || cancelled==null || cancelled.kind()!=SettlementKind.CANCELLED ||
                    cancelled.settledAttempt().phase()!=Phase.PREPARE)
                throw new IOException("Successor PREPARE requires its actual cancelled PREPARE predecessor");
            exact(settlementBytes(cancelled),settlementBytes(next.latestSettlement()),"Successor changed original cancelled settlement/UNKNOWN proof");
            var prior=cancelled.settledAttempt();var pending=next.pendingAttempt();
            if(prior.attemptId().equals(pending.attemptId()) || prior.context().boot().equals(pending.context().boot()) &&
                    prior.bootGeneration()==pending.bootGeneration() && prior.connectionGeneration().equals(pending.connectionGeneration()) &&
                    prior.connectionId()==pending.connectionId())throw new IOException("Successor must use a new execution attempt and actual scope");
            exact(writerBytes(old.currentWriter()),writerBytes(pending.beforeWriter()),"Successor seed differs from cancelled current W");
        }
        if(older.kind()==Kind.PREPARE_SUCCESSOR_INTENT && newer.kind()==Kind.PREPARED_CONFIRMED){
            var old=older.assertions();var next=newer.assertions();var pending=old.pendingAttempt();
            if(next.pendingAttempt()!=null || next.latestSettlement()==null || next.latestSettlement().kind()!=SettlementKind.CONFIRMED)
                throw new IOException("Successor confirmation requires complete confirmed settlement and no pending attempt");
            exact(attemptBytes(pending),attemptBytes(next.latestSettlement().settledAttempt()),"Successor confirmation changed exact pending scope");
            exact(writerBytes(pending.targetWriter()),writerBytes(next.currentWriter()),"Confirmed current W differs from successor target");
            exact(writerBytes(pending.targetWriter()),writerBytes(next.targetWriter()),"Confirmed target W differs from successor target");
            if(old.originalPrepareWitness()==null)
                exact(attemptBytes(pending),attemptBytes(next.originalPrepareWitness()),"First successor confirmation must initialize its actual witness");
        }
    }
    private static void requireSuccessorIntent(B1ResumeRecord record)throws IOException {
        var s=record.assertions();var a=s.pendingAttempt();var cancelled=s.latestSettlement();
        var p=WindowsWorldProvisionReceiptDataCodec.decode(record.originalReceipt()).plan();
        if(record.sequence()==0 || B1ResumeRecord.allZero(record.previousRecordSha256()) || a==null || a.phase()!=Phase.PREPARE ||
                cancelled==null || cancelled.kind()!=SettlementKind.CANCELLED || cancelled.settledAttempt().phase()!=Phase.PREPARE ||
                s.previousOwner()==null || s.retirement()==null || s.acknowledgedCertifyWitness()!=null || s.marker()!=null)
            throw new IOException("Complete non-genesis successor PREPARE/source/retirement/cancelled assertions required");
        if(a.attemptId().equals(p.prepareWriteUuid()) || a.attemptId().equals(p.certifyWriteUuid()) ||
                a.attemptId().equals(cancelled.settledAttempt().attemptId()) || a.beforeWriter().ownerBoot()!=null ||
                a.beforeWriter().lastSlot()!=null || !s.currentOwner().boot().equals(a.context().boot()))
            throw new IOException("Successor execution identity or original unowned/no-receipt seed differs");
        exact(writerBytes(s.currentWriter()),writerBytes(a.beforeWriter()),"Successor current W differs from complete seed");
        exact(writerBytes(s.targetWriter()),writerBytes(a.targetWriter()),"Successor target W differs from complete attempt");
        requireContextTarget(a);
    }
    private static void requireContextTarget(AttemptAssertions a)throws IOException {
        var c=a.context();var w=a.targetWriter();var slot=w.lastSlot();
        if(slot==null || !c.boot().equals(w.ownerBoot()) || c.writerEpoch()!=w.epoch() || !c.binding().equals(w.binding()) ||
                !c.boot().equals(slot.boot()) || c.writerEpoch()!=slot.writerEpoch() || !c.binding().equals(slot.binding()) ||
                !c.operation().equals(slot.operation()))throw new IOException("Successor target receipt differs from current context");
        exact(c.originalOperationFingerprint(),slot.fingerprint(),"Successor target context fingerprint differs");
    }
    static void requireConfirmedPrepareState(AttemptAssertions pending,FullStateAssertions confirmed)throws IOException {
        if(pending==null || pending.phase()!=Phase.PREPARE || confirmed.pendingAttempt()!=null ||
                confirmed.latestSettlement()==null || confirmed.latestSettlement().kind()!=SettlementKind.CONFIRMED)
            throw new IOException("Actual confirmed PREPARE settlement required");
        exact(attemptBytes(pending),attemptBytes(confirmed.latestSettlement().settledAttempt()),"Confirmed PREPARE scope changed");
        exact(writerBytes(pending.targetWriter()),writerBytes(confirmed.currentWriter()),"Confirmed PREPARE current W changed");
        exact(writerBytes(pending.targetWriter()),writerBytes(confirmed.targetWriter()),"Confirmed PREPARE target W changed");
    }
    private static byte[] settlementBytes(SettlementAssertions value)throws IOException {
        var bytes=new ByteArrayOutputStream();try(var out=new DataOutputStream(bytes)){
            text(out,value.kind().name());attempt(out,value.settledAttempt());text(out,value.proofProtocol());
            blob(out,value.fullOriginalEvidence());out.write(value.originalUnknownDiagnosticIdentity());
        }return bytes.toByteArray();
    }
    private static void consecutiveCommon(B1ResumeRecord older,byte[] olderBytes,B1ResumeRecord newer)throws IOException {
        if(older.sequence()==Long.MAX_VALUE||newer.sequence()!=older.sequence()+1)throw new IOException("B1 slots are not consecutive sequence");
        exact(WindowsWorldProvisioner.digest(olderBytes),newer.previousRecordSha256(),"B1 predecessor hash conflict");
        exact(older.sourceAnchorSha256(),newer.sourceAnchorSha256(),"B1 source changed across slots");exact(older.originalReceipt(),newer.originalReceipt(),"B1 original receipt changed across slots");
        if(!older.barrierNamespace().equals(newer.barrierNamespace()))throw new IOException("B1 namespace changed across slots");
        var original=older.assertions().originalPrepareWitness();
        if(original!=null){if(newer.assertions().originalPrepareWitness()==null)throw new IOException("Original prepare witness removed");
            exact(attemptBytes(original),attemptBytes(newer.assertions().originalPrepareWitness()),"Original prepare scope/witness changed");}
        if(older.kind()==Kind.GENESIS&&newer.kind()==Kind.PREPARED_CONFIRMED)
            exact(attemptBytes(older.assertions().pendingAttempt()),attemptBytes(newer.assertions().originalPrepareWitness()),"Confirmed prepare differs from original genesis attempt");
        var acknowledged=older.assertions().acknowledgedCertifyWitness();
        if(acknowledged!=null){if(newer.assertions().acknowledgedCertifyWitness()==null)throw new IOException("Acknowledged certify witness removed");
            exact(attemptBytes(acknowledged),attemptBytes(newer.assertions().acknowledgedCertifyWitness()),"Acknowledged certify witness changed");}
        if(older.kind()==Kind.NATIVE_ACK_CERTIFY_INTENT&&newer.kind()==Kind.CERTIFIED){
            requireCertifyWitness(older.assertions().pendingAttempt());requireCertifyWitness(newer.assertions().acknowledgedCertifyWitness());
            exact(attemptBytes(older.assertions().pendingAttempt()),attemptBytes(newer.assertions().acknowledgedCertifyWitness()),"Acknowledged certify differs from original pending full attempt");}
        if(newer.kind()==Kind.ATTEMPT_CANCELLED){var pending=older.assertions().pendingAttempt();
            if(pending==null)throw new IOException("Cancelled settlement has no original pending attempt");
            exact(attemptBytes(pending),attemptBytes(newer.assertions().latestSettlement().settledAttempt()),"Cancelled settlement changed original attempt scope");}
    }
}
