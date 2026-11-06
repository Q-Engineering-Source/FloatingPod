package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestCodec.*;
import java.io.*;
import java.util.*;
import java.util.zip.DeflaterOutputStream;

/** 仅真实NBT二进制fixture；不复制生产decoder/validation算法，不模拟实际factory成功。 */
final class SourceForestFixtures {
    static final SourceLimitProfile LIMITS=SourceLimitProfile.of(1_000_000,100_000,100_000,64,4096,10_000_000,1_000_000,65536);
    static UUID id(long n){return new UUID(0,n);}
    static Forest forest()throws IOException{return forest(false,false);}
    static Forest forest(boolean otherOnly,boolean unresolved)throws IOException{return forest(otherOnly,unresolved,false);}
    static Forest forest(boolean otherOnly,boolean unresolved,boolean classificationChange)throws IOException {
        List<Carrier> carriers=new ArrayList<>();List<Member> members=new ArrayList<>();
        for(int carrierIndex=0;carrierIndex<2;carrierIndex++) {
            List<CarrierStage> carrierStages=new ArrayList<>();Map<Integer,List<Stage>> memberStages=new HashMap<>();
            for(StageKind kind:StageKind.values()) {
                boolean stageOther=otherOnly||classificationChange&&kind==StageKind.FIXED;
                long pod=1+kind.ordinal();byte[] leaf=entity(stageOther?"minecraft:pig":"floatingpod:pod",id(pod),List.of());
                byte[] middle=entity("minecraft:pig",id(90),List.of(leaf));
                byte[] root=entity(stageOther?"minecraft:pig":"floatingpod:pod",id(pod),carrierIndex==0?List.of(middle):List.of());
                byte[] chunk=chunk(List.of(root));carrierStages.add(new CarrierStage(kind,id(100+carrierIndex*10+kind.ordinal()),new Bytes(chunk)));
                List<byte[]> payloads=carrierIndex==0?List.of(root,middle,leaf):List.of(root);
                for(int local=0;local<payloads.size();local++) {
                    int member=carrierIndex==0?local+1:4;UUID parent=local==0?null:id(member-1);List<Integer> path=Collections.nCopies(local+1,0);
                    Classification classification=stageOther||local==1?Classification.OTHER:Classification.FLOATING_POD;
                    if(unresolved&&member==3&&kind==StageKind.FIXED)classification=Classification.UNKNOWN;
                    boolean actual=kind==StageKind.ACTUAL;Observation observation=actual?Observation.RETURNED:Observation.NOT_OBSERVED;int count=actual?1:0;
                    memberStages.computeIfAbsent(member,k->new ArrayList<>()).add(new Stage(kind,id(200+member*10+kind.ordinal()),id(10+carrierIndex),parent,path,classification,new Bytes(payloads.get(local)),
                        observation,count,observation,count,observation,count,observation,count,observation,count,"",null));
                }
            }
            byte[] raw=carrierStages.getFirst().chunkNbt().copy();ByteArrayOutputStream compressed=new ByteArrayOutputStream();
            try(DeflaterOutputStream output=new DeflaterOutputStream(compressed)){output.write(raw);}
            carriers.add(new Carrier(id(10+carrierIndex),"DIM0/r."+carrierIndex+".0.mca",carrierIndex,0,0,carrierIndex*32,0,0,0x8000000000000001L,new Bytes(new byte[16]),7,12288,2,new Bytes(compressed.toByteArray()),2,1,0x80000000,CarrierDisposition.CURRENT,id(600+carrierIndex),carrierStages));
            for(var entry:memberStages.entrySet()) {
                int member=entry.getKey();List<Stage> stages=entry.getValue();members.add(new Member(id(member),Disposition.INDEXED,id(300+member),Observation.NOT_OBSERVED,0,null,stages,
                    List.of(new Lineage(LineageKind.RAW_TO_FIXED,stages.get(0).captureId(),stages.get(1).captureId(),id(400+member)),
                            new Lineage(LineageKind.FIXED_TO_ACTUAL,stages.get(1).captureId(),stages.get(2).captureId(),id(500+member)))));
            }
        }
        members.sort(Comparator.comparing(Member::id,SourceForestCodec::compareUuid));
        return new Forest(new Identity(id(1001),id(1002),id(1003),1,id(1004),"world","save","cleanroom/scalar",id(1005),9),Coverage.SUPPLIED_ROOTS_ONLY,new Bytes(new byte[]{1,2,3}),List.of(id(1),id(4)),List.of(id(1),id(4)),List.of(id(1),id(4)),carriers,members);
    }
    static Forest externalAncestors(Classification ancestorClassification,boolean omitBackground,boolean unknownSibling)throws IOException {
        Forest original=forest();Member oldMember=original.members().get(2);List<Stage> members=new ArrayList<>();List<CarrierStage> carrierStages=new ArrayList<>();
        for(Stage old:oldMember.stages()) {
            members.add(new Stage(old.kind(),old.captureId(),old.carrierId(),null,old.entityPath(),old.classification(),old.entityNbt(),old.factory(),old.factoryCalls(),old.read(),old.readCalls(),old.caps(),old.capsCalls(),old.placement(),old.placementCalls(),old.index(),old.indexCalls(),"",null));
            byte[] middle=entity("minecraft:pig",id(90),List.of(old.entityNbt().copy()));
            byte[] ancestor=entity("minecraft:cow",id(101+old.kind().ordinal()),List.of(middle));
            byte[] siblingChild=entity("minecraft:pig",id(190),List.of());byte[] sibling=entity("minecraft:cow",id(180),List.of(siblingChild));
            List<BackgroundOccurrence> background=omitBackground?List.of():List.of(
                new BackgroundOccurrence(id(800+old.kind().ordinal()*10),List.of(0),ancestorClassification,new Bytes(ancestor)),
                new BackgroundOccurrence(id(801+old.kind().ordinal()*10),List.of(0,0),Classification.OTHER,new Bytes(middle)),
                new BackgroundOccurrence(id(802+old.kind().ordinal()*10),List.of(1),unknownSibling?Classification.UNKNOWN:Classification.OTHER,new Bytes(sibling)),
                new BackgroundOccurrence(id(803+old.kind().ordinal()*10),List.of(1,0),Classification.OTHER,new Bytes(siblingChild)));
            carrierStages.add(new CarrierStage(old.kind(),id(100+old.kind().ordinal()),new Bytes(chunk(List.of(ancestor,sibling))),background));
        }
        Member included=new Member(oldMember.id(),oldMember.disposition(),oldMember.dispositionObservation(),oldMember.dispositionAction(),oldMember.dispositionCalls(),oldMember.dispositionCaptureId(),members,oldMember.lineage());
        Carrier old=original.carriers().getFirst();ByteArrayOutputStream compressed=new ByteArrayOutputStream();try(DeflaterOutputStream output=new DeflaterOutputStream(compressed)){output.write(carrierStages.getFirst().chunkNbt().copy());}
        Carrier carrier=new Carrier(old.id(),old.regionKey(),old.regionX(),old.regionZ(),old.dimension(),old.chunkX(),old.chunkZ(),old.slot(),old.volumeSerialBits(),old.fileId128(),old.saveGeneration(),old.originalRegionLength(),old.compressionType(),new Bytes(compressed.toByteArray()),old.sectorOffset(),old.sectorCount(),old.timestampBits(),old.disposition(),old.dispositionObservation(),carrierStages);
        return new Forest(original.identity(),original.coverage(),original.sourceObservation(),List.of(included.id()),List.of(included.id()),List.of(included.id()),List.of(carrier),List.of(included));
    }
    static byte[] entity(String type,UUID uuid,List<byte[]> passengers)throws IOException {
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();DataOutputStream out=new DataOutputStream(bytes);out.writeByte(10);out.writeUTF("");
        named(out,8,"id");out.writeUTF(type);named(out,4,"UUIDMost");out.writeLong(uuid.getMostSignificantBits());named(out,4,"UUIDLeast");out.writeLong(uuid.getLeastSignificantBits());
        named(out,9,"Pos");out.writeByte(6);out.writeInt(3);out.writeLong(0x8000000000000000L);out.writeLong(0x7ff8000000000042L);out.writeLong(0x3ff0000000000000L);
        named(out,9,"Rotation");out.writeByte(5);out.writeInt(2);out.writeInt(0x80000000);out.writeInt(0x7fc00042);
        named(out,10,"ForgeCaps");named(out,8,"opaque");out.writeUTF("x\u0000\u2603");named(out,7,"bytes");out.writeInt(3);out.write(new byte[]{0,-1,42});out.writeByte(0);
        named(out,11,"unknownInts");out.writeInt(2);out.writeInt(Integer.MIN_VALUE);out.writeInt(17);
        named(out,12,"unknownLongs");out.writeInt(1);out.writeLong(Long.MIN_VALUE);
        named(out,9,"Passengers");out.writeByte(10);out.writeInt(passengers.size());for(byte[] passenger:passengers)out.write(passenger,3,passenger.length-3);
        out.writeByte(0);return bytes.toByteArray();
    }
    static byte[] chunk(List<byte[]> entities)throws IOException {
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();DataOutputStream out=new DataOutputStream(bytes);out.writeByte(10);out.writeUTF("");
        named(out,10,"Level");named(out,9,"Entities");out.writeByte(10);out.writeInt(entities.size());for(byte[] entity:entities)out.write(entity,3,entity.length-3);
        named(out,9,"TileEntities");out.writeByte(10);out.writeInt(0);named(out,7,"Blocks");out.writeInt(4);out.write(new byte[]{1,2,3,4});out.writeByte(0);
        named(out,3,"DataVersion");out.writeInt(1343);out.writeByte(0);return bytes.toByteArray();
    }
    static void named(DataOutputStream out,int type,String name)throws IOException{out.writeByte(type);out.writeUTF(name);}
    static Forest members(Forest original,List<Member> members){return new Forest(original.identity(),original.coverage(),original.sourceObservation(),original.rawRoots(),original.fixedRoots(),original.actualRoots(),original.carriers(),members);}
}
