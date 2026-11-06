package com.wi.floatingpod.server;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/** 永久 receipt V1 DATA 编码；没有 native handle、批准、custody 或 DATA-to-native 转换。 */
public final class WindowsWorldProvisionReceiptDataCodec {
    private WindowsWorldProvisionReceiptDataCodec() { }
    private static final long MAGIC = 0x4650575243503031L;
    private static final int MAX_BYTES = 65536;
    public record PlanData(UUID creationOperationUuid, UUID bootstrapOperationUuid, UUID prepareWriteUuid,
            UUID certifyWriteUuid, UUID bindingUuid, int physicalFormat, UUID mysqlServerUuid,
            String schema, int schemaFormat, String ddlSha256, UUID provisioningUuid) implements FirstBindingPlanData {
        public PlanData {
            Objects.requireNonNull(creationOperationUuid);Objects.requireNonNull(bootstrapOperationUuid);
            Objects.requireNonNull(prepareWriteUuid);Objects.requireNonNull(certifyWriteUuid);
            Objects.requireNonNull(bindingUuid);Objects.requireNonNull(mysqlServerUuid);Objects.requireNonNull(provisioningUuid);
            if (prepareWriteUuid.equals(certifyWriteUuid)) throw new IllegalArgumentException("Two phase write UUIDs must be distinct");
            if (physicalFormat != 2 || (schemaFormat != 2 && schemaFormat != 3 && schemaFormat != 5) || bindingUuid.version() != 4)
                throw new IllegalArgumentException("Receipt DATA requires schema2/3/5, physical2 and UUIDv4 binding");
            if (schema == null || !schema.matches("[A-Za-z0-9_]{1,64}")) throw new IllegalArgumentException("Safe exact schema required");
            if (schemaFormat == 5) {
                if (!schema.matches("[a-z][a-z0-9_]{0,63}")) throw new IllegalArgumentException("Schema5 recipe requires canonical lowercase schema");
                for (var id : new UUID[]{creationOperationUuid,bootstrapOperationUuid,prepareWriteUuid,certifyWriteUuid,
                        bindingUuid,mysqlServerUuid,provisioningUuid})
                    if (id.equals(new UUID(0,0))) throw new IllegalArgumentException("Schema5 recipe requires nonzero UUIDs");
            }
            WindowsWorldProvisioner.hashBytes(ddlSha256);
            ddlSha256 = ddlSha256.toUpperCase(Locale.ROOT);
        }
    }
    public record ReceiptData(PlanData plan, String sourceIdentity, String guardIdentity,
            long creatorPid, long creatorCreationTime, WindowsWorldProvisioner.BootIdentity creationBoot) {
        public ReceiptData {
            Objects.requireNonNull(plan);Objects.requireNonNull(creationBoot);
            if (!identity(sourceIdentity) || !identity(guardIdentity)) throw new IllegalArgumentException("Receipt requires full canonical FileId128 identity");
            WindowsMarkerIo.validateProcessIdentity(creatorPid,creatorCreationTime);
        }
    }
    private static boolean identity(String text) {
        return text != null && text.matches("FP_WIN32_FILE_ID_V1;volume=[0-9a-f]{16};id=[0-9a-f]{32}");
    }
    public static byte[] encode(ReceiptData data) throws IOException {
        Objects.requireNonNull(data);
        var buffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(buffer)) {
            out.writeLong(MAGIC);out.writeInt(1);var p=data.plan();
            uuid(out,p.creationOperationUuid());uuid(out,p.bootstrapOperationUuid());uuid(out,p.prepareWriteUuid());
            uuid(out,p.certifyWriteUuid());uuid(out,p.bindingUuid());out.writeInt(p.physicalFormat());
            uuid(out,p.mysqlServerUuid());text(out,p.schema());out.writeInt(p.schemaFormat());out.write(WindowsWorldProvisioner.hashBytes(p.ddlSha256()));
            uuid(out,p.provisioningUuid());text(out,data.sourceIdentity());text(out,data.guardIdentity());
            out.writeLong(data.creatorPid());out.writeLong(data.creatorCreationTime());
            uuid(out,data.creationBoot().bootId());out.writeLong(data.creationBoot().generation());
            out.flush();out.write(WindowsWorldProvisioner.digest(buffer.toByteArray()));
        }
        byte[] bytes=buffer.toByteArray();
        if (bytes.length > MAX_BYTES) throw new IOException("Invalid full receipt size");
        return bytes;
    }
    public static ReceiptData decode(byte[] original) throws IOException {
        if (original == null || original.length < 32 || original.length > MAX_BYTES) throw new IOException("Invalid full receipt size");
        byte[] bytes=original.clone();byte[] body=Arrays.copyOf(bytes,bytes.length-32);
        if (!MessageDigest.isEqual(WindowsWorldProvisioner.digest(body),Arrays.copyOfRange(bytes,body.length,bytes.length)))
            throw new IOException("Receipt internal full-body digest mismatch");
        try (var in=new DataInputStream(new ByteArrayInputStream(body))) {
            if(in.readLong()!=MAGIC || in.readInt()!=1) throw new IOException("Unknown receipt encoding/version");
            UUID creation=uuid(in),bootstrap=uuid(in),prepare=uuid(in),certify=uuid(in),binding=uuid(in);
            int physical=in.readInt();UUID server=uuid(in);String schema=text(in);int format=in.readInt();
            byte[] ddl=in.readNBytes(32);if(ddl.length!=32)throw new IOException("Short receipt DDL SHA");
            var plan=new PlanData(creation,bootstrap,prepare,certify,binding,physical,server,schema,format,
                HexFormat.of().withUpperCase().formatHex(ddl),uuid(in));
            // Preserve the old decoder's direct IOException outcome for malformed FileId text.
            String source=text(in),guard=text(in);
            WindowsWorldProvisioner.validateIdentityText(source);WindowsWorldProvisioner.validateIdentityText(guard);
            var data=new ReceiptData(plan,source,guard,in.readLong(),in.readLong(),
                new WindowsWorldProvisioner.BootIdentity(uuid(in),in.readLong()));
            if(in.read()!=-1)throw new IOException("Trailing receipt fields");
            if(!Arrays.equals(bytes,encode(data)))throw new IOException("Noncanonical full receipt encoding");
            return data;
        } catch(IllegalArgumentException | NullPointerException failure) {
            throw new IOException("Invalid complete receipt identity",failure);
        }
    }
    private static void uuid(DataOutputStream out,UUID value)throws IOException {out.writeLong(value.getMostSignificantBits());out.writeLong(value.getLeastSignificantBits());}
    private static UUID uuid(DataInputStream in)throws IOException {return new UUID(in.readLong(),in.readLong());}
    private static void text(DataOutputStream out,String value)throws IOException {
        var encoded=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
        if(encoded.remaining()>4096)throw new IOException("Receipt text exceeds exact encoding bound");
        byte[] bytes=new byte[encoded.remaining()];encoded.get(bytes);out.writeInt(bytes.length);out.write(bytes);
    }
    private static String text(DataInputStream in)throws IOException {
        int length=in.readInt();if(length<0||length>4096||length>in.available())throw new IOException("Invalid receipt text length");
        try {return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(in.readNBytes(length))).toString();}
        catch(CharacterCodingException failure){throw new IOException("Invalid canonical receipt UTF-8",failure);}
    }
}
