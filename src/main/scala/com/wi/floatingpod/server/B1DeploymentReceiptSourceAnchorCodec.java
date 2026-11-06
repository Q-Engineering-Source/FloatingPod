package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.floatingpod.storage.mysql.MySqlSessionBarrierNamespace;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/** 独立 schema5 完整 A/anchor DATA；既有 V4 codec 永远不接纳此域。 */
final class B1DeploymentReceiptSourceAnchorCodec {
    static final long MAGIC = 0x46504231414e4336L;
    static final int VERSION = 6, MAX_BYTES = 65536;
    static final String PROTOCOL = "FP_B1_SOURCE_ANCHOR_BINARY_BE_V6";
    static final String CONFIGURATION = "FP_B1_CONFIGURATION_DEPLOYMENT_RECEIPT_V1";
    static final String CONFIGURATION_KEY = "deploymentReceiptConfigurationEncoding";
    static final String SLOT_PROTOCOL = "FP_B1_RESUME_BINARY_BE_V6";
    static final String SLOT0 = ".floatingpod-b1-resume-v6.0", SLOT1 = ".floatingpod-b1-resume-v6.1", SLOT_TMP = ".floatingpod-b1-resume-v6.tmp";
    private B1DeploymentReceiptSourceAnchorCodec() { }

    static byte[] configurationBytes(MySqlRuntimeMetadata.DeploymentReceiptStatic metadata) throws IOException {
        if (metadata == null || metadata.baseline().schemaFormat() != 5) throw new IOException("Actual schema5 complete metadata required");
        var fields = new TreeMap<>(metadata.completeCanonicalFields());
        if (fields.put(CONFIGURATION_KEY, CONFIGURATION) != null) throw new IOException("Configuration marker conflicts with complete metadata");
        var text = new StringBuilder();
        for (var entry : fields.entrySet()) {
            text.append(entry.getKey()).append('=').append(escape(entry.getValue())).append('\n');
            if (text.length() > MAX_BYTES) throw new IOException("Complete schema5 A exceeds bound");
        }
        var encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(text));
        if (encoded.remaining() > MAX_BYTES) throw new IOException("Complete schema5 A UTF8 exceeds bound");
        byte[] result = new byte[encoded.remaining()]; encoded.get(result); return result;
    }
    static MySqlRuntimeMetadata.DeploymentReceiptStatic configurationMetadata(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_BYTES) throw new IOException("Bounded complete schema5 A required");
        String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        var fields = new Properties() {
            @Override public synchronized Object put(Object key, Object value) {
                if (!(key instanceof String) || containsKey(key)) throw new IllegalArgumentException("Duplicate schema5 A key");
                return super.put(key,value);
            }
        };
        try {
            fields.load(new StringReader(text));
            if (!CONFIGURATION.equals(fields.remove(CONFIGURATION_KEY))) throw new IOException("Explicit schema5 A marker required");
            var sorted = new TreeMap<String,String>();
            for (String key : fields.stringPropertyNames()) sorted.put(key,fields.getProperty(key));
            var raw = new StringBuilder();
            for (var e : sorted.entrySet()) raw.append(e.getKey()).append('=').append(escape(e.getValue())).append('\n');
            var metadata = MySqlRuntimeMetadata.fromDeploymentReceiptBytes(raw.toString().getBytes(StandardCharsets.UTF_8));
            exact(bytes,configurationBytes(metadata),"Noncanonical complete schema5 A");
            return metadata;
        } catch (IllegalArgumentException failure) { throw new IOException("Invalid complete schema5 A",failure); }
    }
    private static String escape(String value) {
        var out = new StringBuilder();
        for (int i=0;i<value.length();i++) {
            switch (value.charAt(i)) {
                case '\\' -> out.append("\\\\"); case '\n' -> out.append("\\n"); case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t"); case '\f' -> out.append("\\f"); case ' ' -> out.append("\\ ");
                default -> out.append(value.charAt(i));
            }
        }
        return out.toString();
    }
    static String namespace(MySqlRuntimeMetadata.DeploymentReceiptStatic metadata) {
        Map<String,String> v=metadata.completeCanonicalFields();
        return MySqlSessionBarrierNamespace.forIdentity(java.util.UUID.fromString(v.get("expectedServerUuid")),v.get("schema"),
            metadata.baseline().provisioningUuid(),java.util.UUID.fromString(v.get("expectedBindingUuid"))).lockName();
    }
    static String canonicalConfigurationNamespace(byte[] bytes) throws IOException { return namespace(configurationMetadata(bytes)); }
    static void requirePlan(MySqlRuntimeMetadata.DeploymentReceiptStatic metadata, WindowsWorldProvisionReceiptDataCodec.PlanData p) throws IOException {
        var v=metadata.completeCanonicalFields(); var b=metadata.baseline();
        if (p == null || p.schemaFormat()!=5 || p.physicalFormat()!=2 || b.schemaFormat()!=5 ||
            !p.mysqlServerUuid().toString().equals(v.get("expectedServerUuid")) || !p.schema().equals(v.get("schema")) ||
            !p.bindingUuid().toString().equals(v.get("expectedBindingUuid")) || !p.provisioningUuid().equals(b.provisioningUuid()) ||
            !p.ddlSha256().equalsIgnoreCase(b.ddlSha256())) throw new IOException("Schema5 A and original receipt plan conflict");
    }
    static byte[] encode(B1SourceAnchorCodec.Data data) throws IOException {
        if (data == null) throw new IOException("Complete schema5 anchor DATA required");
        var m=configurationMetadata(data.configuration());
        var receipt=WindowsWorldProvisionReceiptDataCodec.decode(data.receipt());requirePlan(m,receipt.plan());
        WindowsWorldProvisioner.validateIdentityText(data.receiptFileIdentity());
        WindowsWorldProvisioner.validateIdentityText(data.configParentIdentity());
        WindowsWorldProvisioner.validateIdentityText(data.metadataFileIdentity());
        WindowsB1SourceAnchorStore.requireFilename(data.metadataFilename());
        WindowsB1SourceAnchorStore.requireNativeWorldPath(data.configParentNativePath() +
            (data.configParentNativePath().endsWith("\\") ? "" : "\\") + data.metadataFilename());
        WindowsB1SourceAnchorStore.requireNativeWorldPath(data.worldNativePath());
        WindowsB1SourceAnchorStore.requireNativeOutside(data.configParentNativePath(),data.worldNativePath());
        var bodyBuffer=new ByteArrayOutputStream();
        try (var out=new DataOutputStream(new Bounded(bodyBuffer,MAX_BYTES-48))) {
            text(out,PROTOCOL);blob(out,data.configuration());out.write(WindowsWorldProvisioner.digest(data.configuration()));
            blob(out,data.receipt());out.write(WindowsWorldProvisioner.digest(data.receipt()));
            text(out,data.receiptFileIdentity());text(out,data.configParentIdentity());text(out,data.metadataFileIdentity());
            text(out,data.configParentNativePath());text(out,data.worldNativePath());text(out,data.metadataFilename());
            text(out,namespace(m));text(out,SLOT_PROTOCOL);text(out,SLOT0);text(out,SLOT1);text(out,SLOT_TMP);
        }
        byte[] body=bodyBuffer.toByteArray();var result=new ByteArrayOutputStream();
        try (var out=new DataOutputStream(result)) {
            out.writeLong(MAGIC);out.writeInt(VERSION);out.writeInt(body.length);out.write(body);out.write(WindowsWorldProvisioner.digest(body));
        }
        return result.toByteArray();
    }
    static B1SourceAnchorCodec.Data decode(byte[] input) throws IOException {
        if (input==null || input.length<48 || input.length>MAX_BYTES) throw new IOException("Invalid full schema5 anchor bound");
        byte[] bytes=input.clone();
        try (var in=new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readLong()!=MAGIC || in.readInt()!=VERSION) throw new IOException("Unknown schema5 anchor version/domain");
            int size=in.readInt();if (size<=0 || size!=bytes.length-48) throw new IOException("Invalid schema5 anchor payload size");
            byte[] body=in.readNBytes(size);exact(in.readNBytes(32),WindowsWorldProvisioner.digest(body),"Schema5 anchor payload SHA differs");
            if (in.read()!=-1) throw new IOException("Trailing schema5 anchor bytes");
            B1SourceAnchorCodec.Data data;
            try (var fields=new DataInputStream(new ByteArrayInputStream(body))) {
                if (!PROTOCOL.equals(text(fields))) throw new IOException("Unknown schema5 anchor protocol");
                byte[] config=blob(fields);exact(fields.readNBytes(32),WindowsWorldProvisioner.digest(config),"Schema5 A digest differs");
                byte[] receipt=blob(fields);exact(fields.readNBytes(32),WindowsWorldProvisioner.digest(receipt),"Schema5 receipt digest differs");
                data=new B1SourceAnchorCodec.Data(config,receipt,text(fields),text(fields),text(fields),text(fields),text(fields),text(fields));
                if (!canonicalConfigurationNamespace(config).equals(text(fields)) || !SLOT_PROTOCOL.equals(text(fields)) ||
                    !SLOT0.equals(text(fields)) || !SLOT1.equals(text(fields)) || !SLOT_TMP.equals(text(fields))) throw new IOException("Schema5 namespace/slot family differs");
                if (fields.read()!=-1) throw new IOException("Trailing schema5 anchor fields");
            }
            exact(bytes,encode(data),"Noncanonical schema5 anchor bytes");return data;
        } catch (IllegalArgumentException | NullPointerException failure) { throw new IOException("Invalid schema5 anchor identity",failure); }
    }
    private static void text(DataOutputStream out,String value) throws IOException {
        if (value==null || value.isEmpty() || value.length()>4096) throw new IOException("Bounded schema5 anchor text required");
        var encoded=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(value));
        if (encoded.remaining()>4096) throw new IOException("Schema5 anchor UTF8 text exceeds bound");
        byte[] b=new byte[encoded.remaining()];encoded.get(b);blob(out,b);
    }
    private static String text(DataInputStream in) throws IOException {
        byte[] b=blob(in);if (b.length>4096) throw new IOException("Schema5 anchor text exceeds bound");
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(b)).toString();
    }
    private static void blob(DataOutputStream out,byte[] bytes) throws IOException {
        if (bytes==null || bytes.length==0 || bytes.length>MAX_BYTES) throw new IOException("Bounded schema5 anchor blob required");
        out.writeInt(bytes.length);out.write(bytes);
    }
    private static byte[] blob(DataInputStream in) throws IOException {
        int n=in.readInt();if (n<=0 || n>MAX_BYTES || n>in.available()) throw new IOException("Invalid/truncated schema5 anchor blob");
        byte[] b=in.readNBytes(n);if(b.length!=n)throw new EOFException("Truncated schema5 anchor blob");return b;
    }
    private static void exact(byte[] a,byte[] b,String message) throws IOException {
        if(!Arrays.equals(a,b))throw new IOException(message);
    }
    private static final class Bounded extends FilterOutputStream {
        private final int limit;private int count;
        Bounded(OutputStream out,int limit){super(out);this.limit=limit;}
        private void add(int n)throws IOException{if(n<0 || n>limit-count)throw new IOException("Schema5 anchor exceeds full bound");count+=n;}
        @Override public void write(int b)throws IOException{add(1);out.write(b);}
        @Override public void write(byte[] b,int offset,int n)throws IOException{java.util.Objects.checkFromIndexSize(offset,n,b.length);add(n);out.write(b,offset,n);}
    }
}
