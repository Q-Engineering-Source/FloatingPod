package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.floatingpod.storage.mysql.MySqlSessionBarrierNamespace;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** 纯 canonical 数据 codec；磁盘 facts 永远不能构造 live custody。 */
final class B1SourceAnchorCodec {
    static final long MAGIC = 0x46504231414e4334L;
    static final int VERSION = 4, MAX_BYTES = 65536, MAX_TEXT = 4096;
    static final String PROTOCOL = "FP_B1_SOURCE_ANCHOR_BINARY_BE_V4";
    static final String SLOT_PROTOCOL = "FP_B1_RESUME_BINARY_BE_V4";
    static final String SLOT0 = ".floatingpod-b1-resume-v4.0";
    static final String SLOT1 = ".floatingpod-b1-resume-v4.1";
    static final String SLOT_TMP = ".floatingpod-b1-resume-v4.tmp";

    static byte[] configurationBytes(MySqlRuntimeMetadata m) {
        Map<String, String> v = new LinkedHashMap<>();
        v.put("backend", "mysql"); v.put("host", m.host()); v.put("port", Integer.toString(m.port()));
        v.put("schema", m.schema()); v.put("expectedServerUuid", m.expectedServerUuid().toString());
        v.put("expectedBindingUuid", m.expectedBindingUuid().toString());
        v.put("expectedEffectiveUser", m.expectedEffectiveUser()); v.put("username", m.username());
        v.put("pkcs12TrustStorePath", m.pkcs12TrustStorePath().toString());
        v.put("credentialSourceRef", "file:" + m.credentialSourceRef().absolutePath());
        v.put("trustSecretSourceRef", "file:" + m.trustSecretSourceRef().absolutePath());
        v.put("connectTimeoutMillis", Long.toString(m.connectTimeout().toMillis()));
        v.put("socketTimeoutMillis", Long.toString(m.socketTimeout().toMillis()));
        v.put("activeLimit", Integer.toString(m.activeLimit()));
        v.put("innodbFlushLogAtTrxCommit", Integer.toString(m.innodbFlushLogAtTrxCommit()));
        v.put("syncBinlog", Long.toString(m.syncBinlog()));
        var b = m.baseline();
        v.put("approvedBaselinePath", b.absolutePath().toString()); v.put("approvedBaselineSha256", b.fileSha256());
        v.put("approvedBaselineFormat", b.format()); v.put("approvedSchemaFormat", Integer.toString(b.schemaFormat()));
        v.put("approvedCoverage", b.coverage()); v.put("expectedServerVersion", b.serverVersion());
        v.put("expectedProvisioningUuid", b.provisioningUuid().toString()); v.put("approvedDdlSha256", b.ddlSha256());
        v.put("expectedAuditDefiner", b.auditDefiner()); v.put("approvedSourceCaptureSha256", b.sourceCaptureSha256());
        if (b.schemaFormat() == 3) {
            var contract = m.format3RuntimeContract().orElseThrow(() -> new IllegalArgumentException("format3 runtime contract pins are required"));
            v.put("format3ConfigurationEncoding", "FP_B1_CONFIGURATION_FORMAT3_V1");
            v.put("format3RuntimeContractEncoding", "FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V1");
            v.put("format3RuntimeContractPath", contract.absolutePath().toString());
            v.put("format3RuntimeContractSha256", contract.fileSha256());
        }
        StringBuilder text = new StringBuilder();
        v.forEach((key, value) -> text.append(key).append('=').append(escape(value)).append('\n'));
        return text.toString().getBytes(StandardCharsets.UTF_8);
    }
    /** 编码域标记只属于 anchor canonical bytes；普通 runtime properties 仍严格拒绝这些键。 */
    private static MySqlRuntimeMetadata configurationMetadata(byte[] bytes) throws IOException {
        String canonical = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes)).toString();
        String properties = canonical
            .replace("format3ConfigurationEncoding=FP_B1_CONFIGURATION_FORMAT3_V1\n", "")
            .replace("format3RuntimeContractEncoding=FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V1\n", "");
        // 调用者仍逐字节对照完整 canonical 重编码，因此缺失、重复、错值及跨格式标记均拒绝。
        try {
            return MySqlRuntimeMetadata.fromBytes(properties.getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException failure) {
            throw new IOException("Invalid canonical anchor configuration", failure);
        }
    }
    /** 只读取完整 canonical 配置的 namespace；不签发 native 或 SQL 资格。 */
    static String canonicalConfigurationNamespace(byte[] canonical) throws IOException {
        var metadata = configurationMetadata(canonical);
        requireExact(canonical, configurationBytes(metadata), "Noncanonical configuration identity");
        return namespace(metadata);
    }
    private static String escape(String value) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> out.append("\\\\"); case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r"); case '\t' -> out.append("\\t");
                case '\f' -> out.append("\\f"); case ' ' -> out.append("\\ ");
                default -> out.append(c);
            }
        }
        return out.toString();
    }
    static String namespace(MySqlRuntimeMetadata m) {
        return MySqlSessionBarrierNamespace.forIdentity(m.expectedServerUuid(), m.schema(),
            m.baseline().provisioningUuid(), m.expectedBindingUuid()).lockName();
    }
    static void requirePlan(MySqlRuntimeMetadata m, WindowsWorldProvisioner.Plan p) throws IOException {
        if ((m.baseline().schemaFormat() != 2 && m.baseline().schemaFormat() != 3) ||
            m.baseline().schemaFormat() != p.schemaFormat() || p.physicalFormat() != 2 ||
            !p.mysqlServerUuid().equals(m.expectedServerUuid()) || !p.schema().equals(m.schema()) ||
            !p.bindingUuid().equals(m.expectedBindingUuid()) ||
            !p.provisioningUuid().equals(m.baseline().provisioningUuid()) ||
            !p.ddlSha256().equalsIgnoreCase(m.baseline().ddlSha256()))
            throw new IOException("Anchor configuration and creation plan conflict");
    }
    record Data(byte[] configuration, byte[] receipt, String receiptFileIdentity,
                String configParentIdentity, String metadataFileIdentity,
                String configParentNativePath, String worldNativePath, String metadataFilename) {
        Data { configuration = configuration.clone(); receipt = receipt.clone(); }
        @Override public byte[] configuration() { return configuration.clone(); }
        @Override public byte[] receipt() { return receipt.clone(); }
        @Override public String toString() { return "B1SourceAnchorData[redacted; disk facts only]"; }
    }
    static byte[] encode(Data data) throws IOException {
        var m = configurationMetadata(data.configuration);
        requireExact(data.configuration, configurationBytes(m), "Noncanonical configuration identity");
        var receipt = WindowsWorldProvisioner.decodeReceipt(data.receipt);
        requirePlan(m, receipt.plan());
        WindowsWorldProvisioner.validateIdentityText(data.receiptFileIdentity);
        WindowsWorldProvisioner.validateIdentityText(data.configParentIdentity);
        WindowsWorldProvisioner.validateIdentityText(data.metadataFileIdentity);
        WindowsB1SourceAnchorStore.requireFilename(data.metadataFilename);
        WindowsB1SourceAnchorStore.requireNativeOutside(data.configParentNativePath, data.worldNativePath);
        var bodyBuffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bodyBuffer)) {
            text(out, PROTOCOL); blob(out, data.configuration);
            out.write(WindowsWorldProvisioner.digest(data.configuration));
            blob(out, data.receipt); out.write(WindowsWorldProvisioner.digest(data.receipt));
            text(out, data.receiptFileIdentity); text(out, data.configParentIdentity); text(out, data.metadataFileIdentity);
            text(out, data.configParentNativePath); text(out, data.worldNativePath); text(out, data.metadataFilename);
            text(out, namespace(m)); text(out, SLOT_PROTOCOL); text(out, SLOT0); text(out, SLOT1); text(out, SLOT_TMP);
        }
        byte[] body = bodyBuffer.toByteArray();
        if (body.length > MAX_BYTES - 48) throw new IOException("Anchor exceeds canonical 65536 byte bound");
        var result = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(result)) {
            out.writeLong(MAGIC); out.writeInt(VERSION); out.writeInt(body.length); out.write(body);
            out.write(WindowsWorldProvisioner.digest(body));
        }
        return result.toByteArray();
    }
    static Data decode(byte[] input) throws IOException {
        if (input == null || input.length < 48 || input.length > MAX_BYTES) throw new IOException("Invalid anchor full size");
        byte[] bytes = input.clone();
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readLong() != MAGIC || in.readInt() != VERSION) throw new IOException("Unknown anchor protocol/version");
            int size = in.readInt();
            if (size <= 0 || size != bytes.length - 48) throw new IOException("Invalid anchor full payload length");
            byte[] body = in.readNBytes(size), hash = in.readNBytes(32);
            requireExact(hash, WindowsWorldProvisioner.digest(body), "Anchor payload SHA mismatch");
            if (in.read() != -1) throw new IOException("Trailing anchor bytes");
            Data data;
            try (var fields = new DataInputStream(new ByteArrayInputStream(body))) {
                if (!PROTOCOL.equals(text(fields))) throw new IOException("Unknown anchor payload protocol");
                byte[] config = blob(fields); requireExact(fields.readNBytes(32), WindowsWorldProvisioner.digest(config), "Config digest conflict");
                byte[] receipt = blob(fields); requireExact(fields.readNBytes(32), WindowsWorldProvisioner.digest(receipt), "Receipt digest conflict");
                data = new Data(config, receipt, text(fields), text(fields), text(fields), text(fields), text(fields), text(fields));
                var m = configurationMetadata(config);
                if (!namespace(m).equals(text(fields)) || !SLOT_PROTOCOL.equals(text(fields)) ||
                    !SLOT0.equals(text(fields)) || !SLOT1.equals(text(fields)) || !SLOT_TMP.equals(text(fields)))
                    throw new IOException("Anchor namespace/fixed slot protocol conflict");
                if (fields.read() != -1) throw new IOException("Trailing anchor fields");
            }
            requireExact(bytes, encode(data), "Noncanonical anchor encoding");
            return data;
        } catch (IllegalArgumentException | NullPointerException failure) {
            throw new IOException("Invalid anchor canonical identity", failure);
        }
    }
    /** Explicit V2 internal configuration; the outer anchor V4 framing remains unchanged. */
    static byte[] configurationBytesV2(MySqlRuntimeMetadata.FullV2 m) {
        Map<String, String> v = new LinkedHashMap<>();
        v.put("backend", "mysql"); v.put("host", m.host()); v.put("port", Integer.toString(m.port()));
        v.put("schema", m.schema()); v.put("expectedServerUuid", m.expectedServerUuid().toString());
        v.put("expectedBindingUuid", m.expectedBindingUuid().toString());
        v.put("expectedEffectiveUser", m.expectedEffectiveUser()); v.put("username", m.username());
        v.put("pkcs12TrustStorePath", m.pkcs12TrustStorePath().toString());
        v.put("credentialSourceRef", "file:" + m.credentialSourceRef().absolutePath());
        v.put("trustSecretSourceRef", "file:" + m.trustSecretSourceRef().absolutePath());
        v.put("connectTimeoutMillis", Long.toString(m.connectTimeout().toMillis()));
        v.put("socketTimeoutMillis", Long.toString(m.socketTimeout().toMillis()));
        v.put("activeLimit", Integer.toString(m.activeLimit()));
        v.put("innodbFlushLogAtTrxCommit", Integer.toString(m.innodbFlushLogAtTrxCommit()));
        v.put("syncBinlog", Long.toString(m.syncBinlog()));
        var b = m.baseline();
        v.put("approvedBaselinePath", b.absolutePath().toString()); v.put("approvedBaselineSha256", b.fileSha256());
        v.put("approvedBaselineFormat", b.format()); v.put("approvedSchemaFormat", Integer.toString(b.schemaFormat()));
        v.put("approvedCoverage", b.coverage()); v.put("expectedServerVersion", b.serverVersion());
        v.put("expectedProvisioningUuid", b.provisioningUuid().toString()); v.put("approvedDdlSha256", b.ddlSha256());
        v.put("expectedAuditDefiner", b.auditDefiner()); v.put("approvedSourceCaptureSha256", b.sourceCaptureSha256());
        if (b.schemaFormat() == 3) {
            var contract = m.format3RuntimeContract();
            v.put("format3ConfigurationEncoding", "FP_B1_CONFIGURATION_FORMAT3_V2");
            v.put("format3RuntimeContractEncoding", "FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V2");
            v.put("format3ManifestConfigurationEncoding", m.format3ManifestConfigurationEncoding());
            v.put("format3RuntimeContractPath", contract.absolutePath().toString());
            v.put("format3RuntimeContractSha256", contract.fileSha256());
        }
        StringBuilder text = new StringBuilder();
        v.forEach((key, value) -> text.append(key).append('=').append(escape(value)).append('\n'));
        return text.toString().getBytes(StandardCharsets.UTF_8);
    }
    private static MySqlRuntimeMetadata.FullV2 configurationMetadataV2(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length > MAX_BYTES) throw new IOException("Invalid full V2 configuration size");
        String canonical = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        Properties fields = new Properties() {
            @Override public synchronized Object put(Object key, Object value) {
                if (containsKey(key)) throw new IllegalArgumentException("Duplicate V2 canonical configuration key");
                return super.put(key,value);
            }
        };
        try {
            fields.load(new StringReader(canonical));
            if (!"FP_B1_CONFIGURATION_FORMAT3_V2".equals(fields.remove("format3ConfigurationEncoding")) ||
                !"FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V2".equals(fields.remove("format3RuntimeContractEncoding")))
                throw new IOException("Unknown or missing V2 internal configuration markers");
            StringBuilder properties = new StringBuilder();
            for (String key : fields.stringPropertyNames())
                properties.append(key).append('=').append(escape(fields.getProperty(key))).append('\n');
            var metadata = MySqlRuntimeMetadata.fromBytesV2(properties.toString().getBytes(StandardCharsets.UTF_8));
            requireExact(bytes,configurationBytesV2(metadata),"Noncanonical V2 configuration identity");
            return metadata;
        } catch (IllegalArgumentException failure) { throw new IOException("Invalid V2 canonical anchor configuration",failure); }
    }
    static String canonicalConfigurationNamespaceV2(byte[] canonical) throws IOException {
        return namespaceV2(configurationMetadataV2(canonical));
    }
    private static String namespaceV2(MySqlRuntimeMetadata.FullV2 m) {
        return MySqlSessionBarrierNamespace.forIdentity(m.expectedServerUuid(),m.schema(),
            m.baseline().provisioningUuid(),m.expectedBindingUuid()).lockName();
    }
    private static void requirePlanV2(MySqlRuntimeMetadata.FullV2 m, WindowsWorldProvisioner.Plan p) throws IOException {
        if (m.baseline().schemaFormat() != 3 || p.schemaFormat() != 3 || p.physicalFormat() != 2 ||
            !p.mysqlServerUuid().equals(m.expectedServerUuid()) || !p.schema().equals(m.schema()) ||
            !p.bindingUuid().equals(m.expectedBindingUuid()) || !p.provisioningUuid().equals(m.baseline().provisioningUuid()) ||
            !p.ddlSha256().equalsIgnoreCase(m.baseline().ddlSha256())) throw new IOException("V2 anchor configuration and creation plan conflict");
    }

    static byte[] encodeV2(Data data) throws IOException {
        var m = configurationMetadataV2(data.configuration);
        requireExact(data.configuration, configurationBytesV2(m), "Noncanonical configuration identity");
        var receipt = WindowsWorldProvisioner.decodeReceipt(data.receipt);
        requirePlanV2(m, receipt.plan());
        WindowsWorldProvisioner.validateIdentityText(data.receiptFileIdentity);
        WindowsWorldProvisioner.validateIdentityText(data.configParentIdentity);
        WindowsWorldProvisioner.validateIdentityText(data.metadataFileIdentity);
        WindowsB1SourceAnchorStore.requireFilename(data.metadataFilename);
        WindowsB1SourceAnchorStore.requireNativeOutside(data.configParentNativePath, data.worldNativePath);
        var bodyBuffer = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bodyBuffer)) {
            text(out, PROTOCOL); blob(out, data.configuration);
            out.write(WindowsWorldProvisioner.digest(data.configuration));
            blob(out, data.receipt); out.write(WindowsWorldProvisioner.digest(data.receipt));
            text(out, data.receiptFileIdentity); text(out, data.configParentIdentity); text(out, data.metadataFileIdentity);
            text(out, data.configParentNativePath); text(out, data.worldNativePath); text(out, data.metadataFilename);
            text(out, namespaceV2(m)); text(out, SLOT_PROTOCOL); text(out, SLOT0); text(out, SLOT1); text(out, SLOT_TMP);
        }
        byte[] body = bodyBuffer.toByteArray();
        if (body.length > MAX_BYTES - 48) throw new IOException("Anchor exceeds canonical 65536 byte bound");
        var result = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(result)) {
            out.writeLong(MAGIC); out.writeInt(VERSION); out.writeInt(body.length); out.write(body);
            out.write(WindowsWorldProvisioner.digest(body));
        }
        return result.toByteArray();
    }
    static Data decodeV2(byte[] input) throws IOException {
        if (input == null || input.length < 48 || input.length > MAX_BYTES) throw new IOException("Invalid anchor full size");
        byte[] bytes = input.clone();
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readLong() != MAGIC || in.readInt() != VERSION) throw new IOException("Unknown anchor protocol/version");
            int size = in.readInt();
            if (size <= 0 || size != bytes.length - 48) throw new IOException("Invalid anchor full payload length");
            byte[] body = in.readNBytes(size), hash = in.readNBytes(32);
            requireExact(hash, WindowsWorldProvisioner.digest(body), "Anchor payload SHA mismatch");
            if (in.read() != -1) throw new IOException("Trailing anchor bytes");
            Data data;
            try (var fields = new DataInputStream(new ByteArrayInputStream(body))) {
                if (!PROTOCOL.equals(text(fields))) throw new IOException("Unknown anchor payload protocol");
                byte[] config = blob(fields); requireExact(fields.readNBytes(32), WindowsWorldProvisioner.digest(config), "Config digest conflict");
                byte[] receipt = blob(fields); requireExact(fields.readNBytes(32), WindowsWorldProvisioner.digest(receipt), "Receipt digest conflict");
                data = new Data(config, receipt, text(fields), text(fields), text(fields), text(fields), text(fields), text(fields));
                var m = configurationMetadataV2(config);
                if (!namespaceV2(m).equals(text(fields)) || !SLOT_PROTOCOL.equals(text(fields)) ||
                    !SLOT0.equals(text(fields)) || !SLOT1.equals(text(fields)) || !SLOT_TMP.equals(text(fields)))
                    throw new IOException("Anchor namespace/fixed slot protocol conflict");
                if (fields.read() != -1) throw new IOException("Trailing anchor fields");
            }
            requireExact(bytes, encodeV2(data), "Noncanonical anchor encoding");
            return data;
        } catch (IllegalArgumentException | NullPointerException failure) {
            throw new IOException("Invalid anchor canonical identity", failure);
        }
    }
    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] bytes;
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(value));
            bytes = new byte[encoded.remaining()]; encoded.get(bytes);
        } catch (java.nio.charset.CharacterCodingException failure) { throw new IOException("Invalid anchor UTF-8 text", failure); }
        if (bytes.length > MAX_TEXT) throw new IOException("Anchor text exceeds bound");
        blob(out, bytes);
    }
    private static String text(DataInputStream in) throws IOException {
        byte[] bytes = blob(in);
        if (bytes.length > MAX_TEXT) throw new IOException("Anchor text exceeds bound");
        try { return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString(); }
        catch (java.nio.charset.CharacterCodingException failure) { throw new IOException("Invalid anchor UTF-8 text", failure); }
    }
    private static void blob(DataOutputStream out, byte[] value) throws IOException { out.writeInt(value.length); out.write(value); }
    private static byte[] blob(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > MAX_BYTES || size > in.available()) throw new IOException("Invalid anchor field length");
        return in.readNBytes(size);
    }
    static void requireExact(byte[] expected, byte[] actual, String message) throws IOException {
        if (!Arrays.equals(expected, actual)) throw new IOException(message);
    }
}
