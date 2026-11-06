package com.wi.floatingpod.bootstrap;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.floatingpod.server.WindowsWorldProvisioner;
import com.wi.floatingpod.storage.mysql.MySqlAuditCoverage$;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Static birth-source pins. This is DATA only; it cannot prove current custody or authorize IO. */
public final class ProductionOrdinarySourceApproval {
    private static final int MAX_BYTES = 65_536;
    private static final Set<String> KEYS = Set.of(
        "approvalFormat", "creationUuid", "bootstrapUuid", "prepareUuid", "certifyUuid",
        "creationBootUuid", "creationBootGeneration", "bindingUuid", "physicalFormat",
        "serverUuid", "schema", "schemaFormat", "ddlSha256", "provisioningUuid",
        "canonicalReceiptLength", "canonicalReceiptSha256", "sourceWorldIdentity",
        "guardFileIdentity", "receiptFileIdentity", "anchorFileIdentity", "worldPath",
        "anchorPath", "anchorSha256", "configurationSha256", "metadataSha256",
        "baselineSha256", "runtimeContractSha256");

    /** Immutable original creation identity. No public constructor accepts caller observations as authority. */
    public static final class ApprovedOriginalCreation {
        private final WindowsWorldProvisioner.Plan plan;
        private final UUID creationBoot;
        private final long creationBootGeneration;
        private final int canonicalReceiptLength;
        private final String canonicalReceiptSha256;
        private final String sourceWorldIdentity, guardFileIdentity, receiptFileIdentity, anchorFileIdentity;
        private final Path worldPath, anchorPath;
        private final String anchorSha256, configurationSha256, metadataSha256, baselineSha256, runtimeContractSha256;

        private ApprovedOriginalCreation(WindowsWorldProvisioner.Plan plan, UUID creationBoot,
                long creationBootGeneration, int canonicalReceiptLength, String canonicalReceiptSha256,
                String sourceWorldIdentity, String guardFileIdentity, String receiptFileIdentity,
                String anchorFileIdentity, Path worldPath, Path anchorPath, String anchorSha256,
                String configurationSha256, String metadataSha256, String baselineSha256,
                String runtimeContractSha256) {
            this.plan = Objects.requireNonNull(plan);
            this.creationBoot = Objects.requireNonNull(creationBoot);
            this.creationBootGeneration = creationBootGeneration;
            this.canonicalReceiptLength = canonicalReceiptLength;
            this.canonicalReceiptSha256 = canonicalReceiptSha256;
            this.sourceWorldIdentity = sourceWorldIdentity;
            this.guardFileIdentity = guardFileIdentity;
            this.receiptFileIdentity = receiptFileIdentity;
            this.anchorFileIdentity = anchorFileIdentity;
            this.worldPath = worldPath;
            this.anchorPath = anchorPath;
            this.anchorSha256 = anchorSha256;
            this.configurationSha256 = configurationSha256;
            this.metadataSha256 = metadataSha256;
            this.baselineSha256 = baselineSha256;
            this.runtimeContractSha256 = runtimeContractSha256;
        }
        public WindowsWorldProvisioner.Plan plan() { return plan; }
        public UUID creationBootUuid() { return creationBoot; }
        public long creationBootGeneration() { return creationBootGeneration; }
        public int canonicalReceiptLength() { return canonicalReceiptLength; }
        public String canonicalReceiptSha256() { return canonicalReceiptSha256; }
        public String sourceWorldIdentity() { return sourceWorldIdentity; }
        public String guardFileIdentity() { return guardFileIdentity; }
        public String receiptFileIdentity() { return receiptFileIdentity; }
        public String anchorFileIdentity() { return anchorFileIdentity; }
        public Path worldPath() { return worldPath; }
        public Path anchorPath() { return anchorPath; }
        public String anchorSha256() { return anchorSha256; }
        public String configurationSha256() { return configurationSha256; }
        public String metadataSha256() { return metadataSha256; }
        public String baselineSha256() { return baselineSha256; }
        public String runtimeContractSha256() { return runtimeContractSha256; }
        @Override public String toString() { return "ApprovedOriginalCreation[redacted; static source DATA only]"; }
    }

    private ProductionOrdinarySourceApproval() {}

    /** Reads only bounded public source pins. It never opens native state, JDBC or secret sources. */
    public static ApprovedOriginalCreation loadPinned(Path path, String expectedSha256, Path controlPath,
            Path worldPath, MySqlRuntimeMetadata metadata, String metadataSha256,
            String configurationSha256, String runtimeContractSha256) throws IOException {
        Path approvedPath = absolute(path, "ordinary source approval");
        Path control = absolute(controlPath, "control file");
        Path world = absolute(worldPath, "ordinary world");
        if (approvedPath.startsWith(world) || control.startsWith(world))
            throw invalid("Controlled approval and control files must be outside the source world");
        Properties properties = readPinnedProperties(approvedPath, expectedSha256);
        Objects.requireNonNull(metadata, "actual metadata required");
        return validatePinned(properties, world, metadata.baseline(), metadata.expectedServerUuid(),
            metadata.expectedBindingUuid(), metadata.schema(), metadataSha256, configurationSha256, runtimeContractSha256);
    }

    /** 显式 FullV2 静态 source pins；不打开 native、B/K 或数据库。 */
    public static ApprovedOriginalCreation loadPinnedV2(Path path, String expectedSha256, Path controlPath,
            Path worldPath, MySqlRuntimeMetadata.FullV2 metadata, String metadataSha256,
            String configurationSha256, String runtimeContractSha256) throws IOException {
        Path approvedPath = absolute(path, "ordinary source approval");
        Path control = absolute(controlPath, "control file");
        Path world = absolute(worldPath, "ordinary world");
        if (approvedPath.startsWith(world) || control.startsWith(world))
            throw invalid("Controlled approval and control files must be outside the source world");
        Properties properties = readPinnedProperties(approvedPath, expectedSha256);
        for (String key : properties.stringPropertyNames()) requireUnicode(properties.getProperty(key));
        Objects.requireNonNull(metadata, "actual full V2 metadata required");
        return validatePinned(properties, world, metadata.baseline(), metadata.expectedServerUuid(),
            metadata.expectedBindingUuid(), metadata.schema(), metadataSha256, configurationSha256, runtimeContractSha256);
    }

    private static Properties readPinnedProperties(Path approvedPath, String expectedSha256) throws IOException {
        if (!sha(expectedSha256).equals(expectedSha256)) throw invalid("Uppercase source approval SHA256 required");
        byte[] raw = bounded(approvedPath, MAX_BYTES);
        if (!digest(raw).equals(expectedSha256)) throw invalid("Ordinary source approval raw SHA256 mismatch");
        String decoded = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString();
        if (decoded.startsWith("\uFEFF")) throw invalid("Ordinary source approval BOM is forbidden");
        Properties properties = new Properties() {
            @Override public synchronized Object put(Object key, Object value) {
                if (!(key instanceof String name) || !KEYS.contains(name) || containsKey(name))
                    throw invalid("Unknown or duplicate ordinary source approval key");
                return super.put(key, value);
            }
        };
        properties.load(new StringReader(decoded));
        if (!properties.stringPropertyNames().equals(KEYS)) throw invalid("Ordinary source approval keys are incomplete");
        exact(required(properties, "approvalFormat"), "FP_ORDINARY_SOURCE_APPROVAL_V1", "approval format");
        return properties;
    }

    private static ApprovedOriginalCreation validatePinned(Properties properties, Path world,
            MySqlRuntimeMetadata.Baseline baseline, UUID serverExpected, UUID bindingExpected, String schemaExpected,
            String metadataSha256, String configurationSha256, String runtimeContractSha256) {
        exactSha(required(properties, "metadataSha256"), metadataSha256, "approved metadata raw pin");
        exactSha(required(properties, "configurationSha256"), configurationSha256, "approved canonical configuration pin");
        exactSha(required(properties, "baselineSha256"), baseline.fileSha256(), "approved baseline raw pin");
        exactSha(required(properties, "runtimeContractSha256"), runtimeContractSha256, "approved runtime contract raw pin");
        UUID server = uuid(properties, "serverUuid");
        UUID binding = uuid(properties, "bindingUuid");
        UUID provisioning = uuid(properties, "provisioningUuid");
        String schema = required(properties, "schema");
        int schemaFormat = integer(properties, "schemaFormat", 3, 3);
        int physicalFormat = integer(properties, "physicalFormat", 2, 2);
        String ddlSha256 = sha(required(properties, "ddlSha256"));
        exact(server, serverExpected, "birth server UUID");
        exact(binding, bindingExpected, "birth physical binding");
        exact(provisioning, baseline.provisioningUuid(), "birth provisioning UUID");
        exact(schema, schemaExpected, "birth schema");
        exact(schemaFormat, baseline.schemaFormat(), "birth schema format");
        exact(ddlSha256, baseline.ddlSha256(), "birth DDL SHA");
        if (baseline.schemaFormat() != 3 || !Objects.equals(baseline.coverage(), MySqlAuditCoverage$.MODULE$.Format3()))
            throw invalid("Ordinary source approval requires the actual Format3 baseline");

        UUID creation = uuid(properties, "creationUuid");
        UUID bootstrap = uuid(properties, "bootstrapUuid");
        UUID prepare = uuid(properties, "prepareUuid");
        UUID certify = uuid(properties, "certifyUuid");
        if (Set.of(creation, bootstrap, prepare, certify).size() != 4)
            throw invalid("Original creation operation identities must be distinct");
        UUID creationBoot = uuid(properties, "creationBootUuid");
        long creationGeneration = number(properties, "creationBootGeneration");
        int receiptLength = integer(properties, "canonicalReceiptLength", 1, 65_536);
        String receiptSha = sha(required(properties, "canonicalReceiptSha256"));
        String sourceId = nativeIdentity(properties, "sourceWorldIdentity");
        String guardId = nativeIdentity(properties, "guardFileIdentity");
        String receiptId = nativeIdentity(properties, "receiptFileIdentity");
        String anchorId = nativeIdentity(properties, "anchorFileIdentity");
        Path approvedWorld = absolute(Path.of(required(properties, "worldPath")), "approved source world");
        Path anchorPath = absolute(Path.of(required(properties, "anchorPath")), "approved source anchor");
        exact(world, approvedWorld, "approved existing world path");
        String anchorSha = sha(required(properties, "anchorSha256"));
        String configSha = sha(required(properties, "configurationSha256"));
        WindowsWorldProvisioner.Plan plan = new WindowsWorldProvisioner.Plan(creation, bootstrap, prepare, certify,
            binding, physicalFormat, server, schema, schemaFormat, ddlSha256, provisioning);
        return new ApprovedOriginalCreation(plan, creationBoot, creationGeneration, receiptLength, receiptSha,
            sourceId, guardId, receiptId, anchorId, approvedWorld, anchorPath, anchorSha, configSha,
            sha(required(properties, "metadataSha256")), sha(required(properties, "baselineSha256")),
            sha(required(properties, "runtimeContractSha256")));
    }

    private static void requireUnicode(String value) {
        for (int i=0; i<value.length(); i++) {
            char c=value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i>=value.length() || !Character.isLowSurrogate(value.charAt(i))) throw invalid("Unpaired source approval Unicode surrogate");
            } else if (Character.isLowSurrogate(c)) throw invalid("Unpaired source approval Unicode surrogate");
        }
    }

    private static byte[] bounded(Path path, int limit) throws IOException {
        try (var input = Files.newInputStream(path)) {
            byte[] bytes = input.readNBytes(limit + 1);
            if (bytes.length > limit) throw new IOException("Ordinary source approval exceeds its byte bound");
            return bytes;
        }
    }
    private static String nativeIdentity(Properties p, String key) {
        String value = required(p, key);
        if (!value.matches("FP_WIN32_FILE_ID_V1;volume=[0-9a-f]{16};id=[0-9a-f]{32}"))
            throw invalid("Exact pinned native FileId required: " + key);
        return value;
    }
    private static Path absolute(Path p, String field) {
        if (p == null || !p.isAbsolute() || p.getParent() == null || p.getFileName() == null)
            throw invalid("Explicit absolute path required: " + field);
        return p.toAbsolutePath().normalize();
    }
    private static String required(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.isBlank() || value.strip().equalsIgnoreCase("UNSET") ||
            value.strip().equalsIgnoreCase("UNKNOWN") || value.indexOf('<') >= 0 || value.indexOf('>') >= 0 ||
            value.codePoints().anyMatch(c -> c < 32 || c == 127))
            throw invalid("Missing or invalid ordinary source approval field: " + key);
        return value;
    }
    private static UUID uuid(Properties p, String key) {
        String value = required(p, key);
        if (!value.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
            throw invalid("Exact UUID required: " + key);
        return UUID.fromString(value);
    }
    private static long number(Properties p, String key) {
        String value = required(p, key);
        if (!value.matches("[0-9]+")) throw invalid("Positive decimal required: " + key);
        long parsed = Long.parseLong(value);
        if (parsed <= 0) throw invalid("Positive decimal required: " + key);
        return parsed;
    }
    private static int integer(Properties p, String key, int min, int max) {
        long value = number(p, key);
        if (value < min || value > max) throw invalid("Integer outside approved bound: " + key);
        return (int)value;
    }
    private static String sha(String value) {
        if (value == null || !value.matches("[0-9A-F]{64}")) throw invalid("Uppercase SHA256 required");
        return value;
    }
    private static void exact(Object actual, Object expected, String field) {
        if (!Objects.equals(actual, expected)) throw invalid("Pinned ordinary source mismatch: " + field);
    }
    private static void exactSha(String actual, String expected, String field) {
        if (actual == null || expected == null || !actual.equalsIgnoreCase(expected))
            throw invalid("Pinned ordinary source SHA mismatch: " + field);
    }
    private static String digest(byte[] bytes) {
        try { return HexFormat.of().withUpperCase().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException cause) { throw new IllegalStateException("SHA256 unavailable", cause); }
    }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
