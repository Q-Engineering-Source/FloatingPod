package com.wi.floatingpod.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Optional;
import java.util.Map;
import java.util.HashMap;

/** 显式部署 metadata；纯 Java/JDK，不绑定 Scala/JDBC 或任何生产默认路径/端点。 */
public final class MySqlRuntimeMetadata {
    public static final int MAX_METADATA_BYTES = 64 * 1024;
    private static final String FORMAT = "FP_MYSQL_APPROVED_STATIC_V1";
    private static final String COVERAGE1 = "format1:all-fixed-schema-tables-and-static-objects;all-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed";
    private static final String COVERAGE2 = "format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed";
    private static final String COVERAGE3 = "format3:all-twenty-fixed-schema-tables-and-static-objects;all-sixty-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed";
    private static final Set<String> KEYS = Set.of("backend", "host", "port", "schema", "expectedServerUuid",
            "expectedBindingUuid", "expectedEffectiveUser", "username", "pkcs12TrustStorePath",
            "credentialSourceRef", "trustSecretSourceRef", "connectTimeoutMillis", "socketTimeoutMillis",
            "activeLimit", "innodbFlushLogAtTrxCommit", "syncBinlog", "approvedBaselinePath",
            "approvedBaselineSha256", "approvedBaselineFormat", "approvedSchemaFormat", "approvedCoverage",
            "expectedServerVersion", "expectedProvisioningUuid", "approvedDdlSha256", "expectedAuditDefiner",
            "approvedSourceCaptureSha256", "format3RuntimeContractPath", "format3RuntimeContractSha256");

    public record FileReference(Path absolutePath) {
        public FileReference {
            if (absolutePath == null || !absolutePath.isAbsolute()) throw invalid("file reference must be absolute");
        }
        @Override public String toString() { return "FileReference[redacted]"; }
    }
    public record Baseline(Path absolutePath, String fileSha256, String format, int schemaFormat,
                           String coverage, String serverVersion, UUID provisioningUuid,
                           String ddlSha256, String auditDefiner, String sourceCaptureSha256) {
        @Override public String toString() { return "MySqlBaselineMetadata[redacted]"; }
    }
    public record RuntimeContract(Path absolutePath, String fileSha256) {
        public RuntimeContract {
            if (absolutePath == null || !absolutePath.isAbsolute()) throw invalid("format3 runtime contract path must be absolute");
            if (fileSha256 == null || !fileSha256.matches("[0-9a-fA-F]{64}")) throw invalid("format3 runtime contract SHA-256 is malformed");
        }
        @Override public String toString() { return "MySqlRuntimeContract[redacted]"; }
    }
    private final String host, schema, expectedEffectiveUser, username;
    private final int port, activeLimit;
    private final UUID expectedServerUuid, expectedBindingUuid;
    private final Path pkcs12TrustStorePath;
    private final FileReference credentialSourceRef, trustSecretSourceRef;
    private final Duration connectTimeout, socketTimeout;
    private final Baseline baseline;
    private final RuntimeContract runtimeContract;

    public static final String MANIFEST_V2 = "FP_MYSQL_FORMAT3_MANIFEST_CONFIGURATION_BE_V2";
    public static final String MANIFEST_V3 = "FP_MYSQL_FORMAT3_MANIFEST_CONFIGURATION_BE_V3";
    public static final String DEPLOYMENT_RECEIPT_MANIFEST = "FP_MYSQL_DEPLOYMENT_RECEIPT1_MANIFEST_CONFIGURATION_BE_V1";
    public static final String DB_FIRST_MANIFEST = "FP_MYSQL_DB_FIRST1_MANIFEST_CONFIGURATION_BE_V1";
    public static final String DB_FIRST_COVERAGE = "script-publication1-deployment-receipt1-db-first1:exact27-tables-exact81-guards-auditself-and-privileges";
    private static final Set<String> PROJECTION_KEYS;
    static {
        var keys = new java.util.HashSet<>(KEYS);
        keys.removeAll(Set.of("approvedBaselineSha256", "approvedSourceCaptureSha256", "format3RuntimeContractSha256"));
        keys.add("format3ManifestConfigurationEncoding");
        PROJECTION_KEYS = Set.copyOf(keys);
    }
    /** 已验证的无后向 pin 部署语义；不代表完整 metadata、文件批准或 runtime 准入。 */
    public static final class DeploymentSemantics {
        private final String host, schema, expectedEffectiveUser, username;
        private final int port, activeLimit, schemaFormat;
        private final UUID expectedServerUuid, expectedBindingUuid, provisioningUuid;
        private final Path pkcs12TrustStorePath, baselinePath, contractPath;
        private final FileReference credentialSourceRef, trustSecretSourceRef;
        private final Duration connectTimeout, socketTimeout;
        private final String format, coverage, serverVersion, ddl, definer;
        private final java.util.Map<String,String> canonical;
        private DeploymentSemantics(Properties values, boolean manifest) { this(values, manifest, false); }
        private DeploymentSemantics(Properties values, boolean manifest, boolean deploymentReceipt) {
            this(values, manifest, deploymentReceipt ? 5 : 0);
        }
        private DeploymentSemantics(Properties values, boolean manifest, int schemaFamily) {
        boolean deploymentReceipt = schemaFamily == 5;
        boolean dbFirst = schemaFamily == 6;
        boolean extended = deploymentReceipt || dbFirst;
        String contractPrefix = dbFirst ? "dbFirst" : "deploymentReceipt";
            // Properties escapes are decoded after UTF-8 validation; reject ill-formed UTF-16 before minting V2 semantics.
            for (String key : values.stringPropertyNames()) {
                String value = values.getProperty(key);
                for (int i = 0; i < value.length(); i++) {
                    char current = value.charAt(i);
                    if (Character.isHighSurrogate(current)) {
                        if (++i >= value.length() || !Character.isLowSurrogate(value.charAt(i)))
                            throw invalid("V2 property contains an unpaired surrogate: " + key);
                    } else if (Character.isLowSurrogate(current))
                        throw invalid("V2 property contains an unpaired surrogate: " + key);
                }
            }
        if (!"mysql".equals(required(values, "backend"))) throw invalid("runtime backend must be mysql; SQLite requires explicit offline migration");
        host = required(values, "host");
        if (!validHost(host)) throw invalid("host is malformed");
        port = integer(values, "port", 1, 65535);
        schema = required(values, "schema");
        if (!schema.matches("[a-z][a-z0-9_]{0,63}")) throw invalid("schema is malformed");
        expectedServerUuid = uuid(values, "expectedServerUuid");
        expectedBindingUuid = uuid(values, "expectedBindingUuid");
        expectedEffectiveUser = account(values, "expectedEffectiveUser");
        username = account(values, "username");
        pkcs12TrustStorePath = absolutePath(required(values, "pkcs12TrustStorePath"));
        credentialSourceRef = fileReference(required(values, "credentialSourceRef"));
        trustSecretSourceRef = fileReference(required(values, "trustSecretSourceRef"));
        connectTimeout = Duration.ofMillis(integer(values, "connectTimeoutMillis", 1, 30_000));
        socketTimeout = Duration.ofMillis(integer(values, "socketTimeoutMillis", 1, 300_000));
        activeLimit = integer(values, "activeLimit", 1, 256);
        integer(values, "innodbFlushLogAtTrxCommit", 1, 1);
        integer(values, "syncBinlog", 1, 1);
        format = required(values, "approvedBaselineFormat");
        schemaFormat = integer(values, "approvedSchemaFormat", 1, extended ? schemaFamily : 4);
        coverage = required(values, "approvedCoverage");
        if (!FORMAT.equals(format) || !(schemaFormat == 1 && COVERAGE1.equals(coverage)
                || schemaFormat == 2 && COVERAGE2.equals(coverage)
                || schemaFormat == 3 && COVERAGE3.equals(coverage)
                || schemaFormat == 4 && "script-publication1:exact24-tables-exact72-guards-auditself-and-privileges".equals(coverage)
                || deploymentReceipt && schemaFormat == 5 && "script-publication1-deployment-receipt1:exact26-tables-exact78-guards-auditself-and-privileges".equals(coverage)
                || dbFirst && schemaFormat == 6 && DB_FIRST_COVERAGE.equals(coverage))) throw invalid("approved baseline profile is unsupported");
        serverVersion = required(values, "expectedServerVersion");
        if (!"9.7.2".equals(serverVersion)) throw invalid("expected MySQL server version is unsupported");

            baselinePath = absolutePath(required(values,"approvedBaselinePath"));
            provisioningUuid = uuid(values,"expectedProvisioningUuid");
            ddl = digest(values,"approvedDdlSha256");
            definer = account(values,"expectedAuditDefiner");
            if (extended && (schemaFormat != schemaFamily || !(dbFirst ? DB_FIRST_MANIFEST : DEPLOYMENT_RECEIPT_MANIFEST)
                    .equals(required(values,contractPrefix+"ManifestConfigurationEncoding"))))
                throw invalid("extended metadata requires its exact schema and manifest selector");
            boolean hasPath = values.containsKey("format3RuntimeContractPath");
            if (schemaFormat == 3 && !hasPath) throw invalid("format3 runtime contract path is required");
            if (schemaFormat != 3 && hasPath) throw invalid("format3 runtime contract pins are only valid for schema format 3");
            contractPath = extended ? absolutePath(required(values,contractPrefix+"RuntimeContractPath")) : hasPath ? absolutePath(required(values,"format3RuntimeContractPath")) : null;
            if (manifest && !extended && (schemaFormat != 3 || !(MANIFEST_V2.equals(required(values,"format3ManifestConfigurationEncoding")) || MANIFEST_V3.equals(required(values,"format3ManifestConfigurationEncoding")))))
                throw invalid("manifest V2 requires schema3 and exact actual selector");
            var map = new java.util.TreeMap<String,String>();
            for (String key: dbFirst ? DB_FIRST_PROJECTION_KEYS : deploymentReceipt ? DEPLOYMENT_RECEIPT_PROJECTION_KEYS : PROJECTION_KEYS) {
                if ("format3ManifestConfigurationEncoding".equals(key)) { if (manifest) map.put(key,required(values,key)); }
                else if (values.containsKey(key)) map.put(key,required(values,key));
            }
            map.put("port",Integer.toString(port)); map.put("activeLimit",Integer.toString(activeLimit));
            map.put("connectTimeoutMillis",Long.toString(connectTimeout.toMillis())); map.put("socketTimeoutMillis",Long.toString(socketTimeout.toMillis()));
            map.put("innodbFlushLogAtTrxCommit","1"); map.put("syncBinlog","1"); map.put("approvedSchemaFormat",Integer.toString(schemaFormat));
            map.put("expectedServerUuid",expectedServerUuid.toString()); map.put("expectedBindingUuid",expectedBindingUuid.toString());
            map.put("expectedProvisioningUuid",provisioningUuid.toString()); map.put("approvedDdlSha256",ddl.toUpperCase(java.util.Locale.ROOT));
            map.put("pkcs12TrustStorePath",pkcs12TrustStorePath.toString()); map.put("approvedBaselinePath",baselinePath.toString());
            map.put("credentialSourceRef","file:"+credentialSourceRef.absolutePath()); map.put("trustSecretSourceRef","file:"+trustSecretSourceRef.absolutePath());
            if (contractPath != null) map.put(extended ? contractPrefix+"RuntimeContractPath" : "format3RuntimeContractPath",contractPath.toString());
            canonical=java.util.Map.copyOf(map);
        }
        public java.util.Map<String,String> canonicalFields() { return canonical; }
        @Override public String toString() { return "DeploymentSemantics[redacted; no file approval]"; }
    }
    /** Caller owns input/workspace; values undergo the same real deployment validators, without invented pins. */
    public static DeploymentSemantics parseManifestV2Projection(java.util.Map<String,String> fields) {
        if (fields == null || !MANIFEST_V2.equals(fields.get("format3ManifestConfigurationEncoding"))) throw invalid("manifest V2 selector required");
        return parseManifestProjection(fields);
    }
    public static DeploymentSemantics parseManifestV3Projection(java.util.Map<String,String> fields) {
        if (fields == null || !MANIFEST_V3.equals(fields.get("format3ManifestConfigurationEncoding"))) throw invalid("manifest V3 selector required");
        return parseManifestProjection(fields);
    }

    /**
     * 已核验 schema6 配置的无后向文件 pin 语义。输入仍是完整、闭合的运行配置投影；
     * 此类型不读取文件、不生成静态基线、也不签发连接或运行资格。
     */
    public static DeploymentSemantics parseDbFirstManifestProjection(java.util.Map<String,String> fields) {
        if (fields == null || !DB_FIRST_MANIFEST.equals(fields.get("dbFirstManifestConfigurationEncoding")))
            throw invalid("schema6 manifest selector required");
        if (!fields.keySet().equals(DB_FIRST_PROJECTION_KEYS))
            throw invalid("schema6 metadata projection requires exact closed keys without file pins");
        Properties values = new Properties();
        fields.forEach((key, value) -> {
            if (key == null || value == null) throw invalid("null schema6 projection field");
            values.setProperty(key, value);
        });
        DeploymentSemantics result = new DeploymentSemantics(values, true, 6);
        if (!fields.equals(result.canonicalFields()))
            throw invalid("schema6 metadata projection is not canonical");
        return result;
    }
    private static DeploymentSemantics parseManifestProjection(java.util.Map<String,String> fields) {
        if (!fields.keySet().equals(PROJECTION_KEYS)) throw invalid("manifest metadata projection requires exact closed keys");
        Properties values=new Properties(); fields.forEach((k,v)->{if(v==null)throw invalid("null deployment value");values.setProperty(k,v);});
        DeploymentSemantics result = new DeploymentSemantics(values,true);
        if (!fields.equals(result.canonicalFields())) throw invalid("manifest metadata projection is not canonical");
        return result;
    }

    private static final Set<String> DEPLOYMENT_RECEIPT_KEYS;
    private static final Set<String> DEPLOYMENT_RECEIPT_PROJECTION_KEYS;
    private static final Set<String> DB_FIRST_KEYS;
    private static final Set<String> DB_FIRST_PROJECTION_KEYS;
    static {
        var keys = new java.util.HashSet<>(KEYS);
        keys.removeAll(Set.of("format3RuntimeContractPath", "format3RuntimeContractSha256"));
        keys.addAll(Set.of("deploymentReceiptRuntimeContractPath", "deploymentReceiptRuntimeContractSha256", "deploymentReceiptManifestConfigurationEncoding"));
        DEPLOYMENT_RECEIPT_KEYS = Set.copyOf(keys);
        keys.removeAll(Set.of("approvedBaselineSha256", "approvedSourceCaptureSha256", "deploymentReceiptRuntimeContractSha256"));
        DEPLOYMENT_RECEIPT_PROJECTION_KEYS = Set.copyOf(keys);
        var dbKeys = new java.util.HashSet<>(DEPLOYMENT_RECEIPT_KEYS);
        dbKeys.removeAll(Set.of("deploymentReceiptRuntimeContractPath", "deploymentReceiptRuntimeContractSha256", "deploymentReceiptManifestConfigurationEncoding"));
        dbKeys.addAll(Set.of("dbFirstRuntimeContractPath", "dbFirstRuntimeContractSha256", "dbFirstManifestConfigurationEncoding"));
        DB_FIRST_KEYS = Set.copyOf(dbKeys);
        dbKeys.removeAll(Set.of("approvedBaselineSha256", "approvedSourceCaptureSha256", "dbFirstRuntimeContractSha256"));
        DB_FIRST_PROJECTION_KEYS = Set.copyOf(dbKeys);
    }

    /** 完整 schema5 文件 DATA；不能转换为旧 runtime metadata 或 native Plan。 */
    public static final class DeploymentReceiptStatic {
        private final DeploymentSemantics deployment;
        private final Baseline baseline;
        private final RuntimeContract contract;
        private DeploymentReceiptStatic(Properties values) {
            if (!values.stringPropertyNames().equals(DEPLOYMENT_RECEIPT_KEYS)) throw invalid("schema5 metadata requires exact closed keys and actual pins");
            deployment = new DeploymentSemantics(values, true, true);
            baseline = new Baseline(absolutePath(required(values,"approvedBaselinePath")), digest(values,"approvedBaselineSha256"),
                    required(values,"approvedBaselineFormat"), 5, required(values,"approvedCoverage"), required(values,"expectedServerVersion"),
                    uuid(values,"expectedProvisioningUuid"), digest(values,"approvedDdlSha256"), account(values,"expectedAuditDefiner"), digest(values,"approvedSourceCaptureSha256"));
            contract = new RuntimeContract(absolutePath(required(values,"deploymentReceiptRuntimeContractPath")), digest(values,"deploymentReceiptRuntimeContractSha256"));
        }
        public DeploymentSemantics deploymentSemantics() { return deployment; }
        public Baseline baseline() { return baseline; }
        public RuntimeContract runtimeContract() { return contract; }
        public java.util.Map<String,String> completeCanonicalFields() {
            var values = new java.util.TreeMap<>(deployment.canonicalFields());
            values.put("approvedBaselineSha256", baseline.fileSha256().toUpperCase(java.util.Locale.ROOT));
            values.put("approvedSourceCaptureSha256", baseline.sourceCaptureSha256().toUpperCase(java.util.Locale.ROOT));
            values.put("deploymentReceiptRuntimeContractSha256", contract.fileSha256().toUpperCase(java.util.Locale.ROOT));
            return java.util.Map.copyOf(values);
        }
        @Override public String toString() { return "DeploymentReceiptStaticMetadata[DATA; no admission]"; }
    }

    public static DeploymentReceiptStatic fromDeploymentReceiptBytes(byte[] input) throws IOException {
        return new DeploymentReceiptStatic(readClosedProperties(input, DEPLOYMENT_RECEIPT_KEYS));
    }

    /** 独立 schema6 静态输入。文件 pin 不是当前连接的 audit 或 writer 资格。 */
    public static final class DbFirstStatic {
        private final DeploymentSemantics deployment;
        private final Baseline baseline;
        private final RuntimeContract contract;
        private DbFirstStatic(Properties values) {
            if (!values.stringPropertyNames().equals(DB_FIRST_KEYS)) throw invalid("schema6 metadata requires exact closed keys and actual pins");
            deployment = new DeploymentSemantics(values, true, 6);
            baseline = new Baseline(absolutePath(required(values,"approvedBaselinePath")), digest(values,"approvedBaselineSha256"),
                    required(values,"approvedBaselineFormat"), 6, required(values,"approvedCoverage"), required(values,"expectedServerVersion"),
                    uuid(values,"expectedProvisioningUuid"), digest(values,"approvedDdlSha256"), account(values,"expectedAuditDefiner"), digest(values,"approvedSourceCaptureSha256"));
            contract = new RuntimeContract(absolutePath(required(values,"dbFirstRuntimeContractPath")), digest(values,"dbFirstRuntimeContractSha256"));
        }
        public DeploymentSemantics deploymentSemantics() { return deployment; }
        public Baseline baseline() { return baseline; }
        public RuntimeContract runtimeContract() { return contract; }
        public java.util.Map<String,String> completeCanonicalFields() {
            var values = new java.util.TreeMap<>(deployment.canonicalFields());
            values.put("approvedBaselineSha256", baseline.fileSha256().toUpperCase(java.util.Locale.ROOT));
            values.put("approvedSourceCaptureSha256", baseline.sourceCaptureSha256().toUpperCase(java.util.Locale.ROOT));
            values.put("dbFirstRuntimeContractSha256", contract.fileSha256().toUpperCase(java.util.Locale.ROOT));
            return java.util.Map.copyOf(values);
        }
        @Override public String toString() { return "DbFirstStaticMetadata[DATA; no admission]"; }
    }

    public static DbFirstStatic fromDbFirstBytes(byte[] input) throws IOException {
        return new DbFirstStatic(readClosedProperties(input, DB_FIRST_KEYS));
    }

    private static Properties readClosedProperties(byte[] input, Set<String> allowedKeys) throws IOException {
        if (input == null || input.length > MAX_METADATA_BYTES) throw invalid("bounded metadata bytes required");
        byte[] bytes = input.clone();
        try {
            if (bytes.length >= 3 && bytes[0] == (byte)0xef && bytes[1] == (byte)0xbb && bytes[2] == (byte)0xbf) throw invalid("metadata UTF-8 BOM is not accepted");
            String decoded = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
            Properties values = new Properties() {
                @Override public synchronized Object put(Object key, Object value) {
                    if (!(key instanceof String name) || !allowedKeys.contains(name) || containsKey(key)) throw invalid("unknown or duplicate metadata property");
                    return super.put(key,value);
                }
            };
            values.load(new StringReader(decoded));
            return values;
        } finally { Arrays.fill(bytes,(byte)0); }
    }
    private MySqlRuntimeMetadata(Properties values) {
        if (!"mysql".equals(required(values, "backend"))) throw invalid("runtime backend must be mysql; SQLite requires explicit offline migration");
        host = required(values, "host");
        if (!validHost(host)) throw invalid("host is malformed");
        port = integer(values, "port", 1, 65535);
        schema = required(values, "schema");
        if (!schema.matches("[a-z][a-z0-9_]{0,63}")) throw invalid("schema is malformed");
        expectedServerUuid = uuid(values, "expectedServerUuid");
        expectedBindingUuid = uuid(values, "expectedBindingUuid");
        expectedEffectiveUser = account(values, "expectedEffectiveUser");
        username = account(values, "username");
        pkcs12TrustStorePath = absolutePath(required(values, "pkcs12TrustStorePath"));
        credentialSourceRef = fileReference(required(values, "credentialSourceRef"));
        trustSecretSourceRef = fileReference(required(values, "trustSecretSourceRef"));
        connectTimeout = Duration.ofMillis(integer(values, "connectTimeoutMillis", 1, 30_000));
        socketTimeout = Duration.ofMillis(integer(values, "socketTimeoutMillis", 1, 300_000));
        activeLimit = integer(values, "activeLimit", 1, 256);
        integer(values, "innodbFlushLogAtTrxCommit", 1, 1);
        integer(values, "syncBinlog", 1, 1);
        String format = required(values, "approvedBaselineFormat");
        int schemaFormat = integer(values, "approvedSchemaFormat", 1, 4);
        String coverage = required(values, "approvedCoverage");
        if (!FORMAT.equals(format) || !(schemaFormat == 1 && COVERAGE1.equals(coverage)
                || schemaFormat == 2 && COVERAGE2.equals(coverage)
                || schemaFormat == 3 && COVERAGE3.equals(coverage)
                || schemaFormat == 4 && "script-publication1:exact24-tables-exact72-guards-auditself-and-privileges".equals(coverage))) throw invalid("approved baseline profile is unsupported");
        String serverVersion = required(values, "expectedServerVersion");
        if (!"9.7.2".equals(serverVersion)) throw invalid("expected MySQL server version is unsupported");
        baseline = new Baseline(absolutePath(required(values, "approvedBaselinePath")), digest(values, "approvedBaselineSha256"),
                format, schemaFormat, coverage, serverVersion, uuid(values, "expectedProvisioningUuid"),
                digest(values, "approvedDdlSha256"), account(values, "expectedAuditDefiner"), digest(values, "approvedSourceCaptureSha256"));
        boolean hasContractPath = values.containsKey("format3RuntimeContractPath");
        boolean hasContractSha = values.containsKey("format3RuntimeContractSha256");
        if (schemaFormat == 3) {
            if (!hasContractPath || !hasContractSha) throw invalid("format3 runtime contract path and raw SHA-256 are required");
            runtimeContract = new RuntimeContract(absolutePath(required(values, "format3RuntimeContractPath")),
                    digest(values, "format3RuntimeContractSha256"));
        } else {
            if (hasContractPath || hasContractSha) throw invalid("format3 runtime contract pins are only valid for schema format 3");
            runtimeContract = null;
        }
    }

    /** Full V2 file DATA remains a different type from legacy runtime inputs; it grants no file approval. */
    public static final class FullV2 {
        private final MySqlRuntimeMetadata fields;
        private final DeploymentSemantics deployment;
        private FullV2(Properties values) {
            if (!values.stringPropertyNames().equals(FULL_V2_KEYS)) throw invalid("full V2 metadata requires exact closed keys and all actual pins");
            Map<String,String> projection = new HashMap<>();
            for (String key : PROJECTION_KEYS) projection.put(key, values.getProperty(key));
            deployment = MANIFEST_V3.equals(projection.get("format3ManifestConfigurationEncoding"))
                    ? parseManifestV3Projection(projection) : parseManifestV2Projection(projection);
            Properties legacy = new Properties();
            for (String key : KEYS) legacy.setProperty(key, values.getProperty(key));
            fields = new MySqlRuntimeMetadata(legacy);
        }
        public String format3ManifestConfigurationEncoding() { return deployment.canonicalFields().get("format3ManifestConfigurationEncoding"); }
        public DeploymentSemantics deploymentSemantics() { return deployment; }
        public String host() { return fields.host(); }
        public int port() { return fields.port(); }
        public String schema() { return fields.schema(); }
        public UUID expectedServerUuid() { return fields.expectedServerUuid(); }
        public UUID expectedBindingUuid() { return fields.expectedBindingUuid(); }
        public String expectedEffectiveUser() { return fields.expectedEffectiveUser(); }
        public String username() { return fields.username(); }
        public Path pkcs12TrustStorePath() { return fields.pkcs12TrustStorePath(); }
        public FileReference credentialSourceRef() { return fields.credentialSourceRef(); }
        public FileReference trustSecretSourceRef() { return fields.trustSecretSourceRef(); }
        public Duration connectTimeout() { return fields.connectTimeout(); }
        public Duration socketTimeout() { return fields.socketTimeout(); }
        public int activeLimit() { return fields.activeLimit(); }
        public int innodbFlushLogAtTrxCommit() { return fields.innodbFlushLogAtTrxCommit(); }
        public long syncBinlog() { return fields.syncBinlog(); }
        public Baseline baseline() { return fields.baseline(); }
        public RuntimeContract format3RuntimeContract() { return fields.format3RuntimeContract().orElseThrow(); }
        @Override public String toString() { return "FullV2Metadata[redacted; no file approval or runtime admission]"; }
    }
    private static final Set<String> FULL_V2_KEYS;
    static {
        var keys = new java.util.HashSet<>(KEYS);
        keys.add("format3ManifestConfigurationEncoding");
        FULL_V2_KEYS = Set.copyOf(keys);
    }
    public static FullV2 loadV2(Path explicitConfigurationPath) throws IOException {
        if (explicitConfigurationPath == null) throw invalid("explicit configuration Path is required");
        byte[] bytes = null;
        try (InputStream input = Files.newInputStream(explicitConfigurationPath)) {
            bytes = input.readNBytes(MAX_METADATA_BYTES + 1);
            return fromBytesV2(bytes);
        } finally { if (bytes != null) Arrays.fill(bytes, (byte) 0); }
    }
    /** Explicit V2 only: no missing selector fallback and no conversion to legacy runtime metadata. */
    public static FullV2 fromBytesV2(byte[] input) throws IOException {
        if (input == null || input.length > MAX_METADATA_BYTES) throw invalid("bounded full V2 metadata bytes are required");
        byte[] bytes = input.clone();
        try {
            if (bytes.length >= 3 && bytes[0] == (byte) 0xef && bytes[1] == (byte) 0xbb && bytes[2] == (byte) 0xbf)
                throw invalid("metadata UTF-8 BOM is not accepted");
            String decoded = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
            Properties values = new Properties() {
                @Override public synchronized Object put(Object key, Object value) {
                    if (!(key instanceof String name) || !FULL_V2_KEYS.contains(name)) throw invalid("unknown full V2 metadata property");
                    if (containsKey(key)) throw invalid("duplicate full V2 metadata property");
                    return super.put(key,value);
                }
            };
            values.load(new StringReader(decoded));
            return new FullV2(values);
        } finally { Arrays.fill(bytes,(byte)0); }
    }

    /** 不打开 trust/secret/baseline 文件或数据库；只读 caller 指定的 metadata file。 */
    public static MySqlRuntimeMetadata load(Path explicitConfigurationPath) throws IOException {
        if (explicitConfigurationPath == null) throw invalid("explicit configuration Path is required");
        byte[] bytes = null;
        try (InputStream input = Files.newInputStream(explicitConfigurationPath)) {
            bytes = input.readNBytes(MAX_METADATA_BYTES + 1);
            return fromBytes(bytes);
        } finally { if (bytes != null) Arrays.fill(bytes, (byte) 0); }
    }

    /** 解码实际 single-handle 读取结果；复制 caller bytes，使用同一严格 parser。 */
    public static MySqlRuntimeMetadata fromBytes(byte[] input) throws IOException {
        if (input == null) throw invalid("metadata bytes are required");
        if (input.length > MAX_METADATA_BYTES) throw invalid("metadata file exceeds 64 KiB limit");
        byte[] bytes = input.clone();
        try {
            if (bytes.length >= 3 && bytes[0] == (byte) 0xef && bytes[1] == (byte) 0xbb && bytes[2] == (byte) 0xbf)
                throw invalid("metadata UTF-8 BOM is not accepted");
            String decoded = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
            Properties properties = new Properties() {
                @Override public synchronized Object put(Object key, Object value) {
                    if ("sqlitePath".equals(key) || "sqlite".equals(key))
                        throw invalid("legacy SQLite metadata requires explicit offline migration; no runtime fallback");
                    if (!(key instanceof String name) || !KEYS.contains(name)) throw invalid("unknown runtime metadata property");
                    if (containsKey(key)) throw invalid("duplicate runtime metadata property");
                    return super.put(key, value);
                }
            };
            properties.load(new StringReader(decoded));
            return new MySqlRuntimeMetadata(properties);
        } finally { Arrays.fill(bytes, (byte) 0); }
    }
    /** file: 后是原样绝对 OS Path；不是 URI，不百分号解码、不猜Authority或工作目录。 */
    public static FileReference fileReference(String reference) {
        if (reference == null || !reference.startsWith("file:")) throw invalid("secret reference must use explicit file:absolute-path syntax");
        return new FileReference(absolutePath(reference.substring(5)));
    }
    private static Path absolutePath(String value) {
        try {
            Path path = Path.of(value);
            if (!path.isAbsolute()) throw invalid("configuration file path must be absolute");
            return path;
        } catch (java.nio.file.InvalidPathException cause) { throw new IllegalArgumentException("configuration file path is malformed", cause); }
    }
    private static String required(Properties values, String key) {
        String value = values.getProperty(key);
        if (value == null || value.isBlank() || value.strip().equalsIgnoreCase("UNKNOWN") || value.strip().equalsIgnoreCase("UNSET")
                || value.indexOf('<') >= 0 || value.indexOf('>') >= 0) throw invalid("missing or unknown required property: " + key);
        return value;
    }
    private static String account(Properties values, String key) {
        String value = required(values, key);
        if (value.codePoints().anyMatch(point -> point < 32 || point == 127)) throw invalid("account property is malformed: " + key);
        return value;
    }
    /** 与固定 public RC endpoint 的 literal syntax 一致；不做 DNS/网络查询。 */
    private static boolean validHost(String host) {
        if (host.length() > 253) return false;
        if (host.indexOf(':') >= 0) {
            if (host.indexOf('%') >= 0 || host.indexOf('[') >= 0 || host.indexOf(']') >= 0
                    || host.contains(":::") || host.indexOf(':') == host.lastIndexOf(':')) return false;
            int compression = host.indexOf("::");
            if (compression >= 0 && host.indexOf("::", compression + 2) >= 0) return false;
            if (host.startsWith(":") && !host.startsWith("::") || host.endsWith(":") && !host.endsWith("::")) return false;
            int groups = 0;
            for (String group : host.split(":", -1)) {
                if (group.isEmpty()) continue;
                if (!group.matches("[0-9a-fA-F]{1,4}")) return false;
                groups++;
            }
            return compression < 0 ? groups == 8 : groups < 8;
        }
        if (host.chars().allMatch(value -> value == '.' || value >= '0' && value <= '9')) {
            String[] parts = host.split("\\.", -1);
            if (parts.length != 4) return false;
            for (String part : parts) if (part.isEmpty() || part.length() > 3
                    || part.length() > 1 && part.charAt(0) == '0' || Integer.parseInt(part) > 255) return false;
            return true;
        }
        return host.matches("(?i)(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*");
    }
    private static int integer(Properties values, String key, int minimum, int maximum) {
        String value = required(values, key);
        if (!value.matches("[0-9]+")) throw invalid("numeric property is malformed: " + key);
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < minimum || parsed > maximum) throw invalid("numeric property is outside allowed range: " + key);
            return parsed;
        } catch (NumberFormatException cause) { throw new IllegalArgumentException("numeric property is out of range: " + key, cause); }
    }
    private static UUID uuid(Properties values, String key) {
        String value = required(values, key);
        if (!value.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) throw invalid("UUID property is malformed: " + key);
        return UUID.fromString(value);
    }
    private static String digest(Properties values, String key) {
        String value = required(values, key);
        if (!value.matches("[0-9a-fA-F]{64}")) throw invalid("SHA-256 property is malformed: " + key);
        return value;
    }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
    public String host() { return host; }
    public int port() { return port; }
    public String schema() { return schema; }
    public UUID expectedServerUuid() { return expectedServerUuid; }
    public UUID expectedBindingUuid() { return expectedBindingUuid; }
    public String expectedEffectiveUser() { return expectedEffectiveUser; }
    public String username() { return username; }
    public Path pkcs12TrustStorePath() { return pkcs12TrustStorePath; }
    public FileReference credentialSourceRef() { return credentialSourceRef; }
    public FileReference trustSecretSourceRef() { return trustSecretSourceRef; }
    public Duration connectTimeout() { return connectTimeout; }
    public Duration socketTimeout() { return socketTimeout; }
    public int activeLimit() { return activeLimit; }
    public int innodbFlushLogAtTrxCommit() { return 1; }
    public long syncBinlog() { return 1L; }
    public Baseline baseline() { return baseline; }
    public Optional<RuntimeContract> format3RuntimeContract() { return Optional.ofNullable(runtimeContract); }
    @Override public String toString() { return "MySqlRuntimeMetadata[redacted]"; }
}
