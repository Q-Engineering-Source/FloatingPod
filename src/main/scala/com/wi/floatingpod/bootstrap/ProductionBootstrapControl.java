package com.wi.floatingpod.bootstrap;

import com.wi.floatingpod.server.DbFirstStartupRetirementCodec;
import com.wi.floatingpod.server.DbFirstStartupRetirementCodec.RequestData;
import com.wi.floatingpod.storage.mysql.ManagedInstallationIdentity;
import java.util.Arrays;
import java.nio.file.NoSuchFileException;
import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.WindowsB1SourceAnchorStore;
import com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec;
import com.wi.floatingpod.storage.mysql.ManagedInstallationBundle;
import com.wi.floatingpod.storage.mysql.ManagedInstallationFiles;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** 独立 launch pin 验证的静态控制输入；不签发 Fresh/custody/SQL/Ready 权限。 */
public final class ProductionBootstrapControl {
    public enum Mode { FRESH, ORDINARY_CERTIFIED }
    public static final class OrdinaryRecoveryUnavailable extends IllegalStateException {
        public OrdinaryRecoveryUnavailable() { super("This bootstrap input does not contain a pinned ordinary Certified source approval"); }
    }
    public record LaunchContract(Path controlPath, String controlSha256) {
        public LaunchContract {
            controlPath = absolute(controlPath, "control path");
            if (controlSha256 == null || !controlSha256.matches("[0-9A-F]{64}"))
                throw invalid("Independently supplied uppercase control SHA256 required");
        }
        @Override public String toString() { return "ProductionLaunchContract[redacted; static approval only]"; }
    }
    public record FreshIdentity(UUID boot, long bootGeneration, long connectionGeneration,
            UUID submitOperation, UUID creation, UUID bootstrap, UUID prepare, UUID certify) {
        public FreshIdentity {
            Objects.requireNonNull(boot); Objects.requireNonNull(submitOperation);
            Objects.requireNonNull(creation); Objects.requireNonNull(bootstrap);
            Objects.requireNonNull(prepare); Objects.requireNonNull(certify);
            if (bootGeneration <= 0 || connectionGeneration <= 0) throw invalid("Positive explicit generations required");
            if (Set.of(boot, submitOperation, creation, bootstrap, prepare, certify).size() != 6)
                throw invalid("Distinct boot and phase identities required");
        }
        @Override public String toString() { return "ProductionFreshIdentity[redacted; not physical proof]"; }
    }
    public record OrdinaryIdentity(UUID boot, long bootGeneration, long connectionGeneration,
            UUID submitOperation, UUID handoffAttempt, UUID handoffOperation) {
        public OrdinaryIdentity {
            Objects.requireNonNull(boot); Objects.requireNonNull(submitOperation);
            Objects.requireNonNull(handoffAttempt); Objects.requireNonNull(handoffOperation);
            if (bootGeneration <= 0 || connectionGeneration <= 0) throw invalid("Positive explicit ordinary generations required");
            if (Set.of(boot, submitOperation, handoffAttempt, handoffOperation).size() != 4)
                throw invalid("Distinct current ordinary boot and handoff identities required");
        }
        @Override public String toString() { return "ProductionOrdinaryIdentity[redacted; current generation only]"; }
    }
    public record Policy(String callerRoles, String auditDefinerRoles, Duration receiptFreshness,
                         Duration writerLease, DedicatedBootstrapBridge.Limits limits) {
        public Policy {
            Objects.requireNonNull(callerRoles); Objects.requireNonNull(auditDefinerRoles); Objects.requireNonNull(limits);
            boundedDuration(receiptFreshness, "receipt freshness"); boundedDuration(writerLease, "writer lease");
            if (writerLease.toNanos() % 1000 != 0) throw invalid("Writer lease must have microsecond precision");
        }
        @Override public String toString() { return "ProductionBootstrapPolicy[redacted]"; }
    }
    private static final Set<String> V1_KEYS = Set.of("controlFormat", "mode", "metadataPath", "metadataSha256", "configurationSha256", "worldPath",
        "host", "port", "schema", "effectiveUser", "username", "serverUuid", "bindingUuid", "activeLimit",
        "connectTimeoutMillis", "socketTimeoutMillis", "baselinePath", "baselineSha256", "baselineFormat", "schemaFormat",
        "coverage", "serverVersion", "provisioningUuid", "ddlSha256", "auditDefiner", "sourceCaptureSha256",
        "bootUuid", "bootGeneration", "connectionGeneration", "submitOperationUuid", "creationUuid", "bootstrapUuid", "prepareUuid", "certifyUuid",
        "callerRoles", "auditDefinerRoles", "importState", "physicalFormat", "receiptFreshnessMillis", "leaseMillis",
        "preparationMillis", "claimMillis", "finalizationMillis", "sqlOuterSha256", "scalarOuterSha256", "scalarRuntimeVersion",
        "scalaLibrarySha256", "scalaParserSha256", "fugueOuterSha256", "fugueRuntimeVersion");
    private static final Set<String> V2_COMMON_KEYS = Set.of("controlFormat", "mode", "metadataPath", "metadataSha256", "configurationSha256", "worldPath",
        "host", "port", "schema", "effectiveUser", "username", "serverUuid", "bindingUuid", "activeLimit",
        "connectTimeoutMillis", "socketTimeoutMillis", "baselinePath", "baselineSha256", "baselineFormat", "schemaFormat",
        "coverage", "serverVersion", "provisioningUuid", "ddlSha256", "auditDefiner", "sourceCaptureSha256",
        "callerRoles", "auditDefinerRoles", "importState", "physicalFormat", "receiptFreshnessMillis", "leaseMillis",
        "preparationMillis", "claimMillis", "finalizationMillis", "sqlOuterSha256", "scalarOuterSha256", "scalarRuntimeVersion",
        "scalaLibrarySha256", "scalaParserSha256", "fugueOuterSha256", "fugueRuntimeVersion", "runtimeContractSha256");
    private static final Set<String> FRESH_IDENTITY_KEYS = Set.of("bootUuid", "bootGeneration", "connectionGeneration",
        "submitOperationUuid", "creationUuid", "bootstrapUuid", "prepareUuid", "certifyUuid");
    private static final Set<String> ORDINARY_IDENTITY_KEYS = Set.of("bootUuid", "bootGeneration", "connectionGeneration",
        "submitOperationUuid", "handoffAttemptUuid", "handoffOperationUuid", "ordinarySourceApprovalPath", "ordinarySourceApprovalSha256");
    private static final Set<String> ALL_KEYS = union(V1_KEYS, V2_COMMON_KEYS, FRESH_IDENTITY_KEYS, ORDINARY_IDENTITY_KEYS);
    private static final Set<UUID> USED_BOOT_IDENTITIES = new HashSet<>();
    private final LaunchContract contract;
    private final Mode mode;
    private final Path metadataPath, worldPath;
    private final String metadataSha256, configurationSha256;
    private final MySqlRuntimeMetadata metadata;
    private final FreshIdentity fresh;
    private final OrdinaryIdentity ordinary;
    private final ProductionOrdinarySourceApproval.ApprovedOriginalCreation originalCreation;
    private final Path ordinarySourceApprovalPath;
    private final String ordinarySourceApprovalSha256, runtimeContractSha256;
    private final int controlVersion;
    private final Policy policy;
    private final EarlyDependencyResolver.ExpectedPins dependencyPins;
    private boolean consumed;
    private ProductionBootstrapControl(LaunchContract contract, Mode mode, Path metadataPath, Path worldPath,
            String metadataSha256, String configurationSha256, MySqlRuntimeMetadata metadata,
            FreshIdentity fresh, OrdinaryIdentity ordinary,
            ProductionOrdinarySourceApproval.ApprovedOriginalCreation originalCreation,
            Path ordinarySourceApprovalPath, String ordinarySourceApprovalSha256, String runtimeContractSha256,
            int controlVersion, Policy policy, EarlyDependencyResolver.ExpectedPins pins) {
        this.contract=contract; this.mode=mode; this.metadataPath=metadataPath; this.worldPath=worldPath;
        this.metadataSha256=metadataSha256; this.configurationSha256=configurationSha256; this.metadata=metadata;
        this.fresh=fresh; this.ordinary=ordinary; this.originalCreation=originalCreation;
        this.ordinarySourceApprovalPath=ordinarySourceApprovalPath; this.ordinarySourceApprovalSha256=ordinarySourceApprovalSha256;
        this.runtimeContractSha256=runtimeContractSha256; this.controlVersion=controlVersion;
        this.policy=policy; this.dependencyPins=pins;
    }
    /** 有界真实控制/metadata/pack读取；不打开 credential/trust sources，不装配 worker/native。 */
    public static final String DEPLOYMENT_RECEIPT_FORMAT = "FP_PRODUCTION_BOOTSTRAP_CONTROL_DEPLOYMENT_RECEIPT1_V1";
    enum ProductionFormat { V2, V3, DEPLOYMENT_RECEIPT, DB_FIRST }
    /** 只发现独立pin对应的精确路由；完整typed load仍必须重新核pin/全部字段，不发行批准。 */
    static ProductionFormat discoverProductionFormat(LaunchContract contract) throws IOException {
        Objects.requireNonNull(contract);
        byte[] control = bounded(contract.controlPath(), 65536);
        exactSha(digest(control), contract.controlSha256(), "control");
        Properties values = new Properties() {
            @Override public synchronized Object put(Object key, Object value) {
                if (!(key instanceof String name) || containsKey(name)) throw invalid("Duplicate control discovery key");
                return super.put(key, value);
            }
        };
        String decoded = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(control)).toString();
        if (decoded.startsWith("\uFEFF")) throw invalid("Control BOM is forbidden");
        values.load(new StringReader(decoded));
        return switch (required(values, "controlFormat")) {
            case "FP_PRODUCTION_BOOTSTRAP_CONTROL_V2" -> ProductionFormat.V2;
            case "FP_PRODUCTION_BOOTSTRAP_CONTROL_V3" -> ProductionFormat.V3;
            case DEPLOYMENT_RECEIPT_FORMAT -> ProductionFormat.DEPLOYMENT_RECEIPT;
            case DB_FIRST_FORMAT -> ProductionFormat.DB_FIRST;
            default -> throw invalid("Unsupported exact production bootstrap control format");
        };
    }

    public static ProductionBootstrapControl load(LaunchContract contract) throws IOException {
        Objects.requireNonNull(contract);
        byte[] control = bounded(contract.controlPath(), 65536);
        exactSha(digest(control), contract.controlSha256(), "control");
        Properties p = new Properties() {
            @Override public synchronized Object put(Object key, Object value) {
                if (!(key instanceof String s) || !ALL_KEYS.contains(s) || containsKey(s)) throw invalid("Unknown or duplicate control key");
                return super.put(key,value);
            }
        };
        String decoded=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(control)).toString();
        if (decoded.startsWith("\uFEFF")) throw invalid("Control BOM is forbidden");
        p.load(new StringReader(decoded));
        String format=required(p,"controlFormat");
        int controlVersion;
        if (format.equals("FP_PRODUCTION_BOOTSTRAP_CONTROL_V1")) controlVersion=1;
        else if (format.equals("FP_PRODUCTION_BOOTSTRAP_CONTROL_V2")) controlVersion=2;
        else throw invalid("Unsupported exact production bootstrap control format");
        Mode mode;
        try { mode=Mode.valueOf(required(p,"mode")); }
        catch (IllegalArgumentException cause) { throw new IllegalArgumentException("Unsupported explicit bootstrap mode",cause); }
        Set<String> expectedKeys;
        if(controlVersion==1) {
            expectedKeys=V1_KEYS;
            if(mode==Mode.ORDINARY_CERTIFIED) throw new OrdinaryRecoveryUnavailable();
        } else {
            expectedKeys=union(V2_COMMON_KEYS,mode==Mode.FRESH?FRESH_IDENTITY_KEYS:ORDINARY_IDENTITY_KEYS);
        }
        requireExactKeys(p,expectedKeys);
        Path metadataPath=path(p,"metadataPath"), world=path(p,"worldPath");
        if (metadataPath.startsWith(world) || contract.controlPath().startsWith(world)) throw invalid("Controlled metadata/control must be outside world");
        String metadataSha=hash(p,"metadataSha256"), configurationSha=hash(p,"configurationSha256");
        byte[] raw=bounded(metadataPath,MySqlRuntimeMetadata.MAX_METADATA_BYTES);
        exactSha(digest(raw),metadataSha,"metadata");
        MySqlRuntimeMetadata m=MySqlRuntimeMetadata.fromBytes(raw);
        String runtimeContractSha=null;
        if(controlVersion==2) {
            runtimeContractSha=hash(p,"runtimeContractSha256");
            String actualRuntimeContractSha=m.format3RuntimeContract().orElseThrow(
                () -> invalid("V2 production control requires the exact Format3 runtime-contract raw pin")).fileSha256();
            exactSha(actualRuntimeContractSha,runtimeContractSha,"runtime contract raw SHA");
        }
        var b=m.baseline();
        exact(m.host(),required(p,"host"),"host"); exact(Integer.toString(m.port()),required(p,"port"),"port");
        exact(m.schema(),required(p,"schema"),"schema"); exact(m.expectedEffectiveUser(),required(p,"effectiveUser"),"effective user");
        exact(m.username(),required(p,"username"),"username");
        exact(m.expectedServerUuid(),uuid(p,"serverUuid"),"server UUID"); exact(m.expectedBindingUuid(),uuid(p,"bindingUuid"),"binding UUID");
        exact(m.activeLimit(),integer(p,"activeLimit",1,256),"active limit");
        exact(m.connectTimeout(),millis(p,"connectTimeoutMillis",30000),"connect timeout");
        exact(m.socketTimeout(),millis(p,"socketTimeoutMillis",300000),"socket timeout");
        exact(b.absolutePath().toAbsolutePath().normalize(),path(p,"baselinePath"),"baseline path");
        exactSha(b.fileSha256(),hash(p,"baselineSha256"),"baseline metadata SHA");
        exact(b.format(),required(p,"baselineFormat"),"baseline format");
        int schemaFormat=integer(p,"schemaFormat",controlVersion==2?3:2,controlVersion==2?3:2);
        exact(b.schemaFormat(),schemaFormat,"schema format");
        exact(b.coverage(),required(p,"coverage"),"baseline coverage"); exact(b.serverVersion(),required(p,"serverVersion"),"server version");
        exact(b.provisioningUuid(),uuid(p,"provisioningUuid"),"provisioning UUID");
        exactSha(b.ddlSha256(),hash(p,"ddlSha256"),"DDL SHA"); exact(b.auditDefiner(),required(p,"auditDefiner"),"audit definer");
        exactSha(b.sourceCaptureSha256(),hash(p,"sourceCaptureSha256"),"source capture SHA");
        exactSha(digest(bounded(b.absolutePath(),8*1024*1024)),hash(p,"baselineSha256"),"actual baseline pack");
        exact(required(p,"importState"),"NONE","Fresh import state"); exact(integer(p,"physicalFormat",2,2),2,"physical format");
        FreshIdentity fresh=null;
        OrdinaryIdentity ordinary=null;
        ProductionOrdinarySourceApproval.ApprovedOriginalCreation originalCreation=null;
        Path approvalPath=null;
        String approvalSha=null;
        if(mode==Mode.FRESH) {
            fresh=new FreshIdentity(uuid(p,"bootUuid"),number(p,"bootGeneration"),number(p,"connectionGeneration"),
                uuid(p,"submitOperationUuid"),uuid(p,"creationUuid"),uuid(p,"bootstrapUuid"),uuid(p,"prepareUuid"),uuid(p,"certifyUuid"));
        } else {
            if(controlVersion!=2) throw new OrdinaryRecoveryUnavailable();
            ordinary=new OrdinaryIdentity(uuid(p,"bootUuid"),number(p,"bootGeneration"),number(p,"connectionGeneration"),
                uuid(p,"submitOperationUuid"),uuid(p,"handoffAttemptUuid"),uuid(p,"handoffOperationUuid"));
            approvalPath=path(p,"ordinarySourceApprovalPath");
            approvalSha=hash(p,"ordinarySourceApprovalSha256");
            if(approvalPath.equals(metadataPath) || approvalPath.equals(contract.controlPath()))
                throw invalid("Ordinary source approval must be a separately pinned control artifact");
        }
        var policy=new Policy(required(p,"callerRoles"),required(p,"auditDefinerRoles"),millis(p,"receiptFreshnessMillis",1800000),
            millis(p,"leaseMillis",1800000),new DedicatedBootstrapBridge.Limits(millis(p,"preparationMillis",1800000),
                millis(p,"claimMillis",1800000),millis(p,"finalizationMillis",1800000)));
        var pins=new EarlyDependencyResolver.ExpectedPins(hash(p,"sqlOuterSha256"),
            Optional.of(new EarlyDependencyResolver.ScalarPins(hash(p,"scalarOuterSha256"),required(p,"scalarRuntimeVersion"),
                hash(p,"scalaLibrarySha256"),hash(p,"scalaParserSha256"))),
            Optional.of(new EarlyDependencyResolver.FuguePins(hash(p,"fugueOuterSha256"),required(p,"fugueRuntimeVersion"))));
        if(mode==Mode.ORDINARY_CERTIFIED) {
            originalCreation=ProductionOrdinarySourceApproval.loadPinned(approvalPath,approvalSha,contract.controlPath(),world,m,
                metadataSha,configurationSha,runtimeContractSha);
            Set<UUID> birth=Set.of(originalCreation.plan().creationOperationUuid(),originalCreation.plan().bootstrapOperationUuid(),
                originalCreation.plan().prepareWriteUuid(),originalCreation.plan().certifyWriteUuid(),originalCreation.creationBootUuid());
            if(Set.of(ordinary.boot(),ordinary.submitOperation(),ordinary.handoffAttempt(),ordinary.handoffOperation()).stream().anyMatch(birth::contains))
                throw invalid("Current ordinary identity reuses an original birth/source operation identity");
        }
        return new ProductionBootstrapControl(contract,mode,metadataPath,world,metadataSha,configurationSha,m,fresh,ordinary,
            originalCreation,approvalPath,approvalSha,runtimeContractSha,controlVersion,policy,pins);
    }
    /** 显式 V3 静态输入；不属于旧 Control 类型，不签发 root、IO、custody 或 Ready。 */
    public static final class StaticV3 {
        private final LaunchContract contract;
        private final Mode mode;
        private final Path metadataPath;
        private final Path worldPath;
        private final String metadataSha256;
        private final String configurationSha256;
        private final MySqlRuntimeMetadata.FullV2 metadata;
        private final SourceLimitProfile sourceLimitProfile;
        private final FreshIdentity fresh;
        private final OrdinaryIdentity ordinary;
        private final ProductionOrdinarySourceApproval.ApprovedOriginalCreation originalCreation;
        private final Path ordinarySourceApprovalPath;
        private final String ordinarySourceApprovalSha256;
        private final String runtimeContractSha256;
        private final Policy policy;
        private final EarlyDependencyResolver.ExpectedPins dependencyPins;
        private boolean consumed;
        private StaticV3(LaunchContract contract, Mode mode, Path metadataPath, Path worldPath, String metadataSha256, String configurationSha256, MySqlRuntimeMetadata.FullV2 metadata, SourceLimitProfile sourceLimitProfile, FreshIdentity fresh, OrdinaryIdentity ordinary, ProductionOrdinarySourceApproval.ApprovedOriginalCreation originalCreation, Path ordinarySourceApprovalPath, String ordinarySourceApprovalSha256, String runtimeContractSha256, Policy policy, EarlyDependencyResolver.ExpectedPins dependencyPins) {
            this.contract=contract;
            this.mode=mode;
            this.metadataPath=metadataPath;
            this.worldPath=worldPath;
            this.metadataSha256=metadataSha256;
            this.configurationSha256=configurationSha256;
            this.metadata=metadata;
            this.sourceLimitProfile=sourceLimitProfile;
            this.fresh=fresh;
            this.ordinary=ordinary;
            this.originalCreation=originalCreation;
            this.ordinarySourceApprovalPath=ordinarySourceApprovalPath;
            this.ordinarySourceApprovalSha256=ordinarySourceApprovalSha256;
            this.runtimeContractSha256=runtimeContractSha256;
            this.policy=policy;
            this.dependencyPins=dependencyPins;
        }
        public LaunchContract contract() { return contract; }
        public Mode mode() { return mode; }
        public Path metadataPath() { return metadataPath; }
        public Path worldPath() { return worldPath; }
        public String metadataSha256() { return metadataSha256; }
        public String configurationSha256() { return configurationSha256; }
        public MySqlRuntimeMetadata.FullV2 metadata() { return metadata; }
        public SourceLimitProfile sourceLimitProfile() { return sourceLimitProfile; }
        public FreshIdentity fresh() { return fresh; }
        public OrdinaryIdentity ordinary() { return ordinary; }
        public ProductionOrdinarySourceApproval.ApprovedOriginalCreation originalCreation() { return originalCreation; }
        public Path ordinarySourceApprovalPath() { return ordinarySourceApprovalPath; }
        public String ordinarySourceApprovalSha256() { return ordinarySourceApprovalSha256; }
        public String runtimeContractSha256() { return runtimeContractSha256; }
        public Policy policy() { return policy; }
        public EarlyDependencyResolver.ExpectedPins dependencyPins() { return dependencyPins; }
        public int controlVersion() { return 3; }
        public void requireFreshWorld(Path actualWorldPath) {
            if (mode != Mode.FRESH) throw new OrdinaryRecoveryUnavailable();
            exact(worldPath,absolute(actualWorldPath,"actual world"),"actual dedicated world path");
            if (Files.exists(worldPath,java.nio.file.LinkOption.NOFOLLOW_LINKS)) throw invalid("Fresh requires absent world; ordinary restart cannot mint Fresh");
        }
        public void requireOrdinaryWorld(Path actualWorldPath) {
            if (mode != Mode.ORDINARY_CERTIFIED || originalCreation == null) throw new OrdinaryRecoveryUnavailable();
            exact(worldPath,absolute(actualWorldPath,"actual ordinary world"),"actual dedicated world path");
            if (!Files.isDirectory(worldPath,java.nio.file.LinkOption.NOFOLLOW_LINKS)) throw invalid("Ordinary Certified restart requires exact existing source directory");
        }
        public void consumeFreshIdentityOnce() {
            if (mode != Mode.FRESH) throw new OrdinaryRecoveryUnavailable();
            synchronized (USED_BOOT_IDENTITIES) {
                if (consumed) throw invalid("Production control already consumed");
                Set<UUID> ids=Set.of(fresh.boot(),fresh.submitOperation(),fresh.creation(),fresh.bootstrap(),fresh.prepare(),fresh.certify());
                if (ids.stream().anyMatch(USED_BOOT_IDENTITIES::contains)) throw invalid("Boot/phase identity was already used in this process");
                USED_BOOT_IDENTITIES.addAll(ids);consumed=true;
            }
        }
        public void consumeOrdinaryIdentityOnce() {
            if (mode != Mode.ORDINARY_CERTIFIED || ordinary == null || originalCreation == null) throw new OrdinaryRecoveryUnavailable();
            synchronized (USED_BOOT_IDENTITIES) {
                if (consumed) throw invalid("Production control already consumed");
                Set<UUID> current=Set.of(ordinary.boot(),ordinary.submitOperation(),ordinary.handoffAttempt(),ordinary.handoffOperation());
                Set<UUID> birth=Set.of(originalCreation.plan().creationOperationUuid(),originalCreation.plan().bootstrapOperationUuid(),
                    originalCreation.plan().prepareWriteUuid(),originalCreation.plan().certifyWriteUuid(),originalCreation.creationBootUuid());
                if (current.stream().anyMatch(birth::contains) || current.stream().anyMatch(USED_BOOT_IDENTITIES::contains))
                    throw invalid("Current ordinary identity reuses original source or process identity");
                USED_BOOT_IDENTITIES.addAll(current);consumed=true;
            }
        }
        @Override public String toString() { return "StaticV3[redacted; B/K IO and runtime approval pending]"; }
    }

    /**
     * 根前有限bootstrap责任：control<=65536、metadata<=64KiB、approval<=65536；
     * 各readNBytes/UTF8 decoder/String/Properties字段图由这些实际输入界约束，不倒称root已付。
     * profile在parseHex前<=1024字符，codec仍要求实际97B；<=512B raw/clone、tag、long[8]、
     * recoding/SHA与初始carrier是此bootstrap输入责任。后续caller保留seed并在真IO root核B/K。
     */
    public static StaticV3 loadV3(LaunchContract contract) throws IOException {
        Objects.requireNonNull(contract);
        byte[] control=bounded(contract.controlPath(),65536);
        exactSha(digest(control),contract.controlSha256(),"control");
        Set<String> allowed=union(V2_COMMON_KEYS,FRESH_IDENTITY_KEYS,ORDINARY_IDENTITY_KEYS,
            Set.of("sourceLimitProfileCanonicalHex","sourceLimitProfileSha256"));
        Properties p=new Properties() {
            @Override public synchronized Object put(Object key,Object value) {
                if (!(key instanceof String name) || !allowed.contains(name) || containsKey(name)) throw invalid("Unknown or duplicate V3 control key");
                return super.put(key,value);
            }
        };
        String decoded=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(control)).toString();
        if (decoded.startsWith("\uFEFF")) throw invalid("Control BOM is forbidden");
        p.load(new StringReader(decoded));
        for (String key:p.stringPropertyNames()) requireV3Unicode(p.getProperty(key));
        exact(required(p,"controlFormat"),"FP_PRODUCTION_BOOTSTRAP_CONTROL_V3","explicit V3 selector");
        Mode mode;
        try { mode=Mode.valueOf(required(p,"mode")); }
        catch (IllegalArgumentException cause) { throw new IllegalArgumentException("Unsupported explicit bootstrap mode",cause); }
        requireExactKeys(p,union(V2_COMMON_KEYS,mode==Mode.FRESH?FRESH_IDENTITY_KEYS:ORDINARY_IDENTITY_KEYS,
            Set.of("sourceLimitProfileCanonicalHex","sourceLimitProfileSha256")));
        String encoded=required(p,"sourceLimitProfileCanonicalHex");
        if (encoded.length()>1024 || (encoded.length()&1)!=0 || !encoded.matches("[0-9a-fA-F]+"))
            throw invalid("V3 profile hex must be bounded even ASCII hexadecimal before decoding");
        byte[] canonical=HexFormat.of().parseHex(encoded);
        if (canonical.length>512) throw invalid("V3 profile decoded input exceeds 512 bytes");
        SourceLimitProfile profile=SourceLimitProfile.decode(canonical,HexFormat.of().parseHex(hash(p,"sourceLimitProfileSha256")));
        Path metadataPath=path(p,"metadataPath"),world=path(p,"worldPath");
        if (metadataPath.startsWith(world) || contract.controlPath().startsWith(world)) throw invalid("Controlled metadata/control must be outside world");
        String metadataSha=hash(p,"metadataSha256"),configurationSha=hash(p,"configurationSha256");
        byte[] raw=bounded(metadataPath,MySqlRuntimeMetadata.MAX_METADATA_BYTES);
        exactSha(digest(raw),metadataSha,"metadata");
        MySqlRuntimeMetadata.FullV2 m=MySqlRuntimeMetadata.fromBytesV2(raw);
        exactSha(WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(m).configurationSha256(),configurationSha,"complete V2 configuration");
        String runtimeContractSha=hash(p,"runtimeContractSha256");
        exactSha(m.format3RuntimeContract().fileSha256(),runtimeContractSha,"runtime contract raw SHA");
        var b=m.baseline();
        exact(m.host(),required(p,"host"),"host"); exact(Integer.toString(m.port()),required(p,"port"),"port");
        exact(m.schema(),required(p,"schema"),"schema"); exact(m.expectedEffectiveUser(),required(p,"effectiveUser"),"effective user");
        exact(m.username(),required(p,"username"),"username");
        exact(m.expectedServerUuid(),uuid(p,"serverUuid"),"server UUID"); exact(m.expectedBindingUuid(),uuid(p,"bindingUuid"),"binding UUID");
        exact(m.activeLimit(),integer(p,"activeLimit",1,256),"active limit");
        exact(m.connectTimeout(),millis(p,"connectTimeoutMillis",30000),"connect timeout");
        exact(m.socketTimeout(),millis(p,"socketTimeoutMillis",300000),"socket timeout");
        exact(b.absolutePath().toAbsolutePath().normalize(),path(p,"baselinePath"),"baseline path");
        exactSha(b.fileSha256(),hash(p,"baselineSha256"),"baseline metadata SHA");
        exact(b.format(),required(p,"baselineFormat"),"baseline format");
        int schemaFormat=integer(p,"schemaFormat",3,3);
        exact(b.schemaFormat(),schemaFormat,"schema format");
        exact(b.coverage(),required(p,"coverage"),"baseline coverage"); exact(b.serverVersion(),required(p,"serverVersion"),"server version");
        exact(b.provisioningUuid(),uuid(p,"provisioningUuid"),"provisioning UUID");
        exactSha(b.ddlSha256(),hash(p,"ddlSha256"),"DDL SHA"); exact(b.auditDefiner(),required(p,"auditDefiner"),"audit definer");
        exactSha(b.sourceCaptureSha256(),hash(p,"sourceCaptureSha256"),"source capture SHA");
        exact(required(p,"importState"),"NONE","Fresh import state"); exact(integer(p,"physicalFormat",2,2),2,"physical format");
        FreshIdentity fresh=null;
        OrdinaryIdentity ordinary=null;
        ProductionOrdinarySourceApproval.ApprovedOriginalCreation originalCreation=null;
        Path approvalPath=null;
        String approvalSha=null;
        if(mode==Mode.FRESH) {
            fresh=new FreshIdentity(uuid(p,"bootUuid"),number(p,"bootGeneration"),number(p,"connectionGeneration"),
                uuid(p,"submitOperationUuid"),uuid(p,"creationUuid"),uuid(p,"bootstrapUuid"),uuid(p,"prepareUuid"),uuid(p,"certifyUuid"));
        } else {
            ordinary=new OrdinaryIdentity(uuid(p,"bootUuid"),number(p,"bootGeneration"),number(p,"connectionGeneration"),
                uuid(p,"submitOperationUuid"),uuid(p,"handoffAttemptUuid"),uuid(p,"handoffOperationUuid"));
            approvalPath=path(p,"ordinarySourceApprovalPath");
            approvalSha=hash(p,"ordinarySourceApprovalSha256");
            if(approvalPath.equals(metadataPath) || approvalPath.equals(contract.controlPath()))
                throw invalid("Ordinary source approval must be a separately pinned control artifact");
        }
        var policy=new Policy(required(p,"callerRoles"),required(p,"auditDefinerRoles"),millis(p,"receiptFreshnessMillis",1800000),
            millis(p,"leaseMillis",1800000),new DedicatedBootstrapBridge.Limits(millis(p,"preparationMillis",1800000),
                millis(p,"claimMillis",1800000),millis(p,"finalizationMillis",1800000)));
        var pins=new EarlyDependencyResolver.ExpectedPins(hash(p,"sqlOuterSha256"),
            Optional.of(new EarlyDependencyResolver.ScalarPins(hash(p,"scalarOuterSha256"),required(p,"scalarRuntimeVersion"),
                hash(p,"scalaLibrarySha256"),hash(p,"scalaParserSha256"))),
            Optional.of(new EarlyDependencyResolver.FuguePins(hash(p,"fugueOuterSha256"),required(p,"fugueRuntimeVersion"))));
        if(mode==Mode.ORDINARY_CERTIFIED) {
            originalCreation=ProductionOrdinarySourceApproval.loadPinnedV2(approvalPath,approvalSha,contract.controlPath(),world,m,
                metadataSha,configurationSha,runtimeContractSha);
            Set<UUID> birth=Set.of(originalCreation.plan().creationOperationUuid(),originalCreation.plan().bootstrapOperationUuid(),
                originalCreation.plan().prepareWriteUuid(),originalCreation.plan().certifyWriteUuid(),originalCreation.creationBootUuid());
            if(Set.of(ordinary.boot(),ordinary.submitOperation(),ordinary.handoffAttempt(),ordinary.handoffOperation()).stream().anyMatch(birth::contains))
                throw invalid("Current ordinary identity reuses an original birth/source operation identity");
        }
        return new StaticV3(contract,mode,metadataPath,world,metadataSha,configurationSha,m,profile,fresh,ordinary,
            originalCreation,approvalPath,approvalSha,runtimeContractSha,policy,pins);
    }
    /** 原始来源静态 DATA；实际 receipt/native/custody 必须在原 IO owner 上另行核验。 */
    public record OriginalSourcePins(WindowsWorldProvisionReceiptDataCodec.PlanData plan,
            UUID creationBootUuid, long creationBootGeneration, int canonicalReceiptLength,
            String canonicalReceiptSha256, String sourceWorldIdentity, String guardFileIdentity,
            String receiptFileIdentity, String anchorFileIdentity, Path worldPath, Path anchorPath,
            String anchorSha256, String configurationSha256, String metadataSha256,
            String baselineSha256, String runtimeContractSha256) {
        @Override public String toString() { return "OriginalSourcePins[static DATA; no custody approval]"; }
    }
    private static final Set<String> ORIGINAL_SOURCE_KEYS = Set.of("approvalFormat", "creationUuid", "bootstrapUuid",
        "prepareUuid", "certifyUuid", "creationBootUuid", "creationBootGeneration", "bindingUuid", "physicalFormat",
        "serverUuid", "schema", "schemaFormat", "ddlSha256", "provisioningUuid", "canonicalReceiptLength",
        "canonicalReceiptSha256", "sourceWorldIdentity", "guardFileIdentity", "receiptFileIdentity", "anchorFileIdentity",
        "worldPath", "anchorPath", "anchorSha256", "configurationSha256", "metadataSha256", "baselineSha256", "runtimeContractSha256");
    /** Java early 阶段仅验证静态文件 pin；K/C/all11 语义由原 root 上的晚期 assembly 验证。 */
    public static final class DeploymentReceiptControl {
        private final LaunchContract contract;
        private final Mode mode;
        private final Path metadataPath, worldPath, ordinarySourceApprovalPath;
        private final String metadataSha256, configurationSha256, ordinarySourceApprovalSha256;
        private final MySqlRuntimeMetadata.DeploymentReceiptStatic metadata;
        private final SourceLimitProfile sourceLimitProfile;
        private final FreshIdentity fresh;
        private final OrdinaryIdentity ordinary;
        private final OriginalSourcePins originalSourcePins;
        private final Policy policy;
        private final EarlyDependencyResolver.ExpectedPins dependencyPins;
        private boolean consumed;
        private DeploymentReceiptControl(LaunchContract contract, Mode mode, Path metadataPath, Path worldPath,
                String metadataSha256, String configurationSha256, MySqlRuntimeMetadata.DeploymentReceiptStatic metadata,
                SourceLimitProfile profile, FreshIdentity fresh, OrdinaryIdentity ordinary, OriginalSourcePins original,
                Path approvalPath, String approvalSha, Policy policy, EarlyDependencyResolver.ExpectedPins pins) {
            this.contract=contract;this.mode=mode;this.metadataPath=metadataPath;this.worldPath=worldPath;
            this.metadataSha256=metadataSha256;this.configurationSha256=configurationSha256;this.metadata=metadata;
            sourceLimitProfile=profile;this.fresh=fresh;this.ordinary=ordinary;originalSourcePins=original;
            ordinarySourceApprovalPath=approvalPath;ordinarySourceApprovalSha256=approvalSha;this.policy=policy;dependencyPins=pins;
        }
        public LaunchContract contract(){return contract;} public Mode mode(){return mode;}
        public Path metadataPath(){return metadataPath;} public Path worldPath(){return worldPath;}
        public String metadataSha256(){return metadataSha256;} public String configurationSha256(){return configurationSha256;}
        public MySqlRuntimeMetadata.DeploymentReceiptStatic metadata(){return metadata;}
        public SourceLimitProfile sourceLimitProfile(){return sourceLimitProfile;}
        public FreshIdentity fresh(){return fresh;} public OrdinaryIdentity ordinary(){return ordinary;}
        public OriginalSourcePins originalSourcePins(){return originalSourcePins;}
        public Path ordinarySourceApprovalPath(){return ordinarySourceApprovalPath;}
        public String ordinarySourceApprovalSha256(){return ordinarySourceApprovalSha256;}
        public String runtimeContractSha256(){return metadata.runtimeContract().fileSha256();}
        public Policy policy(){return policy;} public EarlyDependencyResolver.ExpectedPins dependencyPins(){return dependencyPins;}
        public void requireFreshWorld(Path actual) {
            if(mode!=Mode.FRESH)throw new OrdinaryRecoveryUnavailable();
            exact(worldPath,absolute(actual,"actual world"),"actual world");
            if(Files.exists(worldPath,java.nio.file.LinkOption.NOFOLLOW_LINKS))throw invalid("Fresh requires absent world");
        }
        public void requireOrdinaryWorld(Path actual) {
            if(mode!=Mode.ORDINARY_CERTIFIED)throw new OrdinaryRecoveryUnavailable();
            exact(worldPath,absolute(actual,"actual world"),"actual world");
            if(!Files.isDirectory(worldPath,java.nio.file.LinkOption.NOFOLLOW_LINKS))throw invalid("Ordinary requires exact existing world directory");
        }
        public void consumeFreshIdentityOnce(){if(mode!=Mode.FRESH)throw new OrdinaryRecoveryUnavailable();consume();}
        public void consumeOrdinaryIdentityOnce(){if(mode!=Mode.ORDINARY_CERTIFIED)throw new OrdinaryRecoveryUnavailable();consume();}
        private void consume() {
            Set<UUID> ids=mode==Mode.FRESH?Set.of(fresh.boot(),fresh.submitOperation(),fresh.creation(),fresh.bootstrap(),fresh.prepare(),fresh.certify()):
                Set.of(ordinary.boot(),ordinary.submitOperation(),ordinary.handoffAttempt(),ordinary.handoffOperation());
            synchronized(USED_BOOT_IDENTITIES){
                if(consumed || ids.stream().anyMatch(USED_BOOT_IDENTITIES::contains))throw invalid("Production control identity already consumed");
                USED_BOOT_IDENTITIES.addAll(ids);consumed=true;
            }
        }
        @Override public String toString(){return "DeploymentReceiptControl[static pins; late semantic validation pending]";}
    }
    public static DeploymentReceiptControl loadDeploymentReceipt(LaunchContract contract) throws IOException {
        Objects.requireNonNull(contract);
        var profileKeys=Set.of("sourceLimitProfileCanonicalHex","sourceLimitProfileSha256");
        Properties p=deploymentProperties(contract.controlPath(),contract.controlSha256(),union(V2_COMMON_KEYS,FRESH_IDENTITY_KEYS,ORDINARY_IDENTITY_KEYS,profileKeys));
        exact(required(p,"controlFormat"),DEPLOYMENT_RECEIPT_FORMAT,"schema5 control selector");
        Mode mode=Mode.valueOf(required(p,"mode"));
        requireExactKeys(p,union(V2_COMMON_KEYS,mode==Mode.FRESH?FRESH_IDENTITY_KEYS:ORDINARY_IDENTITY_KEYS,profileKeys));
        String encoded=required(p,"sourceLimitProfileCanonicalHex");
        if(encoded.length()>1024 || (encoded.length()&1)!=0 || !encoded.matches("[0-9a-fA-F]+"))throw invalid("Bounded even profile hex required");
        SourceLimitProfile profile=SourceLimitProfile.decode(HexFormat.of().parseHex(encoded),HexFormat.of().parseHex(hash(p,"sourceLimitProfileSha256")));
        Path metadataPath=path(p,"metadataPath"),world=path(p,"worldPath");
        if(metadataPath.startsWith(world)||contract.controlPath().startsWith(world))throw invalid("Control and metadata must be outside world");
        String metadataSha=hash(p,"metadataSha256"),configurationSha=hash(p,"configurationSha256");
        byte[] raw=bounded(metadataPath,MySqlRuntimeMetadata.MAX_METADATA_BYTES);exactSha(digest(raw),metadataSha,"metadata");
        var m=MySqlRuntimeMetadata.fromDeploymentReceiptBytes(raw);
        exactSha(WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(m).configurationSha256(),configurationSha,"complete schema5 configuration");
        var fields=m.completeCanonicalFields();var b=m.baseline();
        String[][] mapping={{"host","host"},{"port","port"},{"schema","schema"},{"effectiveUser","expectedEffectiveUser"},{"username","username"},
            {"serverUuid","expectedServerUuid"},{"bindingUuid","expectedBindingUuid"},{"activeLimit","activeLimit"},
            {"connectTimeoutMillis","connectTimeoutMillis"},{"socketTimeoutMillis","socketTimeoutMillis"}};
        for(var pair:mapping)exact(required(p,pair[0]),fields.get(pair[1]),pair[0]);
        exact(path(p,"baselinePath"),b.absolutePath().toAbsolutePath().normalize(),"baseline path");
        exactSha(hash(p,"baselineSha256"),b.fileSha256(),"baseline SHA");
        exact(required(p,"baselineFormat"),b.format(),"baseline format");exact(integer(p,"schemaFormat",5,5),b.schemaFormat(),"schema format");
        exact(required(p,"coverage"),b.coverage(),"coverage");exact(required(p,"serverVersion"),b.serverVersion(),"server version");
        exact(uuid(p,"provisioningUuid"),b.provisioningUuid(),"provisioning UUID");exactSha(hash(p,"ddlSha256"),b.ddlSha256(),"DDL");
        exact(required(p,"auditDefiner"),b.auditDefiner(),"audit definer");exactSha(hash(p,"sourceCaptureSha256"),b.sourceCaptureSha256(),"capture");
        exactSha(hash(p,"runtimeContractSha256"),m.runtimeContract().fileSha256(),"K pin");
        requireStaticFilePin(b.absolutePath(),b.fileSha256());requireStaticFilePin(m.runtimeContract().absolutePath(),m.runtimeContract().fileSha256());
        exact(required(p,"importState"),"NONE","import state");integer(p,"physicalFormat",2,2);
        FreshIdentity fresh=null;OrdinaryIdentity ordinary=null;OriginalSourcePins original=null;Path approvalPath=null;String approvalSha=null;
        if(mode==Mode.FRESH)fresh=new FreshIdentity(uuid(p,"bootUuid"),number(p,"bootGeneration"),number(p,"connectionGeneration"),uuid(p,"submitOperationUuid"),uuid(p,"creationUuid"),uuid(p,"bootstrapUuid"),uuid(p,"prepareUuid"),uuid(p,"certifyUuid"));
        else {
            ordinary=new OrdinaryIdentity(uuid(p,"bootUuid"),number(p,"bootGeneration"),number(p,"connectionGeneration"),uuid(p,"submitOperationUuid"),uuid(p,"handoffAttemptUuid"),uuid(p,"handoffOperationUuid"));
            approvalPath=path(p,"ordinarySourceApprovalPath");approvalSha=hash(p,"ordinarySourceApprovalSha256");
            if(approvalPath.startsWith(world)||approvalPath.equals(metadataPath)||approvalPath.equals(contract.controlPath()))throw invalid("Separate outside-world original source pins required");
            Properties a=deploymentProperties(approvalPath,approvalSha,ORIGINAL_SOURCE_KEYS);requireExactKeys(a,ORIGINAL_SOURCE_KEYS);
            exact(required(a,"approvalFormat"),"FP_DEPLOYMENT_RECEIPT1_ORIGINAL_SOURCE_PINS_V1","original static pins selector");
            for(String key:new String[]{"serverUuid","bindingUuid","schema","schemaFormat","physicalFormat","ddlSha256","provisioningUuid","configurationSha256","metadataSha256","baselineSha256","runtimeContractSha256"})exact(required(a,key),required(p,key),"original "+key);
            exact(path(a,"worldPath"),world,"original world");
            UUID creationBoot=uuid(a,"creationBootUuid");long creationGeneration=number(a,"creationBootGeneration");
            var plan=new WindowsWorldProvisionReceiptDataCodec.PlanData(uuid(a,"creationUuid"),uuid(a,"bootstrapUuid"),uuid(a,"prepareUuid"),uuid(a,"certifyUuid"),uuid(a,"bindingUuid"),2,uuid(a,"serverUuid"),required(a,"schema"),5,hash(a,"ddlSha256"),uuid(a,"provisioningUuid"));
            Set<UUID> birthIds=Set.of(creationBoot,plan.creationOperationUuid(),plan.bootstrapOperationUuid(),plan.prepareWriteUuid(),plan.certifyWriteUuid());
            if(Set.of(ordinary.boot(),ordinary.submitOperation(),ordinary.handoffAttempt(),ordinary.handoffOperation()).stream().anyMatch(birthIds::contains))throw invalid("Current identity reuses original identity");
            Path anchor=path(a,"anchorPath");if(anchor.startsWith(world))throw invalid("Original anchor must be outside world");
            original=new OriginalSourcePins(plan,creationBoot,creationGeneration,integer(a,"canonicalReceiptLength",1,65536),hash(a,"canonicalReceiptSha256"),
                staticFileId(a,"sourceWorldIdentity"),staticFileId(a,"guardFileIdentity"),staticFileId(a,"receiptFileIdentity"),staticFileId(a,"anchorFileIdentity"),world,anchor,hash(a,"anchorSha256"),configurationSha,metadataSha,hash(a,"baselineSha256"),hash(a,"runtimeContractSha256"));
        }
        for(String key:mode==Mode.FRESH?FRESH_IDENTITY_KEYS:Set.of("bootUuid","submitOperationUuid","handoffAttemptUuid","handoffOperationUuid"))
            if(key.endsWith("Uuid") && uuid(p,key).equals(new UUID(0,0)))throw invalid("Nonzero schema5 identity required: "+key);
        var policy=new Policy(required(p,"callerRoles"),required(p,"auditDefinerRoles"),millis(p,"receiptFreshnessMillis",1800000),millis(p,"leaseMillis",1800000),new DedicatedBootstrapBridge.Limits(millis(p,"preparationMillis",1800000),millis(p,"claimMillis",1800000),millis(p,"finalizationMillis",1800000)));
        var pins=new EarlyDependencyResolver.ExpectedPins(hash(p,"sqlOuterSha256"),Optional.of(new EarlyDependencyResolver.ScalarPins(hash(p,"scalarOuterSha256"),required(p,"scalarRuntimeVersion"),hash(p,"scalaLibrarySha256"),hash(p,"scalaParserSha256"))),Optional.of(new EarlyDependencyResolver.FuguePins(hash(p,"fugueOuterSha256"),required(p,"fugueRuntimeVersion"))));
        return new DeploymentReceiptControl(contract,mode,metadataPath,world,metadataSha,configurationSha,m,profile,fresh,ordinary,original,approvalPath,approvalSha,policy,pins);
    }
    public static final String DB_FIRST_FORMAT = "FP_PRODUCTION_BOOTSTRAP_CONTROL_DB_FIRST1_V1";
    /** 独立启动操作身份；观察与获取writer不能复用已经消费的operation。 */
    public record DbFirstIdentity(UUID boot, long bootGeneration, long connectionGeneration,
            UUID observeOperation, UUID acquireOperation) {
        public DbFirstIdentity {
            Objects.requireNonNull(boot); Objects.requireNonNull(observeOperation); Objects.requireNonNull(acquireOperation);
            if (bootGeneration <= 0 || connectionGeneration <= 0) throw invalid("Positive schema6 generations required");
            var ids=Set.of(boot,observeOperation,acquireOperation);
            if(ids.contains(new UUID(0,0)))throw invalid("Nonzero distinct schema6 boot/operation identities required");
        }
        @Override public String toString(){return "DbFirstIdentity[static controlled identities]";}
    }
    /** 只有静态pin已核验；配置语义digest必须在原Session/root装配时核对，不能当Ready。 */
    public static final class DbFirstControl {
        private final LaunchContract contract;
        private final ManagedInstallationBundle.Loaded managedBundle;
        private final RequestData startupRetirementRequest;
        private final ManagedInstallationFiles startupRetirementFiles;
        private final Path worldPath;
        private final MySqlRuntimeMetadata.DbFirstStatic metadata;
        private final SourceLimitProfile profile;
        private final DbFirstIdentity identity;
        private final Policy policy;
        private final Duration barrierWait;
        private final String expectedConfigurationSha256;
        private final EarlyDependencyResolver.ExpectedPins dependencyPins;
        private boolean consumed;
        private DbFirstControl(LaunchContract contract,Path worldPath,MySqlRuntimeMetadata.DbFirstStatic metadata,
                SourceLimitProfile profile,DbFirstIdentity identity,Policy policy,Duration barrierWait,
                String expectedConfigurationSha256,EarlyDependencyResolver.ExpectedPins dependencyPins) {
            this(contract,worldPath,metadata,profile,identity,policy,barrierWait,expectedConfigurationSha256,dependencyPins,null);
        }
        private DbFirstControl(LaunchContract contract,Path worldPath,MySqlRuntimeMetadata.DbFirstStatic metadata,
                SourceLimitProfile profile,DbFirstIdentity identity,Policy policy,Duration barrierWait,
                String expectedConfigurationSha256,EarlyDependencyResolver.ExpectedPins dependencyPins,
                ManagedInstallationBundle.Loaded managedBundle) {
            this(contract,worldPath,metadata,profile,identity,policy,barrierWait,expectedConfigurationSha256,
                dependencyPins,managedBundle,null,null);
        }
        private DbFirstControl(LaunchContract contract,Path worldPath,MySqlRuntimeMetadata.DbFirstStatic metadata,
                SourceLimitProfile profile,DbFirstIdentity identity,Policy policy,Duration barrierWait,
                String expectedConfigurationSha256,EarlyDependencyResolver.ExpectedPins dependencyPins,
                ManagedInstallationBundle.Loaded managedBundle,
                RequestData startupRetirementRequest,ManagedInstallationFiles startupRetirementFiles) {
            this.contract=contract;this.worldPath=worldPath;this.metadata=metadata;this.profile=profile;
            this.identity=identity;this.policy=policy;this.barrierWait=barrierWait;
            this.expectedConfigurationSha256=expectedConfigurationSha256;this.dependencyPins=dependencyPins;
            this.managedBundle=managedBundle;
            this.startupRetirementRequest=startupRetirementRequest;
            this.startupRetirementFiles=startupRetirementFiles;
        }
        public LaunchContract contract(){
            if(managedBundle!=null)throw invalid("Managed bootstrap has a persisted installation bundle, not an operator launch contract");
            return contract;
        }
        public Optional<ManagedInstallationBundle.Loaded> managedBundle(){return Optional.ofNullable(managedBundle);}
        public Optional<RequestData> startupRetirementRequest(){
            return Optional.ofNullable(startupRetirementRequest);
        }
        /** 调用者必须先为固定上限的读取/codec预收预算；只读取实际 failed boot 的独立私有事故文件。 */
        public Optional<RequestData> readStartupRetirementRequest(UUID failedBoot) throws IOException {
            Objects.requireNonNull(failedBoot);
            if(startupRetirementRequest!=null && startupRetirementRequest.failedBoot().equals(failedBoot))
                return Optional.of(startupRetirementRequest);
            if(startupRetirementFiles==null || managedBundle==null) return Optional.empty();
            byte[] raw;
            try { raw=startupRetirementFiles.readPrivateFile("startup-retirement-incident-"+failedBoot+".bin",4096); }
            catch(NoSuchFileException absent){return Optional.empty();}
            byte[] canonical=null;
            try {
                var request=DbFirstStartupRetirementCodec.decodeRequest(raw);
                canonical=DbFirstStartupRetirementCodec.encodeRequest(request);
                if(!MessageDigest.isEqual(raw,canonical) || !request.failedBoot().equals(failedBoot))
                    throw invalid("Operator chained incident bytes/failed boot differ");
                validateStartupRequest(request,managedBundle.identity(),expectedConfigurationSha256);
                return Optional.of(request);
            } finally {
                Arrays.fill(raw,(byte)0);
                if(canonical!=null)Arrays.fill(canonical,(byte)0);
            }
        }
        public Path worldPath(){return worldPath;}
        public MySqlRuntimeMetadata.DbFirstStatic metadata(){return metadata;}
        public SourceLimitProfile sourceLimitProfile(){return profile;}
        public DbFirstIdentity identity(){return identity;}
        public Policy policy(){return policy;}
        public Duration barrierWait(){return barrierWait;}
        public String expectedConfigurationSha256(){return expectedConfigurationSha256;}
        public EarlyDependencyResolver.ExpectedPins dependencyPins(){return dependencyPins;}
        public void consumeIdentityOnce(){
            var ids=Set.of(identity.boot(),identity.observeOperation(),identity.acquireOperation());
            synchronized(USED_BOOT_IDENTITIES){
                if(consumed || ids.stream().anyMatch(USED_BOOT_IDENTITIES::contains))throw invalid("Schema6 control identity already consumed");
                USED_BOOT_IDENTITIES.addAll(ids);consumed=true;
            }
        }
        @Override public String toString(){return "DbFirstControl[static pins; semantic validation and SQL admission pending]";}
    }

    /**
     * 普通配置模式的本地 bundle 装配。这里重读固定受控载荷，不伪造独立 launch contract，
     * 不取得 SQL/native/Ready 权限；原 worker 仍须走完整 B/K/C、审计与 Acquire。
     */
    public static DbFirstControl createManaged(Path actualWorldPath, ManagedInstallationBundle.Loaded exactLoaded,
            ManagedInstallationFiles files, DbFirstIdentity bootIdentity, Policy versionedPolicy,
            Duration barrierWait, EarlyDependencyResolver.ExpectedPins verifiedPins,
            String actualManifestConfigurationSha256) throws IOException {
        return createManaged(actualWorldPath,exactLoaded,files,bootIdentity,versionedPolicy,barrierWait,verifiedPins,
            actualManifestConfigurationSha256,null);
    }
    public static DbFirstControl createManaged(Path actualWorldPath, ManagedInstallationBundle.Loaded exactLoaded,
            ManagedInstallationFiles files, DbFirstIdentity bootIdentity, Policy versionedPolicy,
            Duration barrierWait, EarlyDependencyResolver.ExpectedPins verifiedPins,
            String actualManifestConfigurationSha256,
            RequestData startupRetirementRequest) throws IOException {
        Objects.requireNonNull(exactLoaded);Objects.requireNonNull(files);Objects.requireNonNull(bootIdentity);
        Objects.requireNonNull(versionedPolicy);Objects.requireNonNull(barrierWait);Objects.requireNonNull(verifiedPins);
        Path world=absolute(actualWorldPath,"actual managed world");
        if(files.directory().startsWith(world) || world.startsWith(files.directory()))
            throw invalid("Managed installation bundle and world paths must remain disjoint");
        var reread=ManagedInstallationBundle.load(files,exactLoaded.identity())
                .orElseThrow(()->invalid("Managed active bundle is not published"));
        if(!reread.equals(exactLoaded))throw invalid("Managed active bundle changed before control assembly");
        if(actualManifestConfigurationSha256==null || !actualManifestConfigurationSha256.matches("[0-9a-fA-F]{64}") ||
                !actualManifestConfigurationSha256.equalsIgnoreCase(reread.configurationDigestSha256()))
            throw invalid("Managed configuration digest differs from the persisted bundle");
        if(!versionedPolicy.equals(ManagedBootstrapPolicy.policy(versionedPolicy.limits().preparation().toMillis())) ||
                !barrierWait.equals(ManagedBootstrapPolicy.barrierWait()) ||
                !verifiedPins.equals(ManagedReleaseDependencies.expectedPins()))
            throw invalid("Managed bootstrap policy or dependency release differs");
        byte[] raw=files.readPrivateFile(ManagedInstallationBundle.RUNTIME_METADATA_FILE,MySqlRuntimeMetadata.MAX_METADATA_BYTES);
        exactSha(digest(raw),reread.metadataSha256(),"managed metadata");
        var metadata=MySqlRuntimeMetadata.fromDbFirstBytes(raw);
        var fields=metadata.completeCanonicalFields();var baseline=metadata.baseline();
        var persisted=reread.identity();
        if(startupRetirementRequest!=null)validateStartupRequest(startupRetirementRequest,persisted,actualManifestConfigurationSha256);
        exact(fields.get("schema"),persisted.schema(),"managed schema");
        exact(fields.get("expectedServerUuid"),persisted.serverUuid().toString(),"managed server UUID");
        exact(fields.get("expectedBindingUuid"),persisted.bindingUuid().toString(),"managed binding UUID");
        exact(baseline.provisioningUuid(),persisted.provisioningUuid(),"managed provisioning UUID");
        exactSha(baseline.ddlSha256(),persisted.recipeSha256(),"managed release DDL");
        exact(baseline.absolutePath(),reread.baselinePath(),"managed baseline path");
        exact(metadata.runtimeContract().absolutePath(),reread.runtimeContractPath(),"managed runtime contract path");
        exactSha(baseline.fileSha256(),reread.baselineSha256(),"managed baseline");
        exactSha(baseline.sourceCaptureSha256(),reread.sourceCaptureSha256(),"managed source capture");
        exactSha(metadata.runtimeContract().fileSha256(),reread.runtimeContractSha256(),"managed runtime contract");
        requireStaticFilePin(baseline.absolutePath(),baseline.fileSha256());
        requireStaticFilePin(metadata.runtimeContract().absolutePath(),metadata.runtimeContract().fileSha256());
        return new DbFirstControl(null,world,metadata,ManagedBootstrapPolicy.sourceLimits(),bootIdentity,versionedPolicy,
                barrierWait,actualManifestConfigurationSha256.toUpperCase(java.util.Locale.ROOT),verifiedPins,reread,startupRetirementRequest,files);
    }
    private static void validateStartupRequest(RequestData request,
            ManagedInstallationIdentity.Record persisted,String configurationSha256) {
        if(!request.bootstrapOperationUuid().equals(persisted.bootstrapOperationUuid()) ||
            !request.serverUuid().equals(persisted.serverUuid()) || !request.bindingUuid().equals(persisted.bindingUuid()) ||
            !request.provisioningUuid().equals(persisted.provisioningUuid()) || !request.schema().equals(persisted.schema()) ||
            !request.configurationSha256().equalsIgnoreCase(configurationSha256) ||
            !request.recipeSha256().equalsIgnoreCase(persisted.recipeSha256()))
            throw invalid("Operator startup incident differs from the exact managed installation");
    }
    public static DbFirstControl loadDbFirst(LaunchContract contract)throws IOException {
        Objects.requireNonNull(contract);
        var common=new HashSet<>(V2_COMMON_KEYS);common.remove("mode");
        var keys=union(common,Set.of("sourceLimitProfileCanonicalHex","sourceLimitProfileSha256",
            "bootUuid","bootGeneration","connectionGeneration","observeOperationUuid","acquireOperationUuid","barrierWaitSeconds"));
        Properties p=deploymentProperties(contract.controlPath(),contract.controlSha256(),keys);
        requireExactKeys(p,keys);exact(required(p,"controlFormat"),DB_FIRST_FORMAT,"schema6 control selector");
        String encoded=required(p,"sourceLimitProfileCanonicalHex");
        if(encoded.length()>1024 || (encoded.length()&1)!=0 || !encoded.matches("[0-9a-fA-F]+"))throw invalid("Bounded even profile hex required");
        var profile=SourceLimitProfile.decode(HexFormat.of().parseHex(encoded),HexFormat.of().parseHex(hash(p,"sourceLimitProfileSha256")));
        Path metadataPath=path(p,"metadataPath"),world=path(p,"worldPath");
        if(metadataPath.startsWith(world)||contract.controlPath().startsWith(world))throw invalid("Control and metadata must be outside world");
        byte[] raw=bounded(metadataPath,MySqlRuntimeMetadata.MAX_METADATA_BYTES);
        exactSha(digest(raw),hash(p,"metadataSha256"),"metadata");
        var m=MySqlRuntimeMetadata.fromDbFirstBytes(raw);var fields=m.completeCanonicalFields();var b=m.baseline();
        String[][] mapping={{"host","host"},{"port","port"},{"schema","schema"},{"effectiveUser","expectedEffectiveUser"},{"username","username"},
            {"serverUuid","expectedServerUuid"},{"bindingUuid","expectedBindingUuid"},{"activeLimit","activeLimit"},
            {"connectTimeoutMillis","connectTimeoutMillis"},{"socketTimeoutMillis","socketTimeoutMillis"}};
        for(var pair:mapping)exact(required(p,pair[0]),fields.get(pair[1]),pair[0]);
        exact(path(p,"baselinePath"),b.absolutePath().toAbsolutePath().normalize(),"baseline path");
        exactSha(hash(p,"baselineSha256"),b.fileSha256(),"baseline SHA");
        exact(required(p,"baselineFormat"),b.format(),"baseline format");exact(integer(p,"schemaFormat",6,6),b.schemaFormat(),"schema format");
        exact(required(p,"coverage"),b.coverage(),"coverage");exact(required(p,"serverVersion"),b.serverVersion(),"server version");
        exact(uuid(p,"provisioningUuid"),b.provisioningUuid(),"provisioning UUID");exactSha(hash(p,"ddlSha256"),b.ddlSha256(),"DDL");
        exact(required(p,"auditDefiner"),b.auditDefiner(),"audit definer");exactSha(hash(p,"sourceCaptureSha256"),b.sourceCaptureSha256(),"capture");
        exactSha(hash(p,"runtimeContractSha256"),m.runtimeContract().fileSha256(),"K pin");
        requireStaticFilePin(b.absolutePath(),b.fileSha256());requireStaticFilePin(m.runtimeContract().absolutePath(),m.runtimeContract().fileSha256());
        exact(required(p,"importState"),"NONE","import state");integer(p,"physicalFormat",2,2);
        var identity=new DbFirstIdentity(uuid(p,"bootUuid"),number(p,"bootGeneration"),number(p,"connectionGeneration"),
            uuid(p,"observeOperationUuid"),uuid(p,"acquireOperationUuid"));
        var policy=new Policy(required(p,"callerRoles"),required(p,"auditDefinerRoles"),millis(p,"receiptFreshnessMillis",1800000),millis(p,"leaseMillis",1800000),
            new DedicatedBootstrapBridge.Limits(millis(p,"preparationMillis",1800000),millis(p,"claimMillis",1800000),millis(p,"finalizationMillis",1800000)));
        String waitText=required(p,"barrierWaitSeconds");
        if(!waitText.matches("(?:0|[1-9]|[12][0-9]|30)"))throw invalid("Schema6 barrier wait requires explicit whole seconds in 0..30");
        var wait=Duration.ofSeconds(Integer.parseInt(waitText));
        var pins=new EarlyDependencyResolver.ExpectedPins(hash(p,"sqlOuterSha256"),Optional.of(new EarlyDependencyResolver.ScalarPins(hash(p,"scalarOuterSha256"),required(p,"scalarRuntimeVersion"),hash(p,"scalaLibrarySha256"),hash(p,"scalaParserSha256"))),Optional.of(new EarlyDependencyResolver.FuguePins(hash(p,"fugueOuterSha256"),required(p,"fugueRuntimeVersion"))));
        return new DbFirstControl(contract,world,m,profile,identity,policy,wait,hash(p,"configurationSha256"),pins);
    }
    private static Properties deploymentProperties(Path path,String sha,Set<String> allowed)throws IOException {
        byte[] raw=bounded(path,65536);exactSha(digest(raw),sha,"controlled static file");
        Properties p=new Properties(){@Override public synchronized Object put(Object key,Object value){if(!(key instanceof String name)||!allowed.contains(name)||containsKey(name))throw invalid("Unknown or duplicate controlled static key");return super.put(key,value);}};
        String decoded=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString();
        if(decoded.startsWith("\uFEFF"))throw invalid("Control BOM is forbidden");p.load(new StringReader(decoded));
        for(String key:p.stringPropertyNames())requireV3Unicode(p.getProperty(key));return p;
    }
    private static String staticFileId(Properties p,String key){String value=required(p,key);if(!value.matches("FP_WIN32_FILE_ID_V1;volume=[0-9a-f]{16};id=[0-9a-f]{32}"))throw invalid("Canonical original FileId required: "+key);return value;}
    private static void requireStaticFilePin(Path path,String sha)throws IOException {
        var attr=Files.readAttributes(path,java.nio.file.attribute.BasicFileAttributes.class,java.nio.file.LinkOption.NOFOLLOW_LINKS);
        if(!attr.isRegularFile()||attr.isSymbolicLink()||attr.size()>16L*1024*1024)throw invalid("Bounded regular B/K file required");
        try {var digest=MessageDigest.getInstance("SHA-256");long count=0;byte[] buffer=new byte[8192];
            try(var input=Files.newInputStream(path,java.nio.file.LinkOption.NOFOLLOW_LINKS)){int n;while((n=input.read(buffer))!=-1){count+=n;if(count>16L*1024*1024)throw invalid("B/K exceeds file bound");digest.update(buffer,0,n);}}
            exactSha(HexFormat.of().withUpperCase().formatHex(digest.digest()),sha,"B/K file");
        }catch(NoSuchAlgorithmException cause){throw new IllegalStateException("SHA256 unavailable",cause);}
    }
    private static void requireV3Unicode(String value) {
        for (int i=0;i<value.length();i++) {
            char c=value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i>=value.length() || !Character.isLowSurrogate(value.charAt(i))) throw invalid("Unpaired V3 control Unicode surrogate");
            } else if (Character.isLowSurrogate(c)) throw invalid("Unpaired V3 control Unicode surrogate");
        }
    }

    /** 非 game owner 只用 actual world context 决定路径；不从目录/DB学习批准目标。 */
    public void requireFreshWorld(Path actualWorldPath) {
        if (mode != Mode.FRESH) throw new OrdinaryRecoveryUnavailable();
        exact(worldPath,absolute(actualWorldPath,"actual world"),"actual dedicated world path");
        if (Files.exists(worldPath, java.nio.file.LinkOption.NOFOLLOW_LINKS)) throw invalid("Fresh requires absent world; ordinary restart cannot mint Fresh");
    }
    /** Existing path check only; native custody/source authority is issued later by the actual IO worker. */
    public void requireOrdinaryWorld(Path actualWorldPath) {
        if (mode != Mode.ORDINARY_CERTIFIED || originalCreation == null) throw new OrdinaryRecoveryUnavailable();
        exact(worldPath,absolute(actualWorldPath,"actual ordinary world"),"actual dedicated world path");
        if (!Files.isDirectory(worldPath,java.nio.file.LinkOption.NOFOLLOW_LINKS))
            throw invalid("Ordinary Certified restart requires the exact existing source world directory");
    }
    /** 每进程一次消费所有固定 phase IDs；失败不可重试/重包装同控制身份。 */
    public void consumeFreshIdentityOnce() {
        if (mode != Mode.FRESH) throw new OrdinaryRecoveryUnavailable();
        synchronized (USED_BOOT_IDENTITIES) {
            if (consumed) throw invalid("Production control already consumed");
            Set<UUID> ids=Set.of(fresh.boot(),fresh.submitOperation(),fresh.creation(),fresh.bootstrap(),fresh.prepare(),fresh.certify());
            if (ids.stream().anyMatch(USED_BOOT_IDENTITIES::contains)) throw invalid("Boot/phase identity was already used in this process");
            USED_BOOT_IDENTITIES.addAll(ids); consumed=true;
        }
    }
    /** Burns only this launch's current identity tuple; original birth IDs remain pinned source DATA. */
    public void consumeOrdinaryIdentityOnce() {
        if(mode!=Mode.ORDINARY_CERTIFIED || ordinary==null || originalCreation==null) throw new OrdinaryRecoveryUnavailable();
        synchronized(USED_BOOT_IDENTITIES) {
            if(consumed) throw invalid("Production control already consumed");
            Set<UUID> current=Set.of(ordinary.boot(),ordinary.submitOperation(),ordinary.handoffAttempt(),ordinary.handoffOperation());
            Set<UUID> birth=Set.of(originalCreation.plan().creationOperationUuid(),originalCreation.plan().bootstrapOperationUuid(),
                originalCreation.plan().prepareWriteUuid(),originalCreation.plan().certifyWriteUuid(),originalCreation.creationBootUuid());
            if(current.stream().anyMatch(birth::contains) || current.stream().anyMatch(USED_BOOT_IDENTITIES::contains))
                throw invalid("Current ordinary identity was reused or collides with original source identity");
            USED_BOOT_IDENTITIES.addAll(current); consumed=true;
        }
    }
    public LaunchContract contract(){return contract;} public Mode mode(){return mode;}
    public Path metadataPath(){return metadataPath;} public Path worldPath(){return worldPath;}
    public String metadataSha256(){return metadataSha256;} public String configurationSha256(){return configurationSha256;}
    public MySqlRuntimeMetadata metadata(){return metadata;} public FreshIdentity fresh(){return fresh;}
    public OrdinaryIdentity ordinary(){return ordinary;}
    public ProductionOrdinarySourceApproval.ApprovedOriginalCreation originalCreation(){return originalCreation;}
    public Path ordinarySourceApprovalPath(){return ordinarySourceApprovalPath;}
    public String ordinarySourceApprovalSha256(){return ordinarySourceApprovalSha256;}
    public String runtimeContractSha256(){return runtimeContractSha256;}
    public int controlVersion(){return controlVersion;}
    public boolean isProductionFormatV2(){return controlVersion==2;}
    public Policy policy(){return policy;} public EarlyDependencyResolver.ExpectedPins dependencyPins(){return dependencyPins;}
    @Override public String toString(){return "ProductionBootstrapControl[verified static pin; redacted; not Fresh/Ready]";}
    private static byte[] bounded(Path p,int max)throws IOException {try(var in=Files.newInputStream(p)){byte[] b=in.readNBytes(max+1);if(b.length>max)throw new IOException("Controlled artifact exceeds bound");return b;}}
    private static Path absolute(Path p,String field){if(p==null||!p.isAbsolute()||p.getParent()==null||p.getFileName()==null)throw invalid("Explicit absolute file/world path required: "+field);return p.toAbsolutePath().normalize();}
    private static Path path(Properties p,String k){return absolute(Path.of(required(p,k)),k);}
    private static String required(Properties p,String k){String s=p.getProperty(k);if(s==null||s.isBlank()||s.strip().equalsIgnoreCase("UNSET")||s.strip().equalsIgnoreCase("UNKNOWN")||s.indexOf('<')>=0||s.indexOf('>')>=0||s.codePoints().anyMatch(c->c<32||c==127))throw invalid("Missing/invalid explicit control key: "+k);return s;}
    private static void requireExactKeys(Properties p,Set<String> expected) {
        if(!p.stringPropertyNames().equals(expected)) throw invalid("Control fields do not exactly match the selected format and mode");
        for(String key:expected) required(p,key);
    }
    @SafeVarargs private static Set<String> union(Set<String>... sets) {
        Set<String> result=new HashSet<>();for(Set<String> set:sets)result.addAll(set);return Set.copyOf(result);
    }
    private static String hash(Properties p,String k){String s=required(p,k);if(!s.matches("[0-9A-F]{64}"))throw invalid("Uppercase SHA256 required: "+k);return s;}
    private static UUID uuid(Properties p,String k){String s=required(p,k);if(!s.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))throw invalid("Exact UUID required: "+k);return UUID.fromString(s);}
    private static long number(Properties p,String k){String s=required(p,k);if(!s.matches("[0-9]+"))throw invalid("Positive integer required: "+k);long n=Long.parseLong(s);if(n<=0)throw invalid("Positive integer required: "+k);return n;}
    private static int integer(Properties p,String k,int min,int max){long n=number(p,k);if(n<min||n>max)throw invalid("Control integer outside bound: "+k);return (int)n;}
    private static Duration millis(Properties p,String k,int max){return Duration.ofMillis(integer(p,k,1,max));}
    private static void boundedDuration(Duration d,String field){if(d==null||d.isNegative()||d.isZero()||d.compareTo(Duration.ofMinutes(30))>0)throw invalid("Finite positive bounded duration required: "+field);}
    private static void exact(Object a,Object b,String field){if(!Objects.equals(a,b))throw invalid("Independent control identity mismatch: "+field);}
    private static void exactSha(String a,String b,String field){if(a==null||!a.equalsIgnoreCase(b))throw invalid("Independent control SHA mismatch: "+field);}
    public static String digest(byte[] b){try{return HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(b));}catch(NoSuchAlgorithmException cause){throw new IllegalStateException("SHA256 unavailable",cause);}}
    private static IllegalArgumentException invalid(String s){return new IllegalArgumentException(s);}
}
