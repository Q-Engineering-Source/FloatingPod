package com.wi.floatingpod.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;
import java.util.List;
import java.util.Optional;
import com.wi.floatingpod.server.DbFirstBootstrapRecordCodec.OwnerData;
import com.wi.floatingpod.server.DbFirstBootstrapRecordCodec.WitnessRef;

/** 当下操作员事故声明及独立退役 DATA；绝不是旧 JVM 的 pre-service receipt 或 B1 阶段。 */
public final class DbFirstStartupRetirementCodec {
    private static final long INCIDENT = 0x46505352494e4331L; // FPSRINC1
    private static final long REQUEST = 0x4650535252455131L; // FPSRREQ1
    private static final long INTENT = 0x46505352494e5431L; // FPSRINT1
    private static final long SETTLEMENT = 0x4650535253455431L; // FPSRSET1
    private static final int MAX = 65_536;
    private DbFirstStartupRetirementCodec() { }

    public static final String REQUEST_FILE = "startup-retirement-incident.bin";
    public static final int MAX_REQUEST_BYTES = 4096;
    /** 仅声明已进入服务后发生 autosave 事故；不声明世界干净、正常停机或不存在 service entry。 */
    public static final String POST_SERVICE_AUTOSAVE_CRASH_UNCHANGED_BUSINESS_DATA =
        "POST_SERVICE_AUTOSAVE_CRASH_UNCHANGED_BUSINESS_DATA";
    /** 已观察到 MainReadPending 逸出 END 的事故；仍须应用验证当前业务 DATA 未改变。 */
    public static final String POST_SERVICE_MAIN_READ_PENDING_CRASH_UNCHANGED_BUSINESS_DATA =
        "POST_SERVICE_MAIN_READ_PENDING_CRASH_UNCHANGED_BUSINESS_DATA";
    /** 原P已提交，且capture容量拒绝发生于ticket/entity之前；仅供该精确事故的受控声明。 */
    public static final String COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE =
        "COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE";
    /** 已实测仅一条activation的停服sealing事故；不承接checkpoint或其他业务增量。 */
    public static final String COMMITTED_ACTIVE_POD_STOP_SEALING_FAILURE =
        "COMMITTED_ACTIVE_POD_STOP_SEALING_FAILURE";
    /** 新建Pod的PREPARATION与唯一初始ACTIVATION已提交；只接受精确P→A→current图。 */
    public static final String COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE =
        "COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE";
    /** Boot Recall Census claim failed before Ready; operator must still verify business DATA is unchanged. */
    public static final String BOOT_CENSUS_CLAIM_FAILURE_UNCHANGED_BUSINESS_DATA =
        "BOOT_CENSUS_CLAIM_FAILURE_UNCHANGED_BUSINESS_DATA";
    /** Boot Pod发布未完成Ready；仍须operator核验业务DATA未改变。 */
    public static final String BOOT_RECOVERY_PUBLICATION_FAILURE_UNCHANGED_BUSINESS_DATA =
        "BOOT_RECOVERY_PUBLICATION_FAILURE_UNCHANGED_BUSINESS_DATA";
    public record ActiveChainIdentity(UUID podUuid, UUID activationOperationUuid, long writerEpoch, String chainSha256) {
        public ActiveChainIdentity {
            nonzero(podUuid); nonzero(activationOperationUuid); hash(chainSha256);
            if (writerEpoch <= 0) throw new IllegalArgumentException("Positive active-chain epoch required");
        }
    }
    /** 受控私有文件中的当下 operator attestation；不含手写 PID/token 或任意 SQL。 */
    public record RequestData(UUID operationUuid, UUID failedBoot, UUID bootstrapOperationUuid, UUID serverUuid,
            UUID bindingUuid, UUID provisioningUuid, String schema, String configurationSha256, String recipeSha256,
            String sourceSha256, String ownerSha256, String completeSha256, String failedArtifactSha256,
            String sourceArchiveSha256, String crashEvidenceSha256, int formatVersion,
            String failureKind, String predecessorReceiptSha256,
            Optional<UUID> failedCreateOperationUuid, Optional<UUID> preparationOperationUuid,
            Optional<ActiveChainIdentity> activeChainIdentity) {
        public RequestData(UUID operationUuid, UUID failedBoot, UUID bootstrapOperationUuid, UUID serverUuid,
                UUID bindingUuid, UUID provisioningUuid, String schema, String configurationSha256, String recipeSha256,
                String sourceSha256, String ownerSha256, String completeSha256, String failedArtifactSha256,
                String sourceArchiveSha256, String crashEvidenceSha256, int formatVersion,
                String failureKind, String predecessorReceiptSha256,
                Optional<UUID> failedCreateOperationUuid, Optional<UUID> preparationOperationUuid) {
            this(operationUuid,failedBoot,bootstrapOperationUuid,serverUuid,bindingUuid,provisioningUuid,schema,
                configurationSha256,recipeSha256,sourceSha256,ownerSha256,completeSha256,failedArtifactSha256,
                sourceArchiveSha256,crashEvidenceSha256,formatVersion,failureKind,predecessorReceiptSha256,
                failedCreateOperationUuid,preparationOperationUuid,Optional.empty());
        }
        public RequestData(UUID operationUuid, UUID failedBoot, UUID bootstrapOperationUuid, UUID serverUuid,
                UUID bindingUuid, UUID provisioningUuid, String schema, String configurationSha256, String recipeSha256,
                String sourceSha256, String ownerSha256, String completeSha256, String failedArtifactSha256,
                String sourceArchiveSha256, String crashEvidenceSha256, int formatVersion,
                String failureKind, String predecessorReceiptSha256) {
            this(operationUuid,failedBoot,bootstrapOperationUuid,serverUuid,bindingUuid,provisioningUuid,schema,
                configurationSha256,recipeSha256,sourceSha256,ownerSha256,completeSha256,failedArtifactSha256,
                sourceArchiveSha256,crashEvidenceSha256,formatVersion,failureKind,predecessorReceiptSha256,
                Optional.empty(),Optional.empty());
        }
        public RequestData(UUID operationUuid, UUID failedBoot, UUID bootstrapOperationUuid, UUID serverUuid,
                UUID bindingUuid, UUID provisioningUuid, String schema, String configurationSha256, String recipeSha256,
                String sourceSha256, String ownerSha256, String completeSha256, String failedArtifactSha256,
                String sourceArchiveSha256, String crashEvidenceSha256) {
            this(operationUuid,failedBoot,bootstrapOperationUuid,serverUuid,bindingUuid,provisioningUuid,schema,
                configurationSha256,recipeSha256,sourceSha256,ownerSha256,completeSha256,failedArtifactSha256,
                sourceArchiveSha256,crashEvidenceSha256,1,"FORGE_EARLY_DUPLICATE_SJDL_BEFORE_RUNTIME",completeSha256);
        }
        public RequestData {
            if (formatVersion < 1 || formatVersion > 5) throw new IllegalArgumentException("Unknown retirement format");
            Objects.requireNonNull(failedCreateOperationUuid); Objects.requireNonNull(preparationOperationUuid); Objects.requireNonNull(activeChainIdentity);
            if (formatVersion >= 3) {
                String expectedKind = switch (formatVersion) {
                    case 3 -> COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE;
                    case 4 -> COMMITTED_ACTIVE_POD_STOP_SEALING_FAILURE;
                    case 5 -> COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE;
                    default -> throw new IllegalArgumentException("Committed startup incident requires v3-v5");
                };
                if (!expectedKind.equals(failureKind) ||
                        failedCreateOperationUuid.isEmpty() || preparationOperationUuid.isEmpty())
                    throw new IllegalArgumentException("Exact committed preparation incident identities required");
                nonzero(failedCreateOperationUuid.orElseThrow()); nonzero(preparationOperationUuid.orElseThrow());
                if (failedCreateOperationUuid.equals(preparationOperationUuid))
                    throw new IllegalArgumentException("Create and preparation execution identities must be distinct");
            } else if (failedCreateOperationUuid.isPresent() || preparationOperationUuid.isPresent() ||
                    COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE.equals(failureKind) ||
                    COMMITTED_ACTIVE_POD_STOP_SEALING_FAILURE.equals(failureKind) ||
                    COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE.equals(failureKind))
                throw new IllegalArgumentException("Committed preparation identities require v3");
            if (formatVersion >= 4) {
                var active = activeChainIdentity.orElseThrow(() -> new IllegalArgumentException("Exact active-chain identity required"));
                if (active.activationOperationUuid().equals(failedCreateOperationUuid.orElseThrow()) ||
                        active.activationOperationUuid().equals(preparationOperationUuid.orElseThrow()))
                    throw new IllegalArgumentException("Activation cannot impersonate create or preparation");
                if (formatVersion == 5 && (active.podUuid().equals(failedCreateOperationUuid.orElseThrow()) ||
                        active.podUuid().equals(preparationOperationUuid.orElseThrow()) ||
                        active.podUuid().equals(active.activationOperationUuid())))
                    throw new IllegalArgumentException("New Pod/create/preparation/activation identities must be distinct");
            } else if (activeChainIdentity.isPresent()) throw new IllegalArgumentException("Active-chain identity requires v4");
            hash(predecessorReceiptSha256);
            if (!failureKind.equals("FORGE_EARLY_DUPLICATE_SJDL_BEFORE_RUNTIME") &&
                    !failureKind.equals("FORGE_EARLY_MISSING_SEC_PERMISSIONS_BEFORE_RUNTIME") &&
                    !failureKind.equals(POST_SERVICE_AUTOSAVE_CRASH_UNCHANGED_BUSINESS_DATA) &&
                    !failureKind.equals(POST_SERVICE_MAIN_READ_PENDING_CRASH_UNCHANGED_BUSINESS_DATA) &&
                    !failureKind.equals(COMMITTED_PREPARATION_CAPTURE_CAPACITY_FAILURE) &&
                    !failureKind.equals(COMMITTED_ACTIVE_POD_STOP_SEALING_FAILURE) &&
                    !failureKind.equals(COMMITTED_NEW_PREPARATION_ACTIVATION_CHECKPOINT_CONTROL_STOP_FAILURE) &&
                    !failureKind.equals(BOOT_CENSUS_CLAIM_FAILURE_UNCHANGED_BUSINESS_DATA) &&
                    !failureKind.equals(BOOT_RECOVERY_PUBLICATION_FAILURE_UNCHANGED_BUSINESS_DATA))
                throw new IllegalArgumentException("Unknown exact operator incident kind");
            if (formatVersion == 1 && (!predecessorReceiptSha256.equals(completeSha256) ||
                    !failureKind.equals("FORGE_EARLY_DUPLICATE_SJDL_BEFORE_RUNTIME")))
                throw new IllegalArgumentException("Legacy incident must keep its original B1 root/kind");
            if (formatVersion >= 2 && predecessorReceiptSha256.equals(completeSha256))
                throw new IllegalArgumentException("Chained incident requires a previous applied retirement");
            for (UUID id : new UUID[]{operationUuid, failedBoot, bootstrapOperationUuid, serverUuid, bindingUuid, provisioningUuid}) nonzero(id);
            if (schema == null || !schema.matches("[A-Za-z_][A-Za-z0-9_]{0,63}"))
                throw new IllegalArgumentException("Exact incident schema required");
            for (String sha : new String[]{configurationSha256, recipeSha256, sourceSha256, ownerSha256,
                    completeSha256, failedArtifactSha256, sourceArchiveSha256, crashEvidenceSha256}) hash(sha);
        }
        @Override public String toString() { return "RequestData[operator attestation; not runtime proof]"; }
    }
    public record IncidentData(RequestData request, String requestSha256, OwnerData failedOwner, OwnerData acceptedBy,
            long writerEpoch, WitnessRef sourceNative, WitnessRef ownerNative, WitnessRef completeNative,
            WitnessRef predecessorNative) {
        public IncidentData(RequestData request, String requestSha256, OwnerData failedOwner, OwnerData acceptedBy,
                long writerEpoch, WitnessRef sourceNative, WitnessRef ownerNative, WitnessRef completeNative) {
            this(request,requestSha256,failedOwner,acceptedBy,writerEpoch,sourceNative,ownerNative,completeNative,completeNative);
        }
        public IncidentData {
            Objects.requireNonNull(request); hash(requestSha256); Objects.requireNonNull(failedOwner); Objects.requireNonNull(acceptedBy);
            Objects.requireNonNull(sourceNative); Objects.requireNonNull(ownerNative); Objects.requireNonNull(completeNative);
            Objects.requireNonNull(predecessorNative);
            if (!predecessorNative.sha256().equals(request.predecessorReceiptSha256()))
                throw new IllegalArgumentException("Incident predecessor differs");
            if (writerEpoch <= 0) throw new IllegalArgumentException("Positive incident writer epoch required");
            if (!request.sourceSha256().equals(failedOwner.sourceReceiptSha256()) ||
                    !request.failedBoot().equals(failedOwner.boot()) || acceptedBy.boot().equals(failedOwner.boot()) ||
                    !request.sourceSha256().equals(acceptedBy.sourceReceiptSha256()) ||
                    !request.sourceSha256().equals(sourceNative.sha256()) ||
                    !request.ownerSha256().equals(ownerNative.sha256()) ||
                    !request.completeSha256().equals(completeNative.sha256()))
                throw new IllegalArgumentException("Incident owner/source differs");
        }
        public String sourceSha256() { return request.sourceSha256(); }
    }
    public static byte[] encodeRequest(RequestData value) throws IOException { return frame(REQUEST, value.formatVersion(), out -> request(out, value)); }
    public static RequestData decodeRequest(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length > MAX_REQUEST_BYTES) throw new IOException("Incident request exceeds bound");
        return decode(bytes, REQUEST, DbFirstStartupRetirementCodec::request);
    }
    public record IntentData(IncidentData incident, String incidentSha256, OwnerData executor,
            String barrier, long connectionId, UUID connectionGeneration, LocalDateTime databaseNow,
            WitnessRef before, WitnessRef target) {
        public IntentData {
            Objects.requireNonNull(incident); hash(incidentSha256); Objects.requireNonNull(executor);
            text(barrier); nonzero(connectionGeneration); time(databaseNow);
            Objects.requireNonNull(before); Objects.requireNonNull(target);
            if (connectionId <= 0 || executor.boot().equals(incident.failedOwner().boot()) ||
                    !executor.sourceReceiptSha256().equals(incident.sourceSha256()))
                throw new IllegalArgumentException("Independent current executor/session required");
        }
    }
    public record SettlementData(IntentData intent, String intentSha256, OwnerData observer,
            String barrier, long connectionId, UUID connectionGeneration, LocalDateTime databaseNow,
            WitnessRef actual, boolean applied) {
        public SettlementData {
            Objects.requireNonNull(intent); hash(intentSha256); Objects.requireNonNull(observer);
            text(barrier); nonzero(connectionGeneration); time(databaseNow); Objects.requireNonNull(actual);
            if (connectionId <= 0 || !observer.sourceReceiptSha256().equals(intent.incident().sourceSha256()) ||
                    !barrier.equals(intent.barrier()))
                throw new IllegalArgumentException("Exact retirement observer/source/barrier required");
        }
    }
    /** 只排序 DATA 前驱关系；native APPLIED/Full27/进程证明仍由原 actor 逐节点核验。 */
    public static Optional<IntentData> successor(List<IntentData> remaining, String predecessorSha256) {
        hash(predecessorSha256);
        var matching=remaining.stream().filter(value -> value.incident().request().predecessorReceiptSha256().equals(predecessorSha256)).toList();
        if(matching.size()>1)throw new IllegalArgumentException("Startup retirement predecessor forks");
        return matching.stream().findFirst();
    }
    public static void requireUniqueIdentities(List<IntentData> history) {
        if(history.stream().map(value -> value.incident().request().operationUuid()).distinct().count()!=history.size() ||
            history.stream().map(value -> value.incident().failedOwner().boot()).distinct().count()!=history.size())
            throw new IllegalArgumentException("Retirement history reuses an operation or failed boot");
    }
    public static byte[] encodeIncident(IncidentData value) throws IOException {
        return frame(INCIDENT, value.request().formatVersion(), out -> incident(out, value));
    }
    public static IncidentData decodeIncident(byte[] bytes) throws IOException {
        return decode(bytes, INCIDENT, DbFirstStartupRetirementCodec::incident);
    }
    public static byte[] encodeIntent(IntentData value) throws IOException {
        return frame(INTENT, value.incident().request().formatVersion(), out -> intent(out, value));
    }
    public static IntentData decodeIntent(byte[] bytes) throws IOException {
        return decode(bytes, INTENT, DbFirstStartupRetirementCodec::intent);
    }
    public static byte[] encodeSettlement(SettlementData value) throws IOException {
        return frame(SETTLEMENT, value.intent().incident().request().formatVersion(), out -> {
            intent(out, value.intent()); digest(out, value.intentSha256()); owner(out, value.observer());
            out.writeUTF(value.barrier()); out.writeLong(value.connectionId()); uuid(out, value.connectionGeneration());
            timestamp(out, value.databaseNow()); witness(out, value.actual()); out.writeBoolean(value.applied());
        });
    }
    public static SettlementData decodeSettlement(byte[] bytes) throws IOException {
        return decode(bytes, SETTLEMENT, (in, version) -> {
            var intent = intent(in, version); String sha = digest(in); var observer = owner(in);
            String barrier = in.readUTF(); long connection = in.readLong(); UUID generation = uuid(in);
            var now = timestamp(in); var actual = witness(in); int applied = in.readUnsignedByte();
            if (applied > 1) throw new IOException("Noncanonical retirement outcome");
            return new SettlementData(intent, sha, observer, barrier, connection, generation, now, actual, applied == 1);
        });
    }
    private static void incident(DataOutputStream out, IncidentData value) throws IOException {
        request(out, value.request()); digest(out, value.requestSha256()); owner(out, value.failedOwner());
        owner(out, value.acceptedBy()); out.writeLong(value.writerEpoch()); witness(out, value.sourceNative());
        witness(out, value.ownerNative()); witness(out, value.completeNative());
        if (value.request().formatVersion() >= 2) witness(out, value.predecessorNative());
    }
    private static IncidentData incident(DataInputStream in, int version) throws IOException {
        var request = request(in, version); String sha = digest(in); var failed = owner(in); var accepted = owner(in);
        long epoch = in.readLong(); var source = witness(in); var owner = witness(in); var complete = witness(in);
        return new IncidentData(request, sha, failed, accepted, epoch, source, owner, complete,
            version >= 2 ? witness(in) : complete);
    }
    private static void request(DataOutputStream out, RequestData value) throws IOException {
        uuid(out, value.operationUuid()); uuid(out, value.failedBoot()); uuid(out, value.bootstrapOperationUuid());
        uuid(out, value.serverUuid()); uuid(out, value.bindingUuid()); uuid(out, value.provisioningUuid());
        out.writeUTF(value.schema()); digest(out, value.configurationSha256()); digest(out, value.recipeSha256());
        digest(out, value.sourceSha256()); digest(out, value.ownerSha256()); digest(out, value.completeSha256());
        digest(out, value.failedArtifactSha256()); digest(out, value.sourceArchiveSha256()); digest(out, value.crashEvidenceSha256());
        out.writeUTF(value.failureKind());
        if (value.formatVersion() >= 2) digest(out, value.predecessorReceiptSha256());
        if (value.formatVersion() >= 3) {
            uuid(out,value.failedCreateOperationUuid().orElseThrow());
            uuid(out,value.preparationOperationUuid().orElseThrow());
        }
        if (value.formatVersion() >= 4) {
            var active = value.activeChainIdentity().orElseThrow();
            uuid(out,active.podUuid()); uuid(out,active.activationOperationUuid());
            out.writeLong(active.writerEpoch()); digest(out,active.chainSha256());
        }
    }
    private static RequestData request(DataInputStream in, int version) throws IOException {
        var operation = uuid(in); var failed = uuid(in); var bootstrap = uuid(in); var server = uuid(in);
        var binding = uuid(in); var provisioning = uuid(in); String schema = in.readUTF();
        String configuration = digest(in), recipe = digest(in), source = digest(in), owner = digest(in), complete = digest(in);
        String artifact = digest(in), archive = digest(in), evidence = digest(in), kind = in.readUTF();
        String predecessor = version >= 2 ? digest(in) : complete;
        var create = version >= 3 ? Optional.of(uuid(in)) : Optional.<UUID>empty();
        var preparation = version >= 3 ? Optional.of(uuid(in)) : Optional.<UUID>empty();
        var active = version >= 4 ? Optional.of(new ActiveChainIdentity(uuid(in),uuid(in),in.readLong(),digest(in))) : Optional.<ActiveChainIdentity>empty();
        return new RequestData(operation, failed, bootstrap, server, binding, provisioning, schema, configuration,
            recipe, source, owner, complete, artifact, archive, evidence, version, kind, predecessor,create,preparation,active);
    }
    private static void intent(DataOutputStream out, IntentData value) throws IOException {
        incident(out, value.incident()); digest(out, value.incidentSha256()); owner(out, value.executor());
        out.writeUTF(value.barrier()); out.writeLong(value.connectionId()); uuid(out, value.connectionGeneration());
        timestamp(out, value.databaseNow()); witness(out, value.before()); witness(out, value.target());
    }
    private static IntentData intent(DataInputStream in, int version) throws IOException {
        return new IntentData(incident(in, version), digest(in), owner(in), in.readUTF(), in.readLong(), uuid(in),
            timestamp(in), witness(in), witness(in));
    }
    private static void owner(DataOutputStream out, OwnerData value) throws IOException {
        uuid(out, value.boot()); out.writeLong(value.generation()); out.writeLong(value.pid());
        out.writeLong(value.creationTime()); digest(out, value.sourceReceiptSha256());
    }
    private static OwnerData owner(DataInputStream in) throws IOException {
        return new OwnerData(uuid(in), in.readLong(), in.readLong(), in.readLong(), digest(in));
    }
    private static void witness(DataOutputStream out, WitnessRef value) throws IOException {
        out.writeUTF(value.fileId()); digest(out, value.sha256()); out.writeLong(value.byteLength());
    }
    private static WitnessRef witness(DataInputStream in) throws IOException {
        return new WitnessRef(in.readUTF(), digest(in), in.readLong());
    }
    private static void timestamp(DataOutputStream out, LocalDateTime value) throws IOException {
        out.writeLong(value.toLocalDate().toEpochDay()); out.writeLong(value.toLocalTime().toNanoOfDay());
    }
    private static LocalDateTime timestamp(DataInputStream in) throws IOException {
        return LocalDateTime.of(java.time.LocalDate.ofEpochDay(in.readLong()), java.time.LocalTime.ofNanoOfDay(in.readLong()));
    }
    private static void uuid(DataOutputStream out, UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits()); out.writeLong(value.getLeastSignificantBits());
    }
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }
    private static void digest(DataOutputStream out, String value) throws IOException { out.write(HexFormat.of().parseHex(value)); }
    private static String digest(DataInputStream in) throws IOException {
        byte[] bytes = in.readNBytes(32);
        if (bytes.length != 32) throw new IOException("Truncated retirement SHA");
        return HexFormat.of().formatHex(bytes);
    }
    @FunctionalInterface private interface Encoder { void write(DataOutputStream out) throws IOException; }
    @FunctionalInterface private interface Decoder<T> { T read(DataInputStream in, int version) throws IOException; }
    private static byte[] frame(long magic, int version, Encoder encoder) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeLong(magic); out.writeInt(version); encoder.write(out);
        }
        byte[] body = bytes.toByteArray();
        if (body.length + 32 > MAX) throw new IOException("Retirement DATA exceeds bound");
        bytes.write(sha(body)); return bytes.toByteArray();
    }
    private static <T> T decode(byte[] bytes, long magic, Decoder<T> decoder) throws IOException {
        if (bytes == null || bytes.length < 44 || bytes.length > MAX) throw new IOException("Invalid retirement DATA size");
        byte[] body = Arrays.copyOf(bytes, bytes.length - 32);
        if (!MessageDigest.isEqual(sha(body), Arrays.copyOfRange(bytes, body.length, bytes.length)))
            throw new IOException("Retirement DATA SHA mismatch");
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            if (in.readLong() != magic) throw new IOException("Unknown retirement DATA domain");
            int version = in.readInt();
            if (version < 1 || version > 5) throw new IOException("Unknown retirement DATA version");
            T result = decoder.read(in, version);
            if (in.read() != -1) throw new IOException("Trailing retirement DATA");
            return result;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid retirement DATA", invalid); }
    }
    private static byte[] sha(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    private static void hash(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Canonical SHA required");
    }
    private static void nonzero(UUID value) {
        if (value == null || value.equals(new UUID(0, 0))) throw new IllegalArgumentException("Nonzero UUID required");
    }
    private static void text(String value) {
        if (value == null || value.isEmpty() || value.length() > 256) throw new IllegalArgumentException("Bounded barrier required");
    }
    private static void time(LocalDateTime value) {
        if (value == null || value.getNano() % 1000 != 0) throw new IllegalArgumentException("Exact DB DATETIME(6) required");
    }
}
