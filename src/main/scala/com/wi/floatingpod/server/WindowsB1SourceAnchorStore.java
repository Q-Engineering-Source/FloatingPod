package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/** 固定、不可覆盖的来源 anchor；只有完整 native ACK 才签发 DurableB1SourceAnchor。 */
public final class WindowsB1SourceAnchorStore {
    private static final String SUFFIX = ".b1-source-v4", TMP_SUFFIX = ".b1-source-v4.tmp";
    private final WindowsMarkerIo io;
    private final Thread owner = Thread.currentThread();
    public WindowsB1SourceAnchorStore(WindowsMarkerIo actualOwnedIo) { io = Objects.requireNonNull(actualOwnedIo); }

    public static final class AnchorConfiguration {
        private enum Encoding { LEGACY, FORMAT3_V2, DEPLOYMENT_RECEIPT_V1 }
        private final byte[] canonical;
        private final Encoding encoding;
        private AnchorConfiguration(MySqlRuntimeMetadata metadata) throws IOException {
            canonical = B1SourceAnchorCodec.configurationBytes(Objects.requireNonNull(metadata));
            encoding = Encoding.LEGACY;
            if (metadata.baseline().schemaFormat() != 2 && metadata.baseline().schemaFormat() != 3)
                throw new IOException("Source anchor requires format 2 or 3 metadata");
        }
        private AnchorConfiguration(MySqlRuntimeMetadata.FullV2 metadata) {
            canonical = B1SourceAnchorCodec.configurationBytesV2(Objects.requireNonNull(metadata));
            encoding = Encoding.FORMAT3_V2;
        }
        private AnchorConfiguration(MySqlRuntimeMetadata.DeploymentReceiptStatic metadata) throws IOException {
            canonical=B1DeploymentReceiptSourceAnchorCodec.configurationBytes(Objects.requireNonNull(metadata));
            encoding=Encoding.DEPLOYMENT_RECEIPT_V1;
        }
        public static AnchorConfiguration ofDeploymentReceiptMetadata(MySqlRuntimeMetadata.DeploymentReceiptStatic metadata)throws IOException {
            return new AnchorConfiguration(metadata);
        }
        public static AnchorConfiguration ofMetadata(MySqlRuntimeMetadata metadata) throws IOException {
            return new AnchorConfiguration(metadata);
        }
        /** 显式选择完整 V2 配置；不签发文件批准、live custody 或运行时资格。 */
        public static AnchorConfiguration ofMetadataV2(MySqlRuntimeMetadata.FullV2 metadata) {
            return new AnchorConfiguration(metadata);
        }
        // 实际 native handle 读回与纯单元共用同一严格版本路径；禁止探测失败后退回 legacy。
        void requireMetadata(byte[] bytes, String failure) throws IOException {
            byte[] actual = switch(encoding) {
                case DEPLOYMENT_RECEIPT_V1 -> B1DeploymentReceiptSourceAnchorCodec.configurationBytes(MySqlRuntimeMetadata.fromDeploymentReceiptBytes(bytes));
                case FORMAT3_V2 -> B1SourceAnchorCodec.configurationBytesV2(MySqlRuntimeMetadata.fromBytesV2(bytes));
                case LEGACY -> B1SourceAnchorCodec.configurationBytes(MySqlRuntimeMetadata.fromBytes(bytes));
            };
            try { B1SourceAnchorCodec.requireExact(canonical, actual, failure); }
            finally { Arrays.fill(actual, (byte) 0); }
        }
        byte[] encode(B1SourceAnchorCodec.Data data) throws IOException {
            requireConfiguration(data.configuration());
            return switch(encoding) {
                case DEPLOYMENT_RECEIPT_V1 -> B1DeploymentReceiptSourceAnchorCodec.encode(data);
                case FORMAT3_V2 -> B1SourceAnchorCodec.encodeV2(data);
                case LEGACY -> B1SourceAnchorCodec.encode(data);
            };
        }
        B1SourceAnchorCodec.Data decode(byte[] bytes) throws IOException {
            var data = switch(encoding) {
                case DEPLOYMENT_RECEIPT_V1 -> B1DeploymentReceiptSourceAnchorCodec.decode(bytes);
                case FORMAT3_V2 -> B1SourceAnchorCodec.decodeV2(bytes);
                case LEGACY -> B1SourceAnchorCodec.decode(bytes);
            };
            requireConfiguration(data.configuration());
            return data;
        }
        String namespace(byte[] configuration) throws IOException {
            requireConfiguration(configuration);
            return switch(encoding) {
                case DEPLOYMENT_RECEIPT_V1 -> B1DeploymentReceiptSourceAnchorCodec.canonicalConfigurationNamespace(configuration);
                case FORMAT3_V2 -> B1SourceAnchorCodec.canonicalConfigurationNamespaceV2(configuration);
                case LEGACY -> B1SourceAnchorCodec.canonicalConfigurationNamespace(configuration);
            };
        }
        private void requireConfiguration(byte[] configuration) throws IOException {
            B1SourceAnchorCodec.requireExact(canonical, configuration, "Anchor configuration differs from expected complete configuration");
        }
        public String configurationSha256() { return WindowsWorldProvisioner.sha256(canonical); }
        @Override public String toString() { return "AnchorConfiguration[redacted; no secret payload]"; }
    }
    public static final class ControlledAnchorLocation {
        private final WindowsB1SourceAnchorStore store;
        private final Path metadata, canonical, temporary;
        private final WindowsMarkerIo.FileIdentity parentIdentity, metadataIdentity;
        private final String parentNativePath, metadataSha;
        private final AnchorConfiguration configuration;
        private final UUID operation;
        private ControlledAnchorLocation(WindowsB1SourceAnchorStore store, Path metadata,
                WindowsMarkerIo.FileIdentity parentIdentity, WindowsMarkerIo.FileIdentity metadataIdentity,
                String parentNativePath, String metadataSha, AnchorConfiguration configuration, UUID operation) {
            this.store = store; this.metadata = metadata; this.parentIdentity = parentIdentity;
            this.metadataIdentity = metadataIdentity; this.parentNativePath = parentNativePath;
            this.metadataSha = metadataSha; this.configuration = configuration; this.operation = operation;
            canonical = metadata.resolveSibling(metadata.getFileName() + (configuration.encoding==AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1 ? ".b1-source-v6" : SUFFIX));
            temporary = metadata.resolveSibling(metadata.getFileName() + (configuration.encoding==AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1 ? ".b1-source-v6.tmp" : TMP_SUFFIX));
        }
        public Path canonicalPath() { return canonical; }
        public Path temporaryPath() { return temporary; }
        public String configurationSha256() { return configuration.configurationSha256(); }
        public String parentIdentity() { return parentIdentity.versionedIdentity(); }
        @Override public String toString() { return "ControlledAnchorLocation[explicit native location; not ACL authority]"; }
    }
    public static final class VerifiedAnchorInput {
        private final ControlledAnchorLocation location;
        private final B1SourceAnchorCodec.Data data;
        private final byte[] canonical;
        private final WindowsMarkerIo.FileIdentity fileIdentity;
        private VerifiedAnchorInput(ControlledAnchorLocation location, B1SourceAnchorCodec.Data data,
                byte[] canonical, WindowsMarkerIo.FileIdentity fileIdentity) {
            this.location = location; this.data = data; this.canonical = canonical.clone(); this.fileIdentity = fileIdentity;
        }
        public WindowsWorldProvisioner.Plan expectedPlan() throws IOException {
            if(location.configuration.encoding==AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1)throw new IOException("Schema5 anchor has no legacy native Plan conversion");
            return WindowsWorldProvisioner.decodeReceipt(data.receipt()).plan();
        }
        public WindowsWorldProvisionReceiptDataCodec.PlanData expectedDeploymentReceiptPlan()throws IOException {
            if(location.configuration.encoding!=AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1)throw new IOException("Explicit schema5 anchor family required");
            return WindowsWorldProvisionReceiptDataCodec.decode(data.receipt()).plan();
        }
        public String expectedReceiptSha256() { return WindowsWorldProvisioner.sha256(data.receipt()); }
        public byte[] originalReceiptCanonicalBytes() { return data.receipt(); }
        public String anchorSha256() { return WindowsWorldProvisioner.sha256(canonical); }
        public String worldNativePath() { return data.worldNativePath(); }
        public String barrierNamespace() throws IOException {
            return location.configuration.namespace(data.configuration());
        }
        public String originalSourceIdentity() throws IOException {
            if(location.configuration.encoding==AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1)
                return WindowsWorldProvisionReceiptDataCodec.decode(data.receipt()).sourceIdentity();
            return WindowsWorldProvisioner.decodeReceipt(data.receipt()).sourceIdentity();
        }
        @Override public String toString() { return "VerifiedAnchorInput[disk facts only; no live custody]"; }
    }
    public static final class DurableB1SourceAnchor {
        private final VerifiedAnchorInput input;
        private DurableB1SourceAnchor(VerifiedAnchorInput input) { this.input = input; }
        public VerifiedAnchorInput verifiedInput() { return input; }
        public String anchorSha256() { return input.anchorSha256(); }
        public String fileIdentity() { return input.fileIdentity.versionedIdentity(); }
        public String durabilityProtocol() { return input.location.configuration.encoding==AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1
            ? "WIN32_FULL_WRITE_FLUSH_NO_REPLACE_WRITE_THROUGH_CANONICAL_READ_FLUSH_READ_CLOSE_V6"
            : "WIN32_FULL_WRITE_FLUSH_NO_REPLACE_WRITE_THROUGH_CANONICAL_READ_FLUSH_READ_CLOSE_V4"; }
        @Override public String toString() { return "DurableB1SourceAnchor[anchor durability only; not Ready]"; }
    }
    public static final class ProvisioningIncomplete extends IOException {
        private ProvisioningIncomplete(Throwable cause) { super("Controlled source anchor absent; provisioning incomplete; no Fresh or SQL authority", cause); }
    }
    private void checkOwner() {
        if (Thread.currentThread() != owner) throw new IllegalStateException("Exact anchor owned IO worker required");
    }
    public ControlledAnchorLocation captureLocation(Path explicitRuntimeMetadata,
            AnchorConfiguration expectedConfiguration, UUID operationId) throws IOException {
        checkOwner(); Objects.requireNonNull(expectedConfiguration); Objects.requireNonNull(operationId);
        Path path = requirePath(explicitRuntimeMetadata);
        refuseConflictingAnchors(path,expectedConfiguration,operationId);
        WindowsMarkerIo.Handle parent = null, metadata = null;
        try {
            parent = io.directory(path.getParent(), operationId);
            var parentId = io.identity(parent); String parentPath = io.finalPath(parent);
            metadata = openMetadata(path, operationId);
            requireExactNativeChild(parentPath, io.finalPath(metadata), path.getFileName().toString());
            var metadataId = io.identity(metadata);
            byte[] bytes = readMetadata(metadata);
            String rawSha;
            try {
                expectedConfiguration.requireMetadata(bytes, "Expected configuration differs from actual native metadata bytes");
                rawSha = WindowsWorldProvisioner.sha256(bytes);
                requireMetadataReadback(metadata, bytes);
            } finally { Arrays.fill(bytes, (byte) 0); }
            requireIdentity(metadataId, io.identity(metadata)); requireIdentity(parentId, io.identity(parent));
            if (!parentPath.equals(io.finalPath(parent))) throw new IOException("Configuration parent moved during capture");
            io.closeChecked(metadata); metadata = null; io.closeChecked(parent); parent = null;
            return new ControlledAnchorLocation(this, path, parentId, metadataId, parentPath, rawSha, expectedConfiguration, operationId);
        } catch (Throwable failure) {
            WindowsMarkerIo.closeSuppressed(metadata, failure); WindowsMarkerIo.closeSuppressed(parent, failure);
            throw propagate(failure);
        }
    }
    private WindowsMarkerIo.Handle openMetadata(Path path, UUID operation) throws IOException {
        // SHARE_READ 阻止 metadata 写入/rename/delete；actual bytes 由这个 handle 解码。
        return io.open(path, WindowsMarkerIo.READ, WindowsMarkerIo.SHARE_READ, WindowsMarkerIo.OPEN_EXISTING,
            WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation);
    }
    private byte[] readMetadata(WindowsMarkerIo.Handle handle) throws IOException {
        int length = Math.toIntExact(io.payloadLength(handle));
        WindowsMarkerIo.payloadBound(length);
        return io.readPayloadExact(handle, length);
    }
    private record ValidatedLocation(WindowsMarkerIo.Handle parent, WindowsMarkerIo.Handle metadata) {}
    private ValidatedLocation validateLocation(ControlledAnchorLocation location) throws IOException {
        checkOwner();
        if (location == null || location.store != this) throw new IOException("Anchor location belongs to another issuer");
        refuseConflictingAnchors(location.metadata,location.configuration,location.operation);
        refuseTemporary(location);
        WindowsMarkerIo.Handle parent = null, metadata = null;
        try {
            parent = io.directory(location.metadata.getParent(), location.operation);
            requireIdentity(location.parentIdentity, io.identity(parent));
            if (!location.parentNativePath.equals(io.finalPath(parent))) throw new IOException("Controlled configuration parent path changed");
            metadata = openMetadata(location.metadata, location.operation);
            requireIdentity(location.metadataIdentity, io.identity(metadata));
            requireExactNativeChild(location.parentNativePath, io.finalPath(metadata), location.metadata.getFileName().toString());
            byte[] bytes = readMetadata(metadata);
            try {
                if (!location.metadataSha.equals(WindowsWorldProvisioner.sha256(bytes))) throw new IOException("Controlled metadata bytes changed");
                location.configuration.requireMetadata(bytes, "Controlled metadata identity changed");
                requireMetadataReadback(metadata, bytes);
            } finally { Arrays.fill(bytes, (byte) 0); }
            requireIdentity(location.metadataIdentity, io.identity(metadata));
            return new ValidatedLocation(parent, metadata);
        } catch (Throwable failure) {
            WindowsMarkerIo.closeSuppressed(metadata, failure); WindowsMarkerIo.closeSuppressed(parent, failure);
            throw propagate(failure);
        }
    }
    public DurableB1SourceAnchor publishCreatedSource(ControlledAnchorLocation location,
            WindowsWorldProvisioner.ProvisionedWorld actualCreatedWorld) throws IOException {
        checkOwner(); Objects.requireNonNull(actualCreatedWorld);
        if(location!=null && location.configuration.encoding==AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1)
            throw new IOException("Schema5 source requires actual dedicated native5 world");
        if (!actualCreatedWorld.createdThisBoot()) throw new IOException("First anchor requires actual createFresh result");
        var proof = actualCreatedWorld.freshProvisioningProof(); var custody = actualCreatedWorld.bootWorldCustodyProof();
        if (custody.freshProvisioningProof() != proof || !proof.creationBoot().bootId().equals(custody.bootId()) ||
            proof.creationBoot().generation() != custody.generation()) throw new IOException("First anchor requires exact current creation custody");
        custody.validateLive();
        refuseLegacySlots(io, custody.directory(), proof.plan().bootstrapOperationUuid());
        WindowsMarkerIo.Handle source = null;
        String worldPath;
        try {
            source = io.directory(custody.directory(), proof.plan().creationOperationUuid());
            if (!proof.sourceWorldIdentity().equals(io.identity(source).versionedIdentity())) throw new IOException("Source identity differs from creation proof");
            worldPath = io.finalPath(source); io.closeChecked(source); source = null;
        } catch (Throwable failure) { WindowsMarkerIo.closeSuppressed(source, failure); throw propagate(failure); }
        if (location == null || location.store != this) throw new IOException("Exact controlled location required");
        requireNativeOutside(location.parentNativePath, worldPath);
        var receipt = WindowsWorldProvisioner.decodeReceipt(proof.receiptCanonicalBytes());
        if (receipt.creatorPid() != proof.creationProcessPid() || receipt.creatorCreationTime() != proof.creationProcessFileTime())
            throw new IOException("Original creator identity differs from opaque receipt");
        var data = new B1SourceAnchorCodec.Data(location.configuration.canonical, proof.receiptCanonicalBytes(),
            proof.receiptFileIdentity(), location.parentIdentity.versionedIdentity(), location.metadataIdentity.versionedIdentity(),
            location.parentNativePath, worldPath, location.metadata.getFileName().toString());
        byte[] bytes = location.configuration.encode(data);
        custody.validateLive();
        DurableB1SourceAnchor ack = publishOrRebarrier(location, data, bytes, null);
        // Native durability ACK is already complete; live custody is separately rechecked before publication to caller.
        custody.validateLive();
        return ack;
    }
    public DurableB1SourceAnchor publishCreatedDeploymentReceiptSource(ControlledAnchorLocation location,
            WindowsWorldProvisioner.DeploymentReceiptWorld actualCreatedWorld)throws IOException {
        checkOwner();Objects.requireNonNull(actualCreatedWorld);
        if(location==null || location.store!=this || location.configuration.encoding!=AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1)
            throw new IOException("Exact controlled schema5 source location required");
        var proof=actualCreatedWorld.creationProof();var custody=actualCreatedWorld.custodyProof();
        if(custody.creationProof()!=proof || !custody.bootId().equals(proof.creationBoot().bootId()) ||
            custody.generation()!=proof.creationBoot().generation())throw new IOException("Exact current native5 creation custody required");
        custody.validateLive();
        WindowsMarkerIo.Handle source=null;String worldPath;
        try {
            source=io.directory(custody.directory(),proof.plan().creationOperationUuid());
            if(!proof.sourceWorldIdentity().equals(io.identity(source).versionedIdentity()))throw new IOException("Native5 source differs from original creation FileID");
            worldPath=io.finalPath(source);refuseDeploymentReceiptConflictingSlots(worldPath,proof.plan().bootstrapOperationUuid());
            io.closeChecked(source);source=null;
        }catch(Throwable failure){WindowsMarkerIo.closeSuppressed(source,failure);throw propagate(failure);}
        requireNativeOutside(location.parentNativePath,worldPath);
        var receipt=WindowsWorldProvisionReceiptDataCodec.decode(proof.receiptCanonicalBytes());
        if(receipt.creatorPid()!=proof.creationProcessPid() || receipt.creatorCreationTime()!=proof.creationProcessFileTime())
            throw new IOException("Native5 receipt creator differs from actual creation proof");
        var data=new B1SourceAnchorCodec.Data(location.configuration.canonical,proof.receiptCanonicalBytes(),proof.receiptFileIdentity(),
            location.parentIdentity.versionedIdentity(),location.metadataIdentity.versionedIdentity(),location.parentNativePath,worldPath,location.metadata.getFileName().toString());
        byte[] bytes=location.configuration.encode(data);custody.validateLive();
        var ack=publishOrRebarrier(location,data,bytes,null);custody.validateLive();return ack;
    }
    public VerifiedAnchorInput readExpectedSource(ControlledAnchorLocation location) throws IOException {
        WindowsMarkerIo.Handle parent = null, metadata = null, canonical = null;
        try {
            var heldLocation = validateLocation(location);
            parent = heldLocation.parent(); metadata = heldLocation.metadata();
            canonical = openCanonical(location);
            if (canonical == null) throw new ProvisioningIncomplete(null);
            var identity = io.identity(canonical);
            requireExactNativeChild(location.parentNativePath, io.finalPath(canonical), location.canonical.getFileName().toString());
            byte[] bytes = io.readPayloadExact(canonical, Math.toIntExact(io.payloadLength(canonical)));
            var data = location.configuration.decode(bytes); requireDataLocation(location, data);
            B1SourceAnchorCodec.requireExact(bytes, io.readPayloadExact(canonical, bytes.length), "Anchor read changed");
            requireIdentity(identity, io.identity(canonical));
            verifyMetadata(location, metadata); verifyParent(location, parent);
            io.closeChecked(canonical); canonical = null;
            io.closeChecked(metadata); metadata = null; io.closeChecked(parent); parent = null;
            return new VerifiedAnchorInput(location, data, bytes, identity);
        } catch (Throwable failure) {
            WindowsMarkerIo.closeSuppressed(canonical, failure); WindowsMarkerIo.closeSuppressed(metadata, failure);
            WindowsMarkerIo.closeSuppressed(parent, failure); throw propagate(failure);
        }
    }
    public DurableB1SourceAnchor rebarrierExisting(ControlledAnchorLocation location, VerifiedAnchorInput exactInput) throws IOException {
        checkOwner();
        if (exactInput == null || exactInput.location != location) throw new IOException("Exact verified anchor input required");
        return publishOrRebarrier(location, exactInput.data, exactInput.canonical, exactInput.fileIdentity);
    }
    private DurableB1SourceAnchor publishOrRebarrier(ControlledAnchorLocation location,
            B1SourceAnchorCodec.Data data, byte[] bytes, WindowsMarkerIo.FileIdentity expectedFile) throws IOException {
        WindowsMarkerIo.Handle parent = null, metadata = null, temporary = null, canonical = null;
        try {
            var heldLocation = validateLocation(location);
            parent = heldLocation.parent(); metadata = heldLocation.metadata(); requireDataLocation(location, data); refuseTemporary(location);
            canonical = openCanonical(location);
            WindowsMarkerIo.FileIdentity created = expectedFile;
            if (canonical == null) {
                if (expectedFile != null) throw new ProvisioningIncomplete(null);
                temporary = io.createMarkerHandle(location.temporary, location.operation);
                io.metadata(temporary, false, 0);
                requireExactNativeChild(location.parentNativePath, io.finalPath(temporary), location.temporary.getFileName().toString());
                io.writePayload(temporary, bytes); io.metadata(temporary, false, bytes.length);
                created = io.identity(temporary); io.flushChecked(temporary);
                io.moveWriteThroughNoReplace(location.temporary, location.canonical, location.operation);
                io.closeChecked(temporary); temporary = null;
                canonical = openCanonical(location);
                if (canonical == null) throw new IOException("Published anchor missing; no ACK");
            }
            var fileId = validateCanonical(location, canonical, bytes, created);
            verifyMetadata(location, metadata); verifyParent(location, parent);
            io.closeChecked(canonical); canonical = null;
            io.closeChecked(metadata); metadata = null; io.closeChecked(parent); parent = null;
            return new DurableB1SourceAnchor(new VerifiedAnchorInput(location, data, bytes, fileId));
        } catch (Throwable failure) {
            // Any temp is retained. Unknown residual or close failure never authorizes deletion or a durable ACK.
            WindowsMarkerIo.closeSuppressed(canonical, failure); WindowsMarkerIo.closeSuppressed(temporary, failure);
            WindowsMarkerIo.closeSuppressed(metadata, failure); WindowsMarkerIo.closeSuppressed(parent, failure); throw propagate(failure);
        }
    }
    private WindowsMarkerIo.FileIdentity validateCanonical(ControlledAnchorLocation location, WindowsMarkerIo.Handle handle,
            byte[] bytes, WindowsMarkerIo.FileIdentity expected) throws IOException {
        io.metadata(handle, false, bytes.length); var identity = io.identity(handle);
        if (expected != null) requireIdentity(expected, identity);
        requireExactNativeChild(location.parentNativePath, io.finalPath(handle), location.canonical.getFileName().toString());
        B1SourceAnchorCodec.requireExact(bytes, io.readPayloadExact(handle, bytes.length), "Existing anchor conflicts");
        io.flushChecked(handle);
        B1SourceAnchorCodec.requireExact(bytes, io.readPayloadExact(handle, bytes.length), "Anchor native readback changed");
        io.metadata(handle, false, bytes.length); requireIdentity(identity, io.identity(handle));
        return identity;
    }
    private void requireDataLocation(ControlledAnchorLocation l, B1SourceAnchorCodec.Data d) throws IOException {
        B1SourceAnchorCodec.requireExact(l.configuration.canonical, d.configuration(), "Anchor configuration differs from controlled metadata");
        if (!l.parentIdentity.versionedIdentity().equals(d.configParentIdentity()) ||
            !l.metadataIdentity.versionedIdentity().equals(d.metadataFileIdentity()) ||
            !l.parentNativePath.equals(d.configParentNativePath()) || !l.metadata.getFileName().toString().equals(d.metadataFilename()))
            throw new IOException("Anchor controlled location identity conflict");
        requireNativeOutside(l.parentNativePath, d.worldNativePath());
        if(l.configuration.encoding==AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1)
            refuseDeploymentReceiptConflictingSlots(d.worldNativePath(),l.operation);
        else refuseLegacySlots(io, d.worldNativePath(), l.operation);
    }
    private void requireMetadataReadback(WindowsMarkerIo.Handle metadata, byte[] expected) throws IOException {
        byte[] actual = readMetadata(metadata);
        try { B1SourceAnchorCodec.requireExact(expected, actual, "Metadata same-handle readback changed"); }
        finally { Arrays.fill(actual, (byte) 0); }
    }
    private void verifyMetadata(ControlledAnchorLocation location, WindowsMarkerIo.Handle metadata) throws IOException {
        requireIdentity(location.metadataIdentity, io.identity(metadata));
        requireExactNativeChild(location.parentNativePath, io.finalPath(metadata), location.metadata.getFileName().toString());
        byte[] bytes = readMetadata(metadata);
        try {
            if (!location.metadataSha.equals(WindowsWorldProvisioner.sha256(bytes))) throw new IOException("Controlled metadata bytes changed before ACK");
            location.configuration.requireMetadata(bytes, "Metadata identity changed before ACK");
            requireMetadataReadback(metadata, bytes);
        } finally { Arrays.fill(bytes, (byte) 0); }
        requireIdentity(location.metadataIdentity, io.identity(metadata));
    }
    private void verifyParent(ControlledAnchorLocation l, WindowsMarkerIo.Handle parent) throws IOException {
        io.metadata(parent, true); requireIdentity(l.parentIdentity, io.identity(parent));
        if (!l.parentNativePath.equals(io.finalPath(parent))) throw new IOException("Controlled parent moved; no ACK");
    }
    private WindowsMarkerIo.Handle openCanonical(ControlledAnchorLocation l) throws IOException {
        try { return io.open(l.canonical, WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
            WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, l.operation); }
        catch (WindowsMarkerIo.NativeFailure failure) { if (failure.win32Error() == 2) return null; throw failure; }
    }
    private void refuseTemporary(ControlledAnchorLocation l) throws IOException {
        WindowsMarkerIo.Handle temp = null;
        try {
            try { temp = io.open(l.temporary, WindowsMarkerIo.READ, 0, WindowsMarkerIo.OPEN_EXISTING,
                WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, l.operation); }
            catch (WindowsMarkerIo.NativeFailure failure) { if (failure.win32Error() == 2) return; throw failure; }
            throw new IOException("Fixed anchor temporary exists; unresolved conflict; no cleanup");
        } catch (Throwable failure) { WindowsMarkerIo.closeSuppressed(temp, failure); throw propagate(failure); }
    }
    private void refuseLegacyAnchors(Path metadata, UUID operation)throws IOException {
        for(int version=1;version<=3;version++)for(String tail:new String[]{"",".tmp"})
            refuseExisting(io,metadata.resolveSibling(metadata.getFileName()+".b1-source-v"+version+tail),operation);
    }
    private void refuseConflictingAnchors(Path metadata,AnchorConfiguration configuration,UUID operation)throws IOException {
        if(configuration.encoding!=AnchorConfiguration.Encoding.DEPLOYMENT_RECEIPT_V1){refuseLegacyAnchors(metadata,operation);return;}
        for(int version=1;version<=5;version++)for(String tail:new String[]{"",".tmp"})
            refuseExisting(io,metadata.resolveSibling(metadata.getFileName()+".b1-source-v"+version+tail),operation);
    }
    private void refuseDeploymentReceiptConflictingSlots(String world,UUID operation)throws IOException {
        requireNativeWorldPath(world);
        for(int version=1;version<=5;version++)for(String tail:new String[]{".0",".1",".tmp"}) {
            String filename=".floatingpod-b1-resume-v"+version+tail;WindowsMarkerIo.Handle handle=null;
            try {
                try{handle=io.openExistingNativeSchema5ConflictSlot(world,filename,operation);}
                catch(WindowsMarkerIo.NativeFailure failure){if(failure.win32Error()==2)continue;throw failure;}
                requireExactNativeChild(world,io.finalPath(handle),filename);
                throw new IOException("Schema5 source conflicts with existing old B1 slot; preserved: "+filename);
            }catch(Throwable primary){WindowsMarkerIo.closeSuppressed(handle,primary);throw propagate(primary);}
        }
    }
    /** Native fixed-name presence probe; historical source files are retained, never migrated/deleted. */
    static void refuseLegacySlots(WindowsMarkerIo io,Path world,UUID operation)throws IOException {
        for(int version=1;version<=3;version++)for(String tail:new String[]{".0",".1",".tmp"})
            refuseExisting(io,world.resolve(".floatingpod-b1-resume-v"+version+tail),operation);
    }
    /** actual GetFinalPathNameByHandleW volume GUID string，无Java Path转换/盘符映射。 */
    static void refuseLegacySlots(WindowsMarkerIo io,String exactNativeWorld,UUID operation)throws IOException {
        requireNativeWorldPath(exactNativeWorld);
        for(int version=1;version<=3;version++)for(String tail:new String[]{".0",".1",".tmp"}) {
            String filename=".floatingpod-b1-resume-v"+version+tail;
            WindowsMarkerIo.Handle handle=null;
            try {
                try {handle=io.openExistingNativeLegacySlot(exactNativeWorld,filename,operation);}
                catch(WindowsMarkerIo.NativeFailure failure){if(failure.win32Error()==2)continue;throw failure;}
                requireExactNativeChild(exactNativeWorld,io.finalPath(handle),filename);
                throw new IOException("Legacy fixed B1 source exists; conflict retained: "+filename);
            } catch(Throwable primary){WindowsMarkerIo.closeSuppressed(handle,primary);throw propagate(primary);}
        }
    }
    static void requireNativeWorldPath(String value) {
        if(value==null||!value.startsWith("\\\\?\\Volume{")||value.length()<49||value.charAt(48)!='\\'
                ||!value.substring(11,47).matches("[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}")
                ||value.charAt(47)!='}'||value.endsWith("\\"))throw new IllegalArgumentException("Exact canonical native volume GUID world path required");
        for(String component:value.substring(49).split("\\\\",-1))requireFilename(component);
    }
    private static void refuseExisting(WindowsMarkerIo io,Path path,UUID operation)throws IOException {
        WindowsMarkerIo.Handle handle=null;
        try {
            try {handle=io.open(path,WindowsMarkerIo.READ,0,WindowsMarkerIo.OPEN_EXISTING,WindowsMarkerIo.NORMAL|WindowsMarkerIo.OPEN_REPARSE,operation);}
            catch(WindowsMarkerIo.NativeFailure failure){if(failure.win32Error()==2)return;throw failure;}
            throw new IOException("Legacy fixed B1 source exists; conflict retained: "+path.getFileName());
        } catch(Throwable primary){WindowsMarkerIo.closeSuppressed(handle,primary);throw propagate(primary);}
    }
    static Path requirePath(Path path) {
        Objects.requireNonNull(path);
        if (!path.isAbsolute() || path.getParent() == null || path.getFileName() == null)
            throw new IllegalArgumentException("Explicit absolute metadata path required");
        for (Path component : path) requireFilename(component.toString());
        return path;
    }
    static void requireFilename(String name) {
        if (name == null || name.isEmpty() || name.equals(".") || name.equals("..") ||
            name.endsWith(".") || name.endsWith(" ") || name.indexOf('~') >= 0 ||
            name.chars().anyMatch(c -> c < 32 || "<>:\"/\\|?*".indexOf(c) >= 0))
            throw new IllegalArgumentException("Exact nonalias Windows filename required");
        String base = name.split("\\.", 2)[0].toUpperCase(Locale.ROOT);
        if (base.matches("CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³]")) throw new IllegalArgumentException("Reserved Windows filename");
    }
    static void requireExactNativeChild(String parent, String child, String filename) throws IOException {
        requireFilename(filename);
        WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(parent, child);
        String prefix = parent.endsWith("\\") ? parent : parent + "\\";
        if (!child.substring(prefix.length()).equalsIgnoreCase(filename)) throw new IOException("Native filename aliases fixed location");
    }
    static void requireNativeOutside(String configParent, String world) throws IOException {
        if (configParent == null || world == null || !configParent.startsWith("\\\\?\\Volume{") || !world.startsWith("\\\\?\\Volume{"))
            throw new IOException("Actual volume GUID native paths required");
        String p = configParent.toLowerCase(Locale.ROOT), w = world.toLowerCase(Locale.ROOT);
        String prefix = w.endsWith("\\") ? w : w + "\\";
        if (p.equals(w) || p.startsWith(prefix)) throw new IOException("Controlled configuration directory must be outside source world");
    }
    private static void requireIdentity(WindowsMarkerIo.FileIdentity expected, WindowsMarkerIo.FileIdentity actual) throws IOException {
        if (!expected.equals(actual)) throw new IOException("Anchor native file identity conflict");
    }
    private static IOException propagate(Throwable failure) {
        if (failure instanceof IOException io) return io;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        return WindowsMarkerIo.ioFailure("source anchor boundary", failure);
    }
}
