package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

final class B1SourceAnchorFixture {
    static final long FILETIME = 133800000000000123L;
    static final String PARENT = "\\\\?\\Volume{00000000-0000-0000-0000-000000000001}\\fixture\\config";
    static final String WORLD = "\\\\?\\Volume{00000000-0000-0000-0000-000000000001}\\fixture\\world";
    static WindowsWorldProvisioner.Plan plan() {
        return new WindowsWorldProvisioner.Plan(UUID.fromString("0fc62f9f-7e40-4b7c-8c11-6c4446b4d80a"),
            UUID.fromString("5c7867c6-cb41-497b-b5fb-5a3ca8900924"), UUID.fromString("1be6d597-6a89-4e86-8881-247c17ad603d"),
            UUID.fromString("3189ce56-515d-4d01-a5e8-cac6492510ec"), UUID.fromString("3e97c5c5-140e-4663-9e8d-fef3c94082b0"),
            2, UUID.fromString("ed281a8f-ef7b-4b38-bb15-487971aa6210"), "fp_b1_native_fixture", 2, "AB".repeat(32),
            UUID.fromString("d79fdd2d-4a7d-49ee-ab00-d459b6982e9e"));
    }
    static String identity(int suffix) {
        byte[] id = new byte[16]; id[15] = (byte) suffix;
        return new WindowsMarkerIo.FileIdentity(0x1234L, id).versionedIdentity();
    }
    static String properties(Path root) {
        String path = root.toAbsolutePath().toString().replace('\\', '/'); var p = plan();
        return "backend=mysql\nhost=database.example.invalid\nport=3307\nschema=" + p.schema() + "\n"
            + "expectedServerUuid=" + p.mysqlServerUuid() + "\nexpectedBindingUuid=" + p.bindingUuid() + "\n"
            + "expectedEffectiveUser=fixture@localhost\nusername=fixture\npkcs12TrustStorePath=" + path + "/trust.p12\n"
            + "credentialSourceRef=file:" + path + "/credential.txt\ntrustSecretSourceRef=file:" + path + "/trust-secret.txt\n"
            + "connectTimeoutMillis=3000\nsocketTimeoutMillis=10000\nactiveLimit=256\ninnodbFlushLogAtTrxCommit=1\nsyncBinlog=1\n"
            + "approvedBaselinePath=" + path + "/approved.json\napprovedBaselineSha256=" + "a".repeat(64) + "\n"
            + "approvedBaselineFormat=FP_MYSQL_APPROVED_STATIC_V1\napprovedSchemaFormat=2\n"
            + "approvedCoverage=format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed\n"
            + "expectedServerVersion=9.7.2\nexpectedProvisioningUuid=" + p.provisioningUuid() + "\n"
            + "approvedDdlSha256=" + p.ddlSha256() + "\nexpectedAuditDefiner=fixture_audit@localhost\n"
            + "approvedSourceCaptureSha256=" + "b".repeat(64) + "\n";
    }
    static MySqlRuntimeMetadata metadata(Path root) throws IOException {
        return MySqlRuntimeMetadata.fromBytes(properties(root).getBytes(StandardCharsets.UTF_8));
    }
    static WindowsWorldProvisioner.ReceiptData receiptData() {
        return new WindowsWorldProvisioner.ReceiptData(plan(), identity(1), identity(2), 1234, FILETIME,
            new WindowsWorldProvisioner.BootIdentity(UUID.fromString("74b00f82-f777-4b09-8b21-7fbc11c68317"), 1));
    }
    static B1SourceAnchorCodec.Data data(Path root) throws IOException {
        return new B1SourceAnchorCodec.Data(B1SourceAnchorCodec.configurationBytes(metadata(root)),
            WindowsWorldProvisioner.encodeReceipt(receiptData()), identity(3), identity(4), identity(5), PARENT, WORLD, "runtime.properties");
    }
    static Path writeMetadata(Path root) throws IOException {
        Path config = Files.createDirectory(root.resolve("config")); Path path = config.resolve("runtime.properties");
        Files.writeString(path, properties(config), StandardCharsets.UTF_8); return path;
    }
}
