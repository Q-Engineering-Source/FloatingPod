package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** 只验证声明与编码，不签发 native、数据库或 Ready 资格。 */
@Tag("b1-anchor-pure")
class Format3MetadataAnchorTest {
    static final String C2 = "format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed";
    static final String C3 = "format3:all-twenty-fixed-schema-tables-and-static-objects;all-sixty-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed";
    @TempDir Path root;
    MySqlRuntimeMetadata metadata(int schema) throws IOException {
        String properties = B1SourceAnchorFixture.properties(root)
            .replace("approvedSchemaFormat=2", "approvedSchemaFormat=" + schema)
            .replace(C2, schema == 3 ? C3 : C2);
        if (schema == 3) {
            properties += "format3RuntimeContractPath=" + root.resolve("format3-contract.json").toAbsolutePath().toString().replace("\\", "/") + "\n"
                + "format3RuntimeContractSha256=" + "d".repeat(64) + "\n";
        }
        return MySqlRuntimeMetadata.fromBytes(properties.getBytes(StandardCharsets.UTF_8));
    }
    WindowsWorldProvisioner.Plan plan(int schema, int physical) {
        var p = B1SourceAnchorFixture.plan();
        return new WindowsWorldProvisioner.Plan(p.creationOperationUuid(), p.bootstrapOperationUuid(),
            p.prepareWriteUuid(), p.certifyWriteUuid(), p.bindingUuid(), physical, p.mysqlServerUuid(),
            p.schema(), schema, p.ddlSha256(), p.provisioningUuid());
    }
    B1SourceAnchorCodec.Data data(int metadataSchema, int planSchema) throws IOException {
        var r = B1SourceAnchorFixture.receiptData();
        var receipt = new WindowsWorldProvisioner.ReceiptData(plan(planSchema, 2), r.sourceIdentity(),
            r.guardIdentity(), r.creatorPid(), r.creatorCreationTime(), r.creationBoot());
        return new B1SourceAnchorCodec.Data(B1SourceAnchorCodec.configurationBytes(metadata(metadataSchema)),
            WindowsWorldProvisioner.encodeReceipt(receipt), B1SourceAnchorFixture.identity(3),
            B1SourceAnchorFixture.identity(4), B1SourceAnchorFixture.identity(5),
            B1SourceAnchorFixture.PARENT, B1SourceAnchorFixture.WORLD, "runtime.properties");
    }
    @Test void schema3DeclarationAndFullV4RoundTripPreservePhysicalAndNamespace() throws Exception {
        var m = metadata(3); var p = plan(3, 2);
        assertEquals(3, m.baseline().schemaFormat()); assertEquals(C3, m.baseline().coverage());
        assertEquals("FP_MYSQL_APPROVED_STATIC_V1", m.baseline().format());
        assertEquals(2, p.physicalFormat()); B1SourceAnchorCodec.requirePlan(m, p);
        assertEquals(WindowsWorldProvisioner.sha256(B1SourceAnchorCodec.configurationBytes(m)),
            WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(m).configurationSha256());
        assertEquals(B1SourceAnchorCodec.namespace(metadata(2)), B1SourceAnchorCodec.namespace(m));
        var original = data(3, 3); byte[] encoded = B1SourceAnchorCodec.encode(original);
        var decoded = B1SourceAnchorCodec.decode(encoded);
        assertArrayEquals(encoded, B1SourceAnchorCodec.encode(decoded));
        assertArrayEquals(original.configuration(), decoded.configuration());
        assertArrayEquals(original.receipt(), decoded.receipt());
        assertEquals(p, WindowsWorldProvisioner.decodeReceipt(decoded.receipt()).plan());
        assertEquals(B1SourceAnchorFixture.FILETIME,
            WindowsWorldProvisioner.decodeReceipt(decoded.receipt()).creatorCreationTime());
        assertEquals(4, java.nio.ByteBuffer.wrap(encoded).getInt(8));
    }
    @Test void format3ContractPathAndRawShaArePartOfCanonicalConfiguration() throws Exception {
        var original = metadata(3);
        byte[] originalBytes = B1SourceAnchorCodec.configurationBytes(original);
        String base = B1SourceAnchorFixture.properties(root).replace("approvedSchemaFormat=2", "approvedSchemaFormat=3")
            .replace(C2, C3);
        String contractPath = "format3RuntimeContractPath=" + root.resolve("format3-contract.json").toAbsolutePath().toString().replace("\\", "/") + "\n";
        String contractSha = "format3RuntimeContractSha256=" + "d".repeat(64) + "\n";
        var changedPath = MySqlRuntimeMetadata.fromBytes((base + contractPath.replace("format3-contract.json", "other-contract.json") + contractSha).getBytes(StandardCharsets.UTF_8));
        var changedSha = MySqlRuntimeMetadata.fromBytes((base + contractPath + "format3RuntimeContractSha256=" + "e".repeat(64) + "\n").getBytes(StandardCharsets.UTF_8));
        assertFalse(java.util.Arrays.equals(originalBytes, B1SourceAnchorCodec.configurationBytes(changedPath)));
        assertFalse(java.util.Arrays.equals(originalBytes, B1SourceAnchorCodec.configurationBytes(changedSha)));
        assertTrue(new String(originalBytes, StandardCharsets.UTF_8).contains("FP_B1_CONFIGURATION_FORMAT3_V1"));
        assertTrue(new String(originalBytes, StandardCharsets.UTF_8).contains("FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V1"));
    }
    @Test void encodingMarkersAreExactAnchorDomainAndNeverRuntimeProperties() throws Exception {
        var original = data(3, 3);
        String canonical = new String(original.configuration(), StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytes(original.configuration()));
        for (String marker : new String[] {"format3ConfigurationEncoding=FP_B1_CONFIGURATION_FORMAT3_V1\n",
                "format3RuntimeContractEncoding=FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V1\n"}) {
            for (String changed : new String[] {canonical.replace(marker, ""), canonical + marker,
                    canonical.replace(marker, marker.replace("_V1", "_V2")),
                    marker + canonical.replace(marker, "")}) {
                var invalid = new B1SourceAnchorCodec.Data(changed.getBytes(StandardCharsets.UTF_8), original.receipt(),
                    original.receiptFileIdentity(), original.configParentIdentity(), original.metadataFileIdentity(),
                    original.configParentNativePath(), original.worldNativePath(), original.metadataFilename());
                assertThrows(IOException.class, () -> B1SourceAnchorCodec.encode(invalid));
            }
            var legacy = data(2, 2);
            var cross = new B1SourceAnchorCodec.Data((new String(legacy.configuration(), StandardCharsets.UTF_8) + marker)
                .getBytes(StandardCharsets.UTF_8), legacy.receipt(), legacy.receiptFileIdentity(),
                legacy.configParentIdentity(), legacy.metadataFileIdentity(), legacy.configParentNativePath(),
                legacy.worldNativePath(), legacy.metadataFilename());
            assertThrows(IOException.class, () -> B1SourceAnchorCodec.encode(cross));
        }
        byte[] malformed = original.configuration(); malformed[0] = (byte) 0xff;
        var invalidUtf8 = new B1SourceAnchorCodec.Data(malformed, original.receipt(), original.receiptFileIdentity(),
            original.configParentIdentity(), original.metadataFileIdentity(), original.configParentNativePath(),
            original.worldNativePath(), original.metadataFilename());
        assertThrows(java.nio.charset.CharacterCodingException.class, () -> B1SourceAnchorCodec.encode(invalidUtf8));
    }
    @Test void bothCrossSchemaDirectionsRejectBeforeAnchorEncoding() throws Exception {
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.requirePlan(metadata(3), plan(2, 2)));
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.requirePlan(metadata(2), plan(3, 2)));
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.encode(data(3, 2)));
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.encode(data(2, 3)));
    }
    @Test void unsupportedSchemaCoverageAndPhysicalFormatRemainExplicitFailures() {
        for (int s : new int[] {0, 4}) assertThrows(IllegalArgumentException.class, () -> metadata(s));
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytes(
            B1SourceAnchorFixture.properties(root).replace("approvedSchemaFormat=2", "approvedSchemaFormat=3")
                .getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytes(
            B1SourceAnchorFixture.properties(root).replace(C2, C3).getBytes(StandardCharsets.UTF_8)));
        for (int s : new int[] {1, 4}) assertThrows(IllegalArgumentException.class, () -> plan(s, 2));
        assertThrows(IllegalArgumentException.class, () -> plan(3, 1));
        assertThrows(IOException.class, () -> WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(
            MySqlRuntimeMetadata.fromBytes(B1SourceAnchorFixture.properties(root)
                .replace("approvedSchemaFormat=2", "approvedSchemaFormat=1")
                .replace(C2, "format1:all-fixed-schema-tables-and-static-objects;all-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed")
                .getBytes(StandardCharsets.UTF_8))));
    }
    @Test void legacySchema2ReceiptBytesRetainOriginalFieldOrderAndDigest() throws Exception {
        var r = B1SourceAnchorFixture.receiptData(); var p = r.plan();
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeLong(0x4650575243503031L); out.writeInt(1);
            for (UUID id : new UUID[] {p.creationOperationUuid(), p.bootstrapOperationUuid(),
                    p.prepareWriteUuid(), p.certifyWriteUuid(), p.bindingUuid()}) uuid(out, id);
            out.writeInt(2); uuid(out, p.mysqlServerUuid()); text(out, p.schema()); out.writeInt(2);
            out.write(HexFormat.of().parseHex(p.ddlSha256())); uuid(out, p.provisioningUuid());
            text(out, r.sourceIdentity()); text(out, r.guardIdentity());
            out.writeLong(r.creatorPid()); out.writeLong(r.creatorCreationTime());
            uuid(out, r.creationBoot().bootId()); out.writeLong(r.creationBoot().generation()); out.flush();
            out.write(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        }
        assertArrayEquals(bytes.toByteArray(), WindowsWorldProvisioner.encodeReceipt(r));
        var d = data(2, 2); var decoded = B1SourceAnchorCodec.decode(B1SourceAnchorCodec.encode(d));
        assertArrayEquals(d.configuration(), decoded.configuration());
        assertArrayEquals(d.receipt(), decoded.receipt());
    }
    @Test void canonicalNamespaceAcceptsBothFormatsWithoutRuntimeMarkerRelaxation() throws Exception {
        for (int format : new int[] {2, 3}) {
            var m = metadata(format);
            assertEquals(B1SourceAnchorCodec.namespace(m),
                B1SourceAnchorCodec.canonicalConfigurationNamespace(B1SourceAnchorCodec.configurationBytes(m)));
        }
    }
    @Test void canonicalNamespaceRejectsWrongDuplicateMissingMisplacedAndNoncanonicalMarkers() throws Exception {
        String canonical = new String(B1SourceAnchorCodec.configurationBytes(metadata(3)), StandardCharsets.UTF_8);
        for (String marker : new String[] {"format3ConfigurationEncoding=FP_B1_CONFIGURATION_FORMAT3_V1\n",
                "format3RuntimeContractEncoding=FP_B1_CONFIGURATION_FORMAT3_CONTRACT_PIN_V1\n"}) {
            for (String changed : new String[] {canonical.replace(marker, ""), canonical + marker,
                    canonical.replace(marker, marker.replace("_V1", "_V2")),
                    marker + canonical.replace(marker, "")}) {
                assertThrows(IOException.class, () -> B1SourceAnchorCodec.canonicalConfigurationNamespace(
                    changed.getBytes(StandardCharsets.UTF_8)));
            }
        }
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.canonicalConfigurationNamespace(
            (canonical + "\n").getBytes(StandardCharsets.UTF_8)));
        String legacy = new String(B1SourceAnchorCodec.configurationBytes(metadata(2)), StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> B1SourceAnchorCodec.canonicalConfigurationNamespace(
            (legacy + "format3ConfigurationEncoding=FP_B1_CONFIGURATION_FORMAT3_V1\n").getBytes(StandardCharsets.UTF_8)));
    }
    private static void uuid(DataOutputStream out, UUID id) throws IOException {
        out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits());
    }
    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8); out.writeInt(bytes.length); out.write(bytes);
    }
}
