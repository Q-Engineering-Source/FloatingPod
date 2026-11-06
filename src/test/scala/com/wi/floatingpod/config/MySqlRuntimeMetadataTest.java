package com.wi.floatingpod.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class MySqlRuntimeMetadataTest {
    @TempDir Path temporary;
    private String fixture() {
        String path = temporary.toAbsolutePath().toString().replace("\\", "/");
        return "backend=mysql\nhost=database.example.invalid\nport=3307\nschema=fp_fixture\n"
                + "expectedServerUuid=00000000-0000-0000-0000-000000000001\n"
                + "expectedBindingUuid=00000000-0000-0000-0000-000000000002\n"
                + "expectedEffectiveUser=fp_fixture@localhost\nusername=fp_fixture\n"
                + "pkcs12TrustStorePath=" + path + "/trust.p12\n"
                + "credentialSourceRef=file:" + path + "/credential.txt\ntrustSecretSourceRef=file:" + path + "/trust-secret.txt\n"
                + "connectTimeoutMillis=3000\nsocketTimeoutMillis=10000\nactiveLimit=256\n"
                + "innodbFlushLogAtTrxCommit=1\nsyncBinlog=1\napprovedBaselinePath=" + path + "/approved.json\n"
                + "approvedBaselineSha256=" + "a".repeat(64) + "\napprovedBaselineFormat=FP_MYSQL_APPROVED_STATIC_V1\n"
                + "approvedSchemaFormat=2\n"
                + "approvedCoverage=format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed\n"
                + "expectedServerVersion=9.7.2\nexpectedProvisioningUuid=00000000-0000-0000-0000-000000000003\n"
                + "approvedDdlSha256=" + "b".repeat(64) + "\nexpectedAuditDefiner=fp_fixture@localhost\n"
                + "approvedSourceCaptureSha256=" + "c".repeat(64) + "\n";
    }
    private MySqlRuntimeMetadata load(String content) throws IOException {
        Path metadata = temporary.resolve("runtime.properties");
        Files.writeString(metadata, content, StandardCharsets.UTF_8);
        return MySqlRuntimeMetadata.load(metadata);
    }

    @Test void explicitMetadataPreservesAllConnectionAndIndependentBaselinePins() throws Exception {
        MySqlRuntimeMetadata metadata = load(fixture());
        assertEquals("database.example.invalid", metadata.host()); assertEquals(3307, metadata.port());
        assertEquals("fp_fixture", metadata.schema()); assertEquals("fp_fixture", metadata.username());
        assertEquals("fp_fixture@localhost", metadata.expectedEffectiveUser());
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000001"), metadata.expectedServerUuid());
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000002"), metadata.expectedBindingUuid());
        assertEquals(Duration.ofMillis(3000), metadata.connectTimeout()); assertEquals(Duration.ofMillis(10000), metadata.socketTimeout());
        assertEquals(256, metadata.activeLimit()); assertEquals(1, metadata.innodbFlushLogAtTrxCommit()); assertEquals(1L, metadata.syncBinlog());
        assertEquals("a".repeat(64), metadata.baseline().fileSha256()); assertEquals("b".repeat(64), metadata.baseline().ddlSha256());
        assertEquals("c".repeat(64), metadata.baseline().sourceCaptureSha256()); assertEquals(2, metadata.baseline().schemaFormat());
        assertEquals(temporary.resolve("credential.txt").toAbsolutePath(), metadata.credentialSourceRef().absolutePath());
        assertEquals(temporary.resolve("trust-secret.txt").toAbsolutePath(), metadata.trustSecretSourceRef().absolutePath());
        assertEquals(temporary.resolve("trust.p12").toAbsolutePath(), metadata.pkcs12TrustStorePath());
        assertEquals(temporary.resolve("approved.json").toAbsolutePath(), metadata.baseline().absolutePath());
        assertEquals("MySqlRuntimeMetadata[redacted]", metadata.toString());
        assertEquals("MySqlBaselineMetadata[redacted]", metadata.baseline().toString());
        assertEquals("FileReference[redacted]", metadata.credentialSourceRef().toString());
        assertFalse(Files.exists(metadata.credentialSourceRef().absolutePath()), "metadata must not open/create secret files");
    }

    @Test void schemaThreeRequiresBothIndependentRuntimeContractPinsAndOtherProfilesRejectThem() throws Exception {
        String f3 = fixture().replace("approvedSchemaFormat=2", "approvedSchemaFormat=3")
                .replace("format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed",
                        "format3:all-twenty-fixed-schema-tables-and-static-objects;all-sixty-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed");
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> load(f3));
        assertEquals("format3 runtime contract path and raw SHA-256 are required", missing.getMessage());
        MySqlRuntimeMetadata accepted = load(f3 + "format3RuntimeContractPath=" + temporary.resolve("contract.json").toAbsolutePath().toString().replace("\\", "/") + "\n"
                + "format3RuntimeContractSha256=" + "d".repeat(64) + "\n");
        assertEquals(temporary.resolve("contract.json").toAbsolutePath(), accepted.format3RuntimeContract().orElseThrow().absolutePath());
        assertEquals("d".repeat(64), accepted.format3RuntimeContract().orElseThrow().fileSha256());
        assertEquals("MySqlRuntimeContract[redacted]", accepted.format3RuntimeContract().orElseThrow().toString());
        assertThrows(IllegalArgumentException.class, () -> load(fixture() + "format3RuntimeContractPath=/tmp/contract.json\n"));
    }
    @Test void sqliteAndLegacyPathAreRejectedWithOfflineMigrationDirection() {
        IllegalArgumentException backend = assertThrows(IllegalArgumentException.class, () -> load(fixture().replace("backend=mysql", "backend=sqlite")));
        assertTrue(backend.getMessage().contains("offline migration"));
        IllegalArgumentException path = assertThrows(IllegalArgumentException.class, () -> load(fixture() + "sqlitePath=old.sqlite\n"));
        assertTrue(path.getMessage().contains("offline migration"));
        assertThrows(IllegalArgumentException.class, () -> load(fixture() + "sqlite=old.sqlite\n"));
    }

    @Test void missingUnknownDuplicateAndSecretPropertiesFailExplicitly() {
        assertThrows(IllegalArgumentException.class, () -> load(fixture().replace("host=database.example.invalid\n", "")));
        assertThrows(IllegalArgumentException.class, () -> load(fixture().replace("host=database.example.invalid", "host=UNKNOWN")));
        assertThrows(IllegalArgumentException.class, () -> load(fixture() + "host=other.example.invalid\n"));
        IllegalArgumentException secret = assertThrows(IllegalArgumentException.class, () -> load(fixture() + "password=synthetic-private-fixture\n"));
        assertEquals("unknown runtime metadata property", secret.getMessage());
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.load(null));
    }

    @Test void rangesUuidHashesAndProfileMismatchDoNotFallback() {
        for (String[] replacement : new String[][] {
                {"port=3307", "port=0"}, {"port=3307", "port=65536"}, {"activeLimit=256", "activeLimit=257"},
                {"host=database.example.invalid", "host=999.999.999.999"},
                {"host=database.example.invalid", "host=bad..hostname"},
                {"activeLimit=256", "activeLimit=0"}, {"connectTimeoutMillis=3000", "connectTimeoutMillis=30001"},
                {"socketTimeoutMillis=10000", "socketTimeoutMillis=300001"}, {"syncBinlog=1", "syncBinlog=0"},
                {"approvedSchemaFormat=2", "approvedSchemaFormat=1"}, {"expectedServerVersion=9.7.2", "expectedServerVersion=UNKNOWN"},
                {"00000000-0000-0000-0000-000000000001", "1-1-1-1-1"}, {"a".repeat(64), "abc"} }) {
            assertThrows(IllegalArgumentException.class, () -> load(fixture().replace(replacement[0], replacement[1])));
        }
    }

    @Test void pathsAndFileReferenceSchemeAreExplicitAndNeverUriDecoded() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fileReference("env:SECRET"));
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fileReference("file:relative.txt"));
        Path literal = temporary.resolve("space%20literal.txt").toAbsolutePath();
        assertEquals(literal, MySqlRuntimeMetadata.fileReference("file:" + literal).absolutePath());
        assertThrows(IllegalArgumentException.class, () -> load(fixture().replace("pkcs12TrustStorePath=" + temporary.toAbsolutePath().toString().replace("\\", "/") + "/trust.p12", "pkcs12TrustStorePath=relative.p12")));
    }

    @Test void malformedUtf8BomAndOversizedMetadataAreRejected() throws Exception {
        Path path = temporary.resolve("invalid.properties");
        Files.write(path, new byte[] {(byte) 0xc3, 0x28});
        assertThrows(CharacterCodingException.class, () -> MySqlRuntimeMetadata.load(path));
        Files.writeString(path, "\ufeff" + fixture(), StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.load(path));
        Files.write(path, new byte[MySqlRuntimeMetadata.MAX_METADATA_BYTES + 1]);
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.load(path));
    }
    private byte[] fullV2Fixture() throws Exception {
        var values = new Properties();
        values.load(new StringReader(fixture()));
        values.setProperty("approvedSchemaFormat","3");
        values.setProperty("approvedCoverage","format3:all-twenty-fixed-schema-tables-and-static-objects;all-sixty-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed");
        values.setProperty("approvedDdlSha256","B".repeat(64));
        values.setProperty("format3RuntimeContractPath",temporary.resolve("contract.json").toAbsolutePath().toString());
        values.setProperty("format3RuntimeContractSha256","D".repeat(64));
        values.setProperty("format3ManifestConfigurationEncoding",MySqlRuntimeMetadata.MANIFEST_V2);
        for (String key : new String[]{"pkcs12TrustStorePath","approvedBaselinePath"})
            values.setProperty(key,Path.of(values.getProperty(key)).toString());
        for (String key : new String[]{"credentialSourceRef","trustSecretSourceRef"})
            values.setProperty(key,"file:"+Path.of(values.getProperty(key).substring(5)));
        var writer = new StringWriter();values.store(writer,null);
        return writer.toString().getBytes(StandardCharsets.UTF_8);
    }
    @Test void explicitFullV2OwnTypePreservesAllPinsAndLegacyRejectsSelector() throws Exception {
        byte[] bytes=fullV2Fixture(),before=bytes.clone();
        var value=MySqlRuntimeMetadata.fromBytesV2(bytes);
        assertArrayEquals(before,bytes);
        assertEquals(MySqlRuntimeMetadata.FullV2.class,value.getClass());
        assertEquals(MySqlRuntimeMetadata.MANIFEST_V2,value.format3ManifestConfigurationEncoding());
        assertEquals("a".repeat(64),value.baseline().fileSha256());
        assertEquals("c".repeat(64),value.baseline().sourceCaptureSha256());
        assertEquals("D".repeat(64),value.format3RuntimeContract().fileSha256());
        Path file=temporary.resolve("full-v2.properties");Files.write(file,bytes);
        assertEquals(value.baseline(),MySqlRuntimeMetadata.loadV2(file).baseline());
        assertThrows(IllegalArgumentException.class,()->MySqlRuntimeMetadata.fromBytes(bytes));
        assertThrows(IllegalArgumentException.class,()->MySqlRuntimeMetadata.fromBytesV2(fixture().getBytes(StandardCharsets.UTF_8)));
    }
    @Test void fullV2RequiresActualPinsSelectorClosedKeysAndCanonicalDeployment() throws Exception {
        String good=new String(fullV2Fixture(),StandardCharsets.UTF_8);
        for (String key : new String[]{"approvedBaselineSha256","approvedSourceCaptureSha256","format3RuntimeContractSha256","format3ManifestConfigurationEncoding"}) {
            String missing=good.replaceAll("(?m)^"+key+"=.*\n?","");
            assertThrows(IllegalArgumentException.class,()->MySqlRuntimeMetadata.fromBytesV2(missing.getBytes(StandardCharsets.UTF_8)),key);
        }
        for (String changed : new String[]{good+"host=other.invalid\n",good+"unknown=value\n",
                good.replace("port=3307","port=03307"),good.replace(MySqlRuntimeMetadata.MANIFEST_V2,"V1"),
                good.replace("D".repeat(64),"bad")})
            assertThrows(IllegalArgumentException.class,()->MySqlRuntimeMetadata.fromBytesV2(changed.getBytes(StandardCharsets.UTF_8)));
        assertThrows(CharacterCodingException.class,()->MySqlRuntimeMetadata.fromBytesV2(new byte[]{(byte)0xC3,0x28}));
    }

    @Test void fullV2RejectsDecodedUnpairedSurrogatesButPreservesValidPair() throws Exception {
        String good=new String(fullV2Fixture(),StandardCharsets.UTF_8);
        for (String escaped:new String[]{"\\uD800","\\uD801","\\uDC00","x\\uD800y"}) {
            String malformed=good.replaceAll("(?m)^username=.*$","username="+java.util.regex.Matcher.quoteReplacement(escaped));
            assertThrows(IllegalArgumentException.class,()->MySqlRuntimeMetadata.fromBytesV2(malformed.getBytes(StandardCharsets.UTF_8)));
        }
        String paired=good.replaceAll("(?m)^username=.*$","username="+java.util.regex.Matcher.quoteReplacement("\\uD83D\\uDE80"));
        assertEquals("\uD83D\uDE80",MySqlRuntimeMetadata.fromBytesV2(paired.getBytes(StandardCharsets.UTF_8)).username());
    }

    @Test void fullV3PreservesActualSelectorAndV2ProjectionDoesNotFallback() throws Exception {
        String text=new String(fullV2Fixture(),StandardCharsets.UTF_8).replace(MySqlRuntimeMetadata.MANIFEST_V2,MySqlRuntimeMetadata.MANIFEST_V3);
        var value=MySqlRuntimeMetadata.fromBytesV2(text.getBytes(StandardCharsets.UTF_8));
        assertEquals(MySqlRuntimeMetadata.MANIFEST_V3,value.format3ManifestConfigurationEncoding());
        var projection=value.deploymentSemantics().canonicalFields();
        assertEquals(projection,MySqlRuntimeMetadata.parseManifestV3Projection(projection).canonicalFields());
        assertThrows(IllegalArgumentException.class,()->MySqlRuntimeMetadata.parseManifestV2Projection(projection));
        assertThrows(IllegalArgumentException.class,()->MySqlRuntimeMetadata.fromBytesV2(text.replace(MySqlRuntimeMetadata.MANIFEST_V3,"unknown").getBytes(StandardCharsets.UTF_8)));
    }

    private String schemaSixFixture() {
        return fixture().replace("approvedSchemaFormat=2", "approvedSchemaFormat=6")
                .replace("format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed",
                        MySqlRuntimeMetadata.DB_FIRST_COVERAGE)
                + "dbFirstRuntimeContractPath=" + temporary.resolve("contract6.json").toAbsolutePath().toString().replace("\\", "/") + "\n"
                + "dbFirstRuntimeContractSha256=" + "d".repeat(64) + "\n"
                + "dbFirstManifestConfigurationEncoding=" + MySqlRuntimeMetadata.DB_FIRST_MANIFEST + "\n";
    }

    @Test void schemaSixPinsRoundTripWithoutOpeningSecretsOrEnteringOlderFamilies() throws Exception {
        String source = schemaSixFixture();
        var actual = MySqlRuntimeMetadata.fromDbFirstBytes(source.getBytes(StandardCharsets.UTF_8));
        assertEquals(6, actual.baseline().schemaFormat());
        assertEquals(MySqlRuntimeMetadata.DB_FIRST_COVERAGE, actual.baseline().coverage());
        assertEquals(temporary.resolve("contract6.json").toAbsolutePath(), actual.runtimeContract().absolutePath());
        assertEquals("d".repeat(64), actual.runtimeContract().fileSha256());
        var complete = actual.completeCanonicalFields();
        assertEquals("D".repeat(64), complete.get("dbFirstRuntimeContractSha256"));
        assertFalse(actual.deploymentSemantics().canonicalFields().containsKey("dbFirstRuntimeContractSha256"));
        var out = new StringBuilder();
        complete.entrySet().stream().sorted(java.util.Map.Entry.comparingByKey()).forEach(e ->
                out.append(e.getKey()).append('=').append(e.getValue().replace("\\", "\\\\")).append('\n'));
        assertEquals(complete, MySqlRuntimeMetadata.fromDbFirstBytes(out.toString().getBytes(StandardCharsets.UTF_8)).completeCanonicalFields());
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromDeploymentReceiptBytes(source.getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytesV2(source.getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> load(source));
        assertFalse(Files.exists(temporary.resolve("credential.txt")));
        assertFalse(Files.exists(temporary.resolve("contract6.json")));
    }

    @Test void schemaSixRejectsMixedSelectorsMissingPinsDuplicatesAndMalformedUtf8() {
        String source = schemaSixFixture();
        for (String bad : java.util.List.of(
                source.replace("approvedSchemaFormat=6", "approvedSchemaFormat=5"),
                source.replace(MySqlRuntimeMetadata.DB_FIRST_MANIFEST, MySqlRuntimeMetadata.DEPLOYMENT_RECEIPT_MANIFEST),
                source.replace("dbFirstRuntimeContractSha256=" + "d".repeat(64) + "\n", ""),
                source + "dbFirstRuntimeContractSha256=" + "d".repeat(64) + "\n",
                source + "deploymentReceiptRuntimeContractSha256=" + "d".repeat(64) + "\n",
                source.replace("activeLimit=256", "activeLimit=257"),
                source.replace("innodbFlushLogAtTrxCommit=1", "innodbFlushLogAtTrxCommit=2"))) {
            assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromDbFirstBytes(bad.getBytes(StandardCharsets.UTF_8)));
        }
        assertThrows(java.nio.charset.CharacterCodingException.class,
                () -> MySqlRuntimeMetadata.fromDbFirstBytes(new byte[] {(byte)0xc3, 0x28}));
        assertThrows(IllegalArgumentException.class,
                () -> MySqlRuntimeMetadata.fromDbFirstBytes(new byte[] {(byte)0xef, (byte)0xbb, (byte)0xbf}));
    }

    @Test void schemaFiveHasClosedStaticMetadataAndCannotEnterLegacyRuntime() throws Exception {
        String text = fixture().replace("approvedSchemaFormat=2", "approvedSchemaFormat=5")
                .replace("format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed",
                        "script-publication1-deployment-receipt1:exact26-tables-exact78-guards-auditself-and-privileges")
                + "deploymentReceiptRuntimeContractPath=" + temporary.resolve("contract.json").toAbsolutePath().toString().replace("\\", "/") + "\n"
                + "deploymentReceiptRuntimeContractSha256=" + "d".repeat(64) + "\n"
                + "deploymentReceiptManifestConfigurationEncoding=" + MySqlRuntimeMetadata.DEPLOYMENT_RECEIPT_MANIFEST + "\n";
        var value = MySqlRuntimeMetadata.fromDeploymentReceiptBytes(text.getBytes(StandardCharsets.UTF_8));
        assertEquals(5, value.baseline().schemaFormat());
        assertEquals("d".repeat(64), value.runtimeContract().fileSha256());
        assertFalse(value.deploymentSemantics().canonicalFields().containsKey("approvedBaselineSha256"));
        assertTrue(value.completeCanonicalFields().containsKey("approvedBaselineSha256"));
        assertTrue(value.completeCanonicalFields().containsKey("deploymentReceiptRuntimeContractSha256"));
        assertThrows(IllegalArgumentException.class, () -> load(text));
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromDeploymentReceiptBytes(
                (text + "format3RuntimeContractSha256=" + "e".repeat(64) + "\n").getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromDeploymentReceiptBytes(
                text.replace(MySqlRuntimeMetadata.DEPLOYMENT_RECEIPT_MANIFEST, MySqlRuntimeMetadata.MANIFEST_V3).getBytes(StandardCharsets.UTF_8)));
        assertFalse(Files.exists(temporary.resolve("credential.txt")));
    }
}
