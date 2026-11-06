package com.wi.floatingpod.config;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CharacterCodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;
@Tag("b1-anchor-pure")
class MySqlRuntimeMetadataBytesTest {
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
    @Test void singleHandleBytesAndPathReaderShareStrictSemanticsAndNeverMutateCaller() throws Exception {
        byte[] caller = fixture().getBytes(StandardCharsets.UTF_8), before = caller.clone();
        MySqlRuntimeMetadata decoded = MySqlRuntimeMetadata.fromBytes(caller);
        assertArrayEquals(before, caller);
        Path path = temporary.resolve("runtime.properties"); Files.write(path, caller);
        MySqlRuntimeMetadata disk = MySqlRuntimeMetadata.load(path);
        assertEquals(disk.host(), decoded.host()); assertEquals(disk.port(), decoded.port());
        assertEquals(disk.baseline(), decoded.baseline());
        assertEquals(disk.credentialSourceRef(), decoded.credentialSourceRef());
        Arrays.fill(caller, (byte) 0);
        assertEquals("database.example.invalid", decoded.host());
    }
    @Test void byteDecoderRetainsBomUtf8DuplicateUnknownAndSqliteRejections() {
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytes(null));
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytes(new byte[65537]));
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytes(new byte[]{(byte)0xef,(byte)0xbb,(byte)0xbf}));
        assertThrows(CharacterCodingException.class, () -> MySqlRuntimeMetadata.fromBytes(new byte[]{(byte)0xc0}));
        for (String suffix : new String[]{"host=second.example.invalid\n", "password=synthetic-secret\n", "sqlitePath=legacy.sqlite\n"}) {
            byte[] input = (fixture() + suffix).getBytes(StandardCharsets.UTF_8), before = input.clone();
            assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytes(input));
            assertArrayEquals(before, input);
        }
        assertThrows(IllegalArgumentException.class, () -> MySqlRuntimeMetadata.fromBytes(
            fixture().replace("backend=mysql", "backend=sqlite").getBytes(StandardCharsets.UTF_8)));
    }

    @Test void schemaFiveByteBoundaryRejectsMalformedUnknownDuplicateAndOversizedWithoutMutatingCaller() {
        for (byte[] input : new byte[][] {new byte[65537], {(byte)0xef,(byte)0xbb,(byte)0xbf}, {(byte)0xc0},
                "backend=mysql\nbackend=mysql\n".getBytes(StandardCharsets.UTF_8),
                "password=synthetic-only\n".getBytes(StandardCharsets.UTF_8)}) {
            byte[] before = input.clone();
            assertThrows(Exception.class, () -> MySqlRuntimeMetadata.fromDeploymentReceiptBytes(input));
            assertArrayEquals(before, input);
        }
    }
}
