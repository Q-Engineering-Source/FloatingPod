package com.wi.floatingpod.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class ManagedDatabaseConfigurationTest {
    @TempDir Path root;

    private Path config(String contents) throws IOException {
        Path path = root.resolve("config/floatingpod/database.properties");
        Files.createDirectories(path.getParent());
        Files.writeString(path, contents, StandardCharsets.UTF_8);
        return path;
    }

    private ManagedDatabaseConfiguration load(String contents) throws IOException {
        Path file = config(contents);
        return ManagedDatabaseConfiguration.load(root, file);
    }

    private String minimum(String credential) {
        return "DBHost=db.example.test\nPort=33317\nDatabase=floatingpod\nUsername=fp_runtime\n" +
            "TrustStore=certs/db.p12\nTrustStorePasswordFile=secrets/trust.pass\n" + credential;
    }

    @Test void loadsOrdinarySettingsAndDoesNotExposeUnicodePassword() throws Exception {
        String secret = "秘密-密碼-🛰";
        try (var settings = load(minimum("Password=" + secret + "  \n"))) {
            assertEquals("db.example.test", settings.host());
            assertEquals(33317, settings.port());
            assertEquals("floatingpod", settings.database());
            assertEquals("fp_runtime", settings.username());
            char[][] observed = new char[1][];
            int length = settings.withPassword(chars -> { observed[0] = chars; return chars.length; });
            assertEquals(secret.length() + 2, length);
            assertArrayEquals(new char[secret.length() + 2], observed[0], "temporary copy is zeroed after callback");
            assertEquals(secret.length() + 2, settings.withPassword(chars -> chars.length).intValue());
            assertEquals(10_000, settings.connectTimeoutMillis());
            assertEquals(30_000, settings.socketTimeoutMillis());
            assertEquals(180_000, settings.initializationTimeoutMillis());
            assertEquals(5_000, settings.lockTimeoutMillis());
            assertFalse(settings.toString().contains(secret));
            assertFalse(settings.toString().contains("fp_runtime"));
        }
    }

    @Test void resolvesCredentialAndTrustPathsAgainstInstanceRootWithoutReadingThem() throws Exception {
        try (var settings = load(minimum("PasswordFile=secrets/runtime.pass\n"))) {
            assertTrue(settings.usesPasswordFile());
            assertThrows(IllegalStateException.class, () -> settings.withPassword(chars -> chars.length));
            assertEquals(root.resolve("secrets/runtime.pass").normalize(), settings.passwordFile().orElseThrow());
            assertEquals(root.resolve("certs/db.p12").normalize(), settings.trustStore().orElseThrow());
            assertEquals(root.resolve("secrets/trust.pass").normalize(), settings.trustStorePasswordFile().orElseThrow());
        }
    }

    @Test void rejectsSettingsOutsideTheClosedSchema6MetadataBoundaryBeforeInitialization() throws Exception {
        assertInvalid(minimum("PasswordFile=secret\n").replace("Database=floatingpod", "Database=FloatingPod"), "Database");
        assertInvalid(minimum("PasswordFile=secret\n").replace("Database=floatingpod", "Database=1floatingpod"), "Database");
        assertInvalid(minimum("PasswordFile=secret\nConnectTimeoutMillis=30001\n"), "ConnectTimeoutMillis");
        assertInvalid(minimum("PasswordFile=secret\nSocketTimeoutMillis=300001\n"), "SocketTimeoutMillis");
        assertInvalid(minimum("PasswordFile=secret\nURLParams=connectTimeout=30001\n"), "ConnectTimeoutMillis");
        assertInvalid(minimum("PasswordFile=secret\nURLParams=socketTimeout=300001\n"), "SocketTimeoutMillis");
        try (var settings = load(minimum("PasswordFile=secret\nConnectTimeoutMillis=30000\nSocketTimeoutMillis=300000\n"))) {
            assertEquals(30_000, settings.connectTimeoutMillis());
            assertEquals(300_000, settings.socketTimeoutMillis());
        }
    }

    @Test void acceptsOnlySafeMappedUrlHintsAndUsesTheirTypedTimeouts() throws Exception {
        try (var settings = load(minimum("PasswordFile=secret\nURLParams=\n"))) {
            assertEquals(10_000, settings.connectTimeoutMillis());
        }
        String params = "connectTimeout=15000&socketTimeout=60000&sslMode=VERIFY_IDENTITY&" +
            "allowMultiQueries=false&useSSL=true&requireSSL=true&verifyServerCertificate=true";
        try (var settings = load(minimum("PasswordFile=secret\nURLParams=" + params + "\n"))) {
            assertEquals(15_000, settings.connectTimeoutMillis());
            assertEquals(60_000, settings.socketTimeoutMillis());
        }
        try (var settings = load(minimum("PasswordFile=secret\nURLParams=useSSL=true&verifyServerCertificate=true\n"))) {
            assertEquals(10_000, settings.connectTimeoutMillis());
        }
    }

    @Test void rejectsMalformedStructureAndSecretsRemainAbsentFromFailures() throws Exception {
        assertInvalid(minimum("Password=x\nPasswordFile=secret\n"), "exactly one");
        assertInvalid(minimum("Password=SECRET_SENTINEL\nDBHost=other\n"), "duplicate key");
        assertInvalid(minimum("Password=x\nBogus=y\n"), "unknown key");
        assertInvalid(minimum("Password=x\n").replace("Database=floatingpod", "Database=bad-name"), "Database");
        assertInvalid(minimum("Password=x\n").replace("Username=fp_runtime", "Username=user@host"), "Username");
        assertInvalid("Port=33317\nDatabase=floatingpod\nUsername=fp_runtime\nPassword=SECRET_SENTINEL\n", "DBHost");
        assertInvalid(minimum("Password=x\n").replace("Port=33317", "Port=65536"), "Port");
        assertInvalid(minimum("Password=x\nConnectTimeoutMillis=0\n"), "range");
        var secretFailure = assertThrows(IllegalArgumentException.class,
            () -> load(minimum("PasswordFile=secret\nConnectTimeoutMillis=SECRET_SENTINEL\n")));
        assertNull(secretFailure.getCause());
        assertInvalid(minimum("Password=x\nInitializationTimeoutMillis=1000\nLockTimeoutMillis=1001\n"), "must not exceed");
        assertInvalid(minimum("Password=x\n").replace("DBHost=db.example.test", "DBHost=db/path"), "DBHost");
        assertInvalid(minimum("Password=x\n").replace("DBHost=db.example.test", "DBHost=db\\\\path"), "DBHost");
        assertInvalid(minimum("Password=x\n").replace("TrustStorePasswordFile=secrets/trust.pass\n", ""), "TrustStore and TrustStorePasswordFile are required");
        assertInvalid("DBHost=host\nPort=33317\nDatabase=floatingpod\nUsername=fp_runtime\nPasswordFile=secret\n", "TrustStore and TrustStorePasswordFile are required");
    }

    @Test void rejectsUrlParameterDuplicatesUnknownUnsafeValuesAndExplicitConflicts() throws Exception {
        assertInvalid(minimum("PasswordFile=secret\nURLParams=useSSL=false\n"), "useSSL");
        assertInvalid(minimum("PasswordFile=secret\nURLParams=verifyServerCertificate=false\n"), "verifyServerCertificate");
        assertInvalid(minimum("PasswordFile=secret\nURLParams=unknown=true\n"), "unsupported parameter");
        assertInvalid(minimum("PasswordFile=secret\nURLParams=sslMode=DISABLED\n"), "sslMode");
        assertInvalid(minimum("PasswordFile=secret\nURLParams=useSSL=true&useSSL=true\n"), "duplicate");
        assertInvalid(minimum("PasswordFile=secret\nConnectTimeoutMillis=10000\nURLParams=connectTimeout=10000\n"), "conflicts");
        assertInvalid(minimum("PasswordFile=secret\nURLParams=socketTimeout=0\n"), "range");
        assertInvalid(minimum("PasswordFile=secret\nURLParams=connectTimeout=3000000\n"), "range");
    }

    @Test void rejectsInvalidUtf8BomOversizeAndWrongLocation() throws Exception {
        Path path = config(minimum("Password=x\n"));
        byte[] good = Files.readAllBytes(path);
        byte[] bom = new byte[good.length + 3];
        bom[0] = (byte) 0xef; bom[1] = (byte) 0xbb; bom[2] = (byte) 0xbf;
        System.arraycopy(good, 0, bom, 3, good.length);
        Files.write(path, bom);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> ManagedDatabaseConfiguration.load(root, path))
            .getMessage().contains("BOM"));

        Files.write(path, new byte[]{(byte) 0xc3, (byte) 0x28});
        var malformed = assertThrows(IllegalArgumentException.class, () -> ManagedDatabaseConfiguration.load(root, path));
        assertNotNull(malformed.getCause());

        Files.write(path, new byte[64 * 1024 + 1]);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> ManagedDatabaseConfiguration.load(root, path))
            .getMessage().contains("64 KiB"));
        assertThrows(IllegalArgumentException.class, () -> ManagedDatabaseConfiguration.load(root, root.resolve("elsewhere.properties")));
    }

    @Test void closeWipesOwnedPasswordAndRejectsFurtherAccess() throws Exception {
        var settings = load(minimum("Password=secret\n"));
        settings.close();
        assertThrows(IllegalStateException.class, () -> settings.withPassword(chars -> chars.length));
        settings.close();
        assertFalse(settings.toString().contains("secret"));
    }

    private void assertInvalid(String text, String expectedMessage) throws IOException {
        var failure = assertThrows(IllegalArgumentException.class, () -> load(text));
        assertTrue(failure.getMessage().contains(expectedMessage), failure.getMessage());
        assertFalse(failure.getMessage().contains("SECRET_SENTINEL"));
    }
}
