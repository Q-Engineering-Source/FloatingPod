package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlOrdinaryCertifiedHandoffFixture$;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Fresh producer 实际退出后由 V4 ordinary producer 接续；V4 退出后再由独立 CurrentV5 consumer 接续。 */
@Tag("b1-verified-current-v5-handoff-native")
final class WindowsB1VerifiedCurrentV5HandoffNativeTest {
    @TempDir Path temporary;
    private static final String CHILD_JAVA = "fp.b1.resume.child.java";
    private static final String CHILD_CLASSPATH = "fp.b1.resume.child.classpath";

    private static String child(String mode, Path root) throws Exception {
        String java = System.getProperty(CHILD_JAVA);
        String classpath = System.getProperty(CHILD_CLASSPATH);
        if (java == null || classpath == null || !Path.of(java).isAbsolute() || classpath.isBlank())
            throw new IOException("Root must freeze the actual Java 25/native child classpath");
        Path outputLog = root.resolve("control/" + mode + "-child.log");
        Process process = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED", "-cp", classpath,
                Child.class.getName(), mode, root.toString()).redirectErrorStream(true).redirectOutput(outputLog.toFile()).start();
        Throwable primary = null;
        try {
            process.getOutputStream().close();
            assertTrue(process.waitFor(90, TimeUnit.SECONDS), mode + " child exceeded 90 seconds");
            String output = Files.isRegularFile(outputLog) ? Files.readString(outputLog, StandardCharsets.UTF_8) : "child output log is missing";
            assertEquals(0, process.exitValue(), mode + " child failed:\n" + output);
            return output;
        } catch (Throwable failure) {
            primary = failure;
            throw failure;
        } finally {
            if (process.isAlive()) {
                try {
                    process.destroyForcibly();
                    assertTrue(process.waitFor(5, TimeUnit.SECONDS), mode + " child did not terminate after failure");
                } catch (Throwable cleanup) {
                    if (primary != null) primary.addSuppressed(cleanup);
                    else throw cleanup;
                }
            }
        }
    }

    private static Properties resultProperties(Path path) throws IOException {
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return properties;
    }

    private static long requiredLong(Properties values, String key) {
        String raw = values.getProperty(key);
        if (raw == null) throw new IllegalStateException("child result omitted " + key);
        return Long.parseLong(raw);
    }

    @Test void exitedFreshAndV4ProcessesFeedASeparateRealCurrentV5OwnedActor() throws Exception {
        Path root = Files.createDirectory(temporary.resolve("ordinary-current-v5"));
        MySqlOrdinaryCertifiedHandoffFixture$.MODULE$.prepareOrdinaryProcessFixture(root);

        child("fresh", root); // waitFor 返回，确认 Fresh producer 进程已实际退出。
        Files.writeString(root.resolve("control/fresh-exited"), "process-exit-observed=true\n", StandardCharsets.UTF_8);
        assertTrue(Files.isRegularFile(root.resolve("control/fresh-exited")));
        assertTrue(Files.isRegularFile(root.resolve("control/ordinary-source.properties")));
        Properties fresh = resultProperties(root.resolve("control/fresh-result.properties"));
        assertEquals("FRESH_BOOTED", fresh.getProperty("actor"));
        assertEquals("true", fresh.getProperty("approvalLoaded"));
        long freshConnectionId = requiredLong(fresh, "connectionId");

        child("ordinary-v4", root); // 消费真实 Fresh V4 source，并由 actor 发布初始 V5。
        Files.writeString(root.resolve("control/ordinary-v4-exited"), "process-exit-observed=true\n", StandardCharsets.UTF_8);
        Path v4Result = root.resolve("control/ordinary-v4-result.properties");
        assertTrue(Files.isRegularFile(v4Result), "first ordinary actor result is missing");
        Properties first = resultProperties(v4Result);
        assertEquals("V4", first.getProperty("nativeSource"));
        assertEquals("DB_ACQUIRED", first.getProperty("nativeAcknowledgement"));
        assertEquals("true", first.getProperty("writerCasCommitted"));
        assertEquals("true", first.getProperty("full20Witness"));
        assertTrue(requiredLong(first, "writerEpoch") > 7L);
        assertTrue(requiredLong(first, "sqlCommits") > 0L);
        assertTrue(requiredLong(first, "full20LockedReads") >= 20L);
        assertTrue(requiredLong(first, "sessionLockOwnerChecks") > 0L);
        assertTrue(requiredLong(first, "recordCount") > 0L);
        long v4ConnectionId = requiredLong(first, "connectionId");
        assertNotEquals(freshConnectionId, v4ConnectionId, "V4 must use a distinct later synthetic JDBC session");

        child("ordinary-v5", root); // 独立进程必须观察并消费 actor 生成的 CurrentV5。
        Path v5Result = root.resolve("control/ordinary-v5-result.properties");
        assertTrue(Files.isRegularFile(v5Result), "CurrentV5 consumer actor result is missing");
        Properties second = resultProperties(v5Result);
        assertEquals("SETTLED_V5", second.getProperty("nativeSource"));
        assertEquals("DB_ACQUIRED", second.getProperty("nativeAcknowledgement"));
        assertEquals("true", second.getProperty("sourceProcessExited"));
        assertEquals("true", second.getProperty("writerCasCommitted"));
        assertEquals("true", second.getProperty("full20Witness"));
        assertTrue(requiredLong(second, "recordCount") >= 3L);
        assertTrue(requiredLong(second, "sqlCommits") > 0L);
        assertTrue(requiredLong(second, "full20LockedReads") >= 20L);
        assertTrue(requiredLong(second, "sessionLockOwnerChecks") > 0L);
        long v5ConnectionId = requiredLong(second, "connectionId");
        assertNotEquals(v4ConnectionId, v5ConnectionId, "CurrentV5 must use a distinct later synthetic JDBC session");
        assertNotEquals(freshConnectionId, v5ConnectionId, "CurrentV5 must not reuse the Fresh synthetic JDBC session");
    }

    public static final class Child {
        private Child() {}
        public static void main(String[] args) throws Exception {
            if (args.length != 2) throw new IllegalArgumentException("exact ordinary actor mode and root are required");
            if (!java.util.Set.of("fresh", "ordinary-v4", "ordinary-v5").contains(args[0]))
                throw new IllegalArgumentException("unknown ordinary actor mode");
            MySqlOrdinaryCertifiedHandoffFixture$.MODULE$.runOrdinaryProcessChild(args[0], Path.of(args[1]));
        }
    }
}
