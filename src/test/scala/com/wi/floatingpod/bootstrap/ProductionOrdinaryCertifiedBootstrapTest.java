package com.wi.floatingpod.bootstrap;

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

/**
 * 在独立 Java 进程中运行真实 Fresh owned actor，并要求其原生 source facts 生成已固定的 ordinary-creation 输入。
 * Scripted JDBC 始终只是合成测试 DATA。
 */
@Tag("mysql-ordinary-certified-bootstrap")
final class ProductionOrdinaryCertifiedBootstrapTest {
    @TempDir Path temporary;

    private static final String CHILD_JAVA = "fp.b1.resume.child.java";
    private static final String CHILD_CLASSPATH = "fp.b1.resume.child.classpath";

    private static void runFreshChild(Path root) throws Exception {
        String java = System.getProperty(CHILD_JAVA);
        String classpath = System.getProperty(CHILD_CLASSPATH);
        if (java == null || classpath == null || !Path.of(java).isAbsolute() || classpath.isBlank())
            throw new IOException("Root must freeze the actual Java 25/native child classpath");
        Path outputLog = root.resolve("control/fresh-child.log");
        Process process = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED", "-cp", classpath,
                Child.class.getName(), "fresh", root.toString()).redirectErrorStream(true).redirectOutput(outputLog.toFile()).start();
        Throwable primary = null;
        try {
            process.getOutputStream().close();
            assertTrue(process.waitFor(90, TimeUnit.SECONDS), "Fresh producer child exceeded 90 seconds");
            String output = Files.isRegularFile(outputLog) ? Files.readString(outputLog, StandardCharsets.UTF_8) : "child output log is missing";
            assertEquals(0, process.exitValue(), output);
        } catch (Throwable failure) {
            primary = failure;
            throw failure;
        } finally {
            if (process.isAlive()) {
                try {
                    process.destroyForcibly();
                    assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Fresh child did not terminate after failure");
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

    @Test void actualFreshOwnedActorPublishesLoadableOriginalCreationAfterItsWorkerCloses() throws Exception {
        Path root = Files.createDirectory(temporary.resolve("fresh-source"));
        MySqlOrdinaryCertifiedHandoffFixture$.MODULE$.prepareOrdinaryProcessFixture(root);
        runFreshChild(root);

        Path approval = root.resolve("control/ordinary-source.properties");
        Path result = root.resolve("control/fresh-result.properties");
        assertTrue(Files.isRegularFile(approval), "actual Fresh source approval was not published");
        assertTrue(Files.size(approval) > 0);
        assertTrue(Files.isRegularFile(result), "Fresh actor result was not recorded after owned-worker shutdown");
        Properties facts = resultProperties(result);
        assertEquals("FRESH_BOOTED", facts.getProperty("actor"));
        assertEquals("true", facts.getProperty("jdbcClosed"));
        assertEquals("true", facts.getProperty("freshNativeSourceAnchor"));
        assertEquals("true", facts.getProperty("approvalLoaded"));
        assertTrue(Long.parseLong(facts.getProperty("sqlCommits")) > 0, facts.toString());
        assertTrue(Integer.parseInt(facts.getProperty("full20LockedReads")) >= 20, facts.toString());
        assertTrue(Integer.parseInt(facts.getProperty("sessionLockOwnerChecks")) > 0, facts.toString());
        assertTrue(Long.parseLong(facts.getProperty("connectionId")) > 0L, facts.toString());
    }

    public static final class Child {
        private Child() {}
        public static void main(String[] args) throws Exception {
            if (args.length != 2 || !"fresh".equals(args[0]))
                throw new IllegalArgumentException("exact fresh mode and root are required");
            MySqlOrdinaryCertifiedHandoffFixture$.MODULE$.runOrdinaryProcessChild(args[0], Path.of(args[1]));
        }
    }
}
