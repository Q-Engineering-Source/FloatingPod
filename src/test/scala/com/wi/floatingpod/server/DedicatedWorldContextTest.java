package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class DedicatedWorldContextTest {
    @TempDir Path temporary;
    private Path properties(String text) throws IOException {
        Path path = temporary.resolve("server.properties");
        Files.write(path, text.getBytes(StandardCharsets.ISO_8859_1));
        return path;
    }
    @Test void missingFileUsesVanillaWorldWithoutCreatingProperties() throws Exception {
        Path absent = temporary.resolve("server.properties");
        assertEquals("world", DedicatedWorldContext.selectFolder(null, absent));
        assertFalse(Files.exists(absent));
    }
    @Test void existingPropertiesSelectLevelNameWithoutRewriting() throws Exception {
        Path path = properties("level-name=actual-world\nother=value\n");
        byte[] before = Files.readAllBytes(path);
        assertEquals("actual-world", DedicatedWorldContext.selectFolder(null, path));
        assertArrayEquals(before, Files.readAllBytes(path));
    }
    @Test void actualCliFolderWinsWithoutReadingEvenInvalidPropertiesPath() throws Exception {
        assertEquals("cli-world", DedicatedWorldContext.selectFolder("cli-world", temporary));
    }
    @Test void propertiesInputStreamUsesIso88591AndUnicodeEscapes() throws Exception {
        Path path = properties("level-name=café-\\u4e16\\u754c\n");
        assertEquals("café-世界", DedicatedWorldContext.selectFolder(null, path));
    }
    @Test void missingKeyDefaultsWithoutAddingKey() throws Exception {
        Path path = properties("other=value\n");
        assertEquals("world", DedicatedWorldContext.selectFolder(null, path));
        assertEquals("other=value\n", Files.readString(path, StandardCharsets.ISO_8859_1));
    }
    @Test void explicitlyEmptyOrWhitespaceFolderFails() throws Exception {
        Path path = properties("level-name=\n");
        assertThrows(IllegalArgumentException.class, () -> DedicatedWorldContext.selectFolder(null, path));
        assertThrows(IllegalArgumentException.class, () -> DedicatedWorldContext.selectFolder("", temporary));
        assertThrows(IllegalArgumentException.class, () -> DedicatedWorldContext.selectFolder(" \t", temporary));
    }
    @Test void nonblankFolderWhitespaceIsPreserved() throws Exception {
        Path path = properties("level-name=\\ world  \n");
        assertEquals(" world  ", DedicatedWorldContext.selectFolder(null, path));
        assertEquals(" cli ", DedicatedWorldContext.selectFolder(" cli ", temporary));
    }
    @Test void malformedEscapePropagatesOriginalParseExceptionAndClosesFile() throws Exception {
        Path path = properties("level-name=\\uZZZZ\n");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> DedicatedWorldContext.selectFolder(null, path));
        assertNull(failure.getCause(), "original Properties parser exception must remain intact");
        assertTrue(failure.getStackTrace().length > 0);
        Files.delete(path);
        assertFalse(Files.exists(path));
    }
    @Test void directoryReadFailurePropagatesIOExceptionRatherThanDefault() {
        IOException failure = assertThrows(IOException.class,
                () -> DedicatedWorldContext.selectFolder(null, temporary));
        assertTrue(failure.getStackTrace().length > 0);
    }
}
