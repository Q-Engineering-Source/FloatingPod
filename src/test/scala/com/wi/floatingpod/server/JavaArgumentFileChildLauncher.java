package com.wi.floatingpod.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** 测试侧 JDK 25 子进程启动器：完整 classpath 和子进程参数写入 @argument 文件。 */
final class JavaArgumentFileChildLauncher {
    private static final long MAX_ARGUMENT_FILE_BYTES = 2_147_483_647L;

    private JavaArgumentFileChildLauncher() { }

    static StartedChild start(Path javaExecutable, String exactClasspath, String mainClass,
                              List<String> programArguments, Path argumentFileDirectory) throws IOException {
        Objects.requireNonNull(javaExecutable, "javaExecutable");
        Objects.requireNonNull(exactClasspath, "exactClasspath");
        Objects.requireNonNull(mainClass, "mainClass");
        Objects.requireNonNull(programArguments, "programArguments");
        Objects.requireNonNull(argumentFileDirectory, "argumentFileDirectory");
        if (!javaExecutable.isAbsolute() || !Files.isRegularFile(javaExecutable)) {
            throw new IOException("Reviewed Java executable must be an existing absolute file");
        }
        if (exactClasspath.isBlank()) throw new IOException("Reviewed child classpath is empty");
        if (!argumentFileDirectory.isAbsolute() || !Files.isDirectory(argumentFileDirectory)) {
            throw new IOException("Argument-file directory must be an existing absolute directory");
        }

        var arguments = new ArrayList<String>(programArguments.size() + 4);
        arguments.add("--enable-native-access=ALL-UNNAMED");
        arguments.add("-cp");
        arguments.add(exactClasspath);
        arguments.add(mainClass);
        for (String argument : programArguments) arguments.add(Objects.requireNonNull(argument, "program argument"));
        byte[] content = encodeArgumentFile(arguments);
        if ((long) content.length > MAX_ARGUMENT_FILE_BYTES) {
            throw new IOException("JDK argument file exceeds the Java launcher maximum size");
        }

        Path argumentFile = Files.createTempFile(argumentFileDirectory, "fp-child-", ".args");
        Process process = null;
        try {
            Files.write(argumentFile, content, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            String argumentFileSha256 = sha256(content);
            String classpathSha256 = sha256(exactClasspath.getBytes(StandardCharsets.UTF_8));
            String argumentFileArgument = "@" + argumentFile.toAbsolutePath();
            process = new ProcessBuilder(javaExecutable.toString(), argumentFileArgument)
                .redirectErrorStream(true)
                .start();
            return new StartedChild(process, argumentFile, content.length, argumentFileSha256, classpathSha256);
        } catch (IOException | RuntimeException | Error primary) {
            boolean processStopped = process == null || stopOwnedProcessAfterLaunchFailure(process, primary);
            if (processStopped) {
                try {
                    Files.deleteIfExists(argumentFile);
                } catch (IOException | RuntimeException | Error cleanup) {
                    primary.addSuppressed(cleanup);
                }
            } else {
                primary.addSuppressed(new IOException("Retaining Java argument file because child termination was not confirmed: " + argumentFile));
            }
            throw primary;
        }
    }

    private static boolean stopOwnedProcessAfterLaunchFailure(Process process, Throwable primary) {
        try {
            if (process.isAlive()) {
                process.destroyForcibly();
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    primary.addSuppressed(new IOException("Child remained alive after launch-wrapper failure cleanup"));
                    return false;
                }
            }
            return !process.isAlive();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            primary.addSuppressed(interrupted);
            return false;
        } catch (RuntimeException | Error cleanup) {
            primary.addSuppressed(cleanup);
            return false;
        }
    }

    private static byte[] encodeArgumentFile(List<String> arguments) throws IOException {
        var text = new StringBuilder();
        long encodedBytes = 0;
        for (String argument : arguments) {
            String encoded = quoteArgument(Objects.requireNonNull(argument, "argument")) + "\n";
            int bytes = encoded.getBytes(StandardCharsets.UTF_8).length;
            try {
                encodedBytes = Math.addExact(encodedBytes, bytes);
            } catch (ArithmeticException overflow) {
                throw new IOException("JDK argument file length overflow", overflow);
            }
            if (encodedBytes > MAX_ARGUMENT_FILE_BYTES) {
                throw new IOException("JDK argument file exceeds the Java launcher maximum size");
            }
            text.append(encoded);
        }
        byte[] bytes = text.toString().getBytes(StandardCharsets.UTF_8);
        if ((long) bytes.length != encodedBytes) throw new IOException("Argument-file UTF-8 length changed during encoding");
        return bytes;
    }

    private static String quoteArgument(String argument) throws IOException {
        if (argument.indexOf('\0') >= 0) throw new IOException("Java argument files cannot represent NUL arguments");
        var quoted = new StringBuilder(argument.length() + 2);
        quoted.append('"');
        for (int i = 0; i < argument.length(); i++) {
            char value = argument.charAt(i);
            switch (value) {
                case '\\' -> quoted.append("\\\\");
                case '"' -> quoted.append("\\\"");
                case '\n' -> quoted.append("\\n");
                case '\r' -> quoted.append("\\r");
                case '\t' -> quoted.append("\\t");
                case '\f' -> quoted.append("\\f");
                default -> {
                    if (Character.isISOControl(value)) throw new IOException("Unsupported control character in Java argument");
                    quoted.append(value);
                }
            }
        }
        return quoted.append('"').toString();
    }

    private static String sha256(byte[] value) throws IOException {
        try {
            return HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IOException("SHA-256 is unavailable", impossible);
        }
    }

    static final class StartedChild {
        private final Process process;
        private final Path argumentFile;
        private final long argumentFileBytes;
        private final String argumentFileSha256;
        private final String classpathSha256;
        private boolean argumentFileDeleted;

        private StartedChild(Process process, Path argumentFile, long argumentFileBytes,
                             String argumentFileSha256, String classpathSha256) {
            this.process = process;
            this.argumentFile = argumentFile;
            this.argumentFileBytes = argumentFileBytes;
            this.argumentFileSha256 = argumentFileSha256;
            this.classpathSha256 = classpathSha256;
        }

        Process process() { return process; }
        long argumentFileBytes() { return argumentFileBytes; }
        String argumentFileSha256() { return argumentFileSha256; }
        String classpathSha256() { return classpathSha256; }

        synchronized void deleteArgumentFileAfterExit() throws IOException {
            if (argumentFileDeleted) return;
            if (process.isAlive()) throw new IOException("Cannot delete Java argument file while its child is alive: " + argumentFile);
            Files.deleteIfExists(argumentFile);
            argumentFileDeleted = true;
        }
    }
}