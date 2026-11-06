package com.wi.floatingpod.bootstrap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * 配置在 Forge 初始化前读取；不连接数据库、不读取密码文件，也不携带 JDBC URL。
 * Password 字面值由本对象持有至 close；调用方应在最后一次使用后关闭本对象。
 */
public final class ManagedDatabaseConfiguration implements AutoCloseable {
    public static final long DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000;
    public static final long DEFAULT_SOCKET_TIMEOUT_MILLIS = 30_000;
    public static final long DEFAULT_INITIALIZATION_TIMEOUT_MILLIS = 180_000;
    public static final long DEFAULT_LOCK_TIMEOUT_MILLIS = 5_000;

    private static final int MAX_FILE_BYTES = 64 * 1024;
    private static final long MAX_TIMEOUT_MILLIS = 1_800_000;
    private static final String RELATIVE_FILE = "config/floatingpod/database.properties";
    private static final Map<String, String> KEYS = Map.ofEntries(
        Map.entry("DBHost", "DBHost"), Map.entry("Port", "Port"),
        Map.entry("Database", "Database"), Map.entry("Username", "Username"),
        Map.entry("Password", "Password"), Map.entry("PasswordFile", "PasswordFile"),
        Map.entry("TrustStore", "TrustStore"), Map.entry("TrustStorePasswordFile", "TrustStorePasswordFile"),
        Map.entry("ConnectTimeoutMillis", "ConnectTimeoutMillis"),
        Map.entry("SocketTimeoutMillis", "SocketTimeoutMillis"),
        Map.entry("InitializationTimeoutMillis", "InitializationTimeoutMillis"),
        Map.entry("LockTimeoutMillis", "LockTimeoutMillis"), Map.entry("URLParams", "URLParams")
    );

    private final Path instanceRoot;
    private final Path configurationFile;
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final char[] password;
    private final Path passwordFile;
    private final Path trustStore;
    private final Path trustStorePasswordFile;
    private final long connectTimeoutMillis;
    private final long socketTimeoutMillis;
    private final long initializationTimeoutMillis;
    private final long lockTimeoutMillis;
    private boolean closed;

    private ManagedDatabaseConfiguration(Path instanceRoot, Path configurationFile, Parsed parsed) {
        this.instanceRoot = instanceRoot;
        this.configurationFile = configurationFile;
        this.host = parsed.host;
        this.port = parsed.port;
        this.database = parsed.database;
        this.username = parsed.username;
        this.password = parsed.password == null ? null : parsed.password.toCharArray();
        this.passwordFile = parsed.passwordFile == null ? null : resolve(instanceRoot, parsed.passwordFile);
        this.trustStore = parsed.trustStore == null ? null : resolve(instanceRoot, parsed.trustStore);
        this.trustStorePasswordFile = parsed.trustStorePasswordFile == null ? null : resolve(instanceRoot, parsed.trustStorePasswordFile);
        this.connectTimeoutMillis = parsed.connectTimeoutMillis;
        this.socketTimeoutMillis = parsed.socketTimeoutMillis;
        this.initializationTimeoutMillis = parsed.initializationTimeoutMillis;
        this.lockTimeoutMillis = parsed.lockTimeoutMillis;
    }

    /** 仅读取实例固定配置文件；相对密码文件及 trust-store 路径以实例根目录解析。 */
    public static ManagedDatabaseConfiguration load(Path instanceRoot, Path configurationFile) throws IOException {
        if (instanceRoot == null || configurationFile == null)
            throw new IllegalArgumentException("instance root and configuration path are required");
        Path root = instanceRoot.toAbsolutePath().normalize();
        Path expected = root.resolve(RELATIVE_FILE).normalize();
        Path config = configurationFile.isAbsolute()
            ? configurationFile.toAbsolutePath().normalize()
            : root.resolve(configurationFile).normalize();
        if (!config.equals(expected))
            throw new IllegalArgumentException("managed database configuration must use the instance database.properties path");

        byte[] bytes = readBounded(config);
        String text = decode(bytes);
        if (!text.isEmpty() && text.charAt(0) == '\uFEFF')
            throw invalid("UTF-8 BOM is not permitted");
        Map<String, String> values = parseLines(text);
        Parsed parsed = validate(values);
        return new ManagedDatabaseConfiguration(root, config, parsed);
    }

    private static byte[] readBounded(Path path) throws IOException {
        try (InputStream input = Files.newInputStream(path); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int total = 0;
            while (true) {
                int read = input.read(buffer, 0, Math.min(buffer.length, MAX_FILE_BYTES + 1 - total));
                if (read < 0) break;
                total += read;
                if (total > MAX_FILE_BYTES) throw invalid("configuration exceeds 64 KiB");
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    private static String decode(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException("managed database configuration is not valid UTF-8", failure);
        }
    }

    private static Map<String, String> parseLines(String text) {
        Map<String, String> values = new HashMap<>();
        String[] lines = text.split("\\R", -1);
        for (int index = 0; index < lines.length; index++) {
            String line = lines[index];
            String leading = line.stripLeading();
            if (leading.isEmpty() || leading.startsWith("#") || leading.startsWith(";")) continue;
            int equals = line.indexOf('=');
            if (equals <= 0) throw invalid("invalid key/value line " + (index + 1));
            String key = line.substring(0, equals);
            String canonical = KEYS.get(key);
            if (canonical == null) throw invalid("unknown key on line " + (index + 1));
            if (values.containsKey(canonical)) throw invalid("duplicate key on line " + (index + 1));
            String value = line.substring(equals + 1);
            // Password 等号后的内容按原样保留，包括空格；其他字段去除首尾空白。
            if (!canonical.equals("Password")) value = value.trim();
            if (canonical.equals("Password")) {
                if (value.indexOf('\0') >= 0 || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0)
                    throw invalid("invalid Password value on line " + (index + 1));
            } else if (value.isEmpty() && !canonical.equals("URLParams")) {
                throw invalid("empty value on line " + (index + 1));
            }
            values.put(canonical, value);
        }
        return values;
    }

    private static Parsed validate(Map<String, String> values) {
        String host = required(values, "DBHost");
        validateHost(host);
        int port = (int) number(required(values, "Port"), 1, 65_535, "Port");
        String database = required(values, "Database");
        if (!database.matches("[a-z][a-z0-9_]{0,63}"))
            throw invalid("Database must start with a lowercase ASCII letter and contain only lowercase ASCII letters, digits or underscores (maximum 64 characters)");
        String username = required(values, "Username");
        if (!username.matches("[A-Za-z0-9_]{1,32}")) throw invalid("Username must be a 1-32 character ASCII account name");

        boolean hasPassword = values.containsKey("Password");
        boolean hasPasswordFile = values.containsKey("PasswordFile");
        if (hasPassword == hasPasswordFile) throw invalid("exactly one of Password or PasswordFile is required");
        String password = hasPassword ? values.get("Password") : null;
        String passwordFile = hasPasswordFile ? pathText(values.get("PasswordFile"), "PasswordFile") : null;

        boolean hasTrust = values.containsKey("TrustStore");
        boolean hasTrustPassword = values.containsKey("TrustStorePasswordFile");
        if (!hasTrust || !hasTrustPassword) throw invalid("TrustStore and TrustStorePasswordFile are required until system trust is supported");
        String trustStore = pathText(values.get("TrustStore"), "TrustStore");
        String trustPassword = pathText(values.get("TrustStorePasswordFile"), "TrustStorePasswordFile");

        long connect = numberOrDefault(values, "ConnectTimeoutMillis", DEFAULT_CONNECT_TIMEOUT_MILLIS);
        long socket = numberOrDefault(values, "SocketTimeoutMillis", DEFAULT_SOCKET_TIMEOUT_MILLIS);
        long initialization = numberOrDefault(values, "InitializationTimeoutMillis", DEFAULT_INITIALIZATION_TIMEOUT_MILLIS);
        long lock = numberOrDefault(values, "LockTimeoutMillis", DEFAULT_LOCK_TIMEOUT_MILLIS);
        if (lock > initialization) throw invalid("LockTimeoutMillis must not exceed InitializationTimeoutMillis");
        String urlParams = values.getOrDefault("URLParams", "");
        long[] mapped = parseUrlParams(urlParams);
        if (mapped[0] > 0) {
            if (values.containsKey("ConnectTimeoutMillis")) throw invalid("connectTimeout conflicts with ConnectTimeoutMillis");
            connect = mapped[0];
        }
        if (mapped[1] > 0) {
            if (values.containsKey("SocketTimeoutMillis")) throw invalid("socketTimeout conflicts with SocketTimeoutMillis");
            socket = mapped[1];
        }
        // 与正常 schema6 metadata 的闭合边界一致，必须在任何建表副作用之前拒绝。
        if (connect > 30_000) throw invalid("ConnectTimeoutMillis exceeds the schema6 maximum of 30000");
        if (socket > 300_000) throw invalid("SocketTimeoutMillis exceeds the schema6 maximum of 300000");
        if (lock > initialization) throw invalid("LockTimeoutMillis must not exceed InitializationTimeoutMillis");

        return new Parsed(host, port, database, username, password, passwordFile,
            trustStore, trustPassword, connect, socket, initialization, lock);
    }

    private static long[] parseUrlParams(String input) {
        long connect = -1, socket = -1;
        if (input.isEmpty()) return new long[]{connect, socket};
        Map<String, String> seen = new HashMap<>();
        for (String pair : input.split("&", -1)) {
            int equals = pair.indexOf('=');
            if (equals <= 0 || equals != pair.lastIndexOf('=')) throw invalid("URLParams contains malformed pair");
            String key = pair.substring(0, equals);
            String value = pair.substring(equals + 1);
            if (value.isEmpty() || seen.putIfAbsent(key, value) != null) throw invalid("URLParams contains empty or duplicate parameter");
            switch (key) {
                case "connectTimeout" -> connect = number(value, 1, MAX_TIMEOUT_MILLIS, "URLParams connectTimeout");
                case "socketTimeout" -> socket = number(value, 1, MAX_TIMEOUT_MILLIS, "URLParams socketTimeout");
                case "sslMode" -> requireExact(value, "VERIFY_IDENTITY", "URLParams sslMode");
                case "allowMultiQueries" -> requireExact(value, "false", "URLParams allowMultiQueries");
                case "useSSL", "requireSSL", "verifyServerCertificate" -> requireExact(value, "true", "URLParams " + key);
                default -> throw invalid("URLParams contains an unsupported parameter");
            }
        }
        return new long[]{connect, socket};
    }

    private static void requireExact(String actual, String expected, String field) {
        if (!actual.equals(expected)) throw invalid(field + " must be " + expected);
    }

    private static void validateHost(String host) {
        if (host.isBlank()) throw invalid("DBHost must not be blank");
        for (int offset = 0; offset < host.length();) {
            int codePoint = host.codePointAt(offset);
            if (Character.isWhitespace(codePoint) || Character.isISOControl(codePoint) || "/\\?#@".indexOf(codePoint) >= 0)
                throw invalid("DBHost contains an invalid character");
            offset += Character.charCount(codePoint);
        }
    }

    private static String pathText(String value, String field) {
        if (value.isBlank()) throw invalid(field + " must not be blank");
        return value;
    }

    private static Path resolve(Path root, String value) {
        Path path = Path.of(value);
        return (path.isAbsolute() ? path : root.resolve(path)).toAbsolutePath().normalize();
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) throw invalid(key + " is required");
        return value;
    }

    private static long numberOrDefault(Map<String, String> values, String key, long fallback) {
        return values.containsKey(key) ? number(values.get(key), 1, MAX_TIMEOUT_MILLIS, key) : fallback;
    }

    private static long number(String text, long minimum, long maximum, String field) {
        try {
            if (!text.matches("[0-9]+")) throw new NumberFormatException();
            long value = Long.parseLong(text);
            if (value < minimum || value > maximum) throw new NumberFormatException();
            return value;
        } catch (NumberFormatException failure) {
            throw invalid(field + " is outside the supported range");
        }
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("Invalid managed database configuration: " + message);
    }

    public Path instanceRoot() { return instanceRoot; }
    public Path configurationFile() { return configurationFile; }
    public String host() { return host; }
    public int port() { return port; }
    public String database() { return database; }
    public String username() { return username; }
    public Optional<Path> passwordFile() { return Optional.ofNullable(passwordFile); }
    public Optional<Path> trustStore() { return Optional.ofNullable(trustStore); }
    public Optional<Path> trustStorePasswordFile() { return Optional.ofNullable(trustStorePasswordFile); }
    public long connectTimeoutMillis() { return connectTimeoutMillis; }
    public long socketTimeoutMillis() { return socketTimeoutMillis; }
    public long initializationTimeoutMillis() { return initializationTimeoutMillis; }
    public long lockTimeoutMillis() { return lockTimeoutMillis; }

    /**
     * 通过临时防御性副本访问字面密码，并在回调结束后清零。
     * 回调不得保存、返回或异步保留数组；PasswordFile 模式不得调用此方法。
     */
    public synchronized <T> T withPassword(Function<char[], T> consumer) {
        if (closed) throw new IllegalStateException("managed database configuration is closed");
        if (consumer == null) throw new IllegalArgumentException("password consumer is required");
        if (password == null) throw new IllegalStateException("password is supplied by file");
        char[] copy = Arrays.copyOf(password, password.length);
        try { return consumer.apply(copy); }
        finally { Arrays.fill(copy, '\0'); }
    }

    public boolean usesPasswordFile() { return passwordFile != null; }

    @Override public synchronized void close() {
        if (!closed) {
            if (password != null) Arrays.fill(password, '\0');
            closed = true;
        }
    }

    @Override public String toString() {
        return "ManagedDatabaseConfiguration[connection=redacted, credentials=redacted, tls=" +
            (trustStore == null ? "trust-store-unspecified" : "pkcs12") +
            ", timeouts=" + connectTimeoutMillis + "/" + socketTimeoutMillis + "/" +
            initializationTimeoutMillis + "/" + lockTimeoutMillis + "ms]";
    }

    private static final class Parsed {
        final String host;
        final int port;
        final String database;
        final String username;
        final String password;
        final String passwordFile;
        final String trustStore;
        final String trustStorePasswordFile;
        final long connectTimeoutMillis;
        final long socketTimeoutMillis;
        final long initializationTimeoutMillis;
        final long lockTimeoutMillis;

        Parsed(String host, int port, String database, String username, String password,
               String passwordFile, String trustStore, String trustStorePasswordFile,
               long connectTimeoutMillis, long socketTimeoutMillis,
               long initializationTimeoutMillis, long lockTimeoutMillis) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.username = username;
            this.password = password;
            this.passwordFile = passwordFile;
            this.trustStore = trustStore;
            this.trustStorePasswordFile = trustStorePasswordFile;
            this.connectTimeoutMillis = connectTimeoutMillis;
            this.socketTimeoutMillis = socketTimeoutMillis;
            this.initializationTimeoutMillis = initializationTimeoutMillis;
            this.lockTimeoutMillis = lockTimeoutMillis;
        }
    }
}
