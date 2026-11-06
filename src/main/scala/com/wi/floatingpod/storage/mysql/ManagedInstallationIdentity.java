package com.wi.floatingpod.storage.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 稳定且非秘密的安装身份 DATA，不是已批准 baseline、writer、runtime qualification 或 Ready 签发者。
 * 调用方必须先创建或读取此记录，再打开 SQL 副作用日志。
 */
public final class ManagedInstallationIdentity {
    public static final String FILE_NAME = "installation.identity";
    public static final int MAX_BYTES = 8 * 1024;
    private static final String FORMAT = "FP_MANAGED_INSTALLATION_IDENTITY_V1";
    private static final Pattern SCHEMA = Pattern.compile("[A-Za-z0-9_]{1,64}");
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final List<String> ORDER = List.of("installationUuid", "serverUuid", "bindingUuid", "provisioningUuid",
            "creationOperationUuid", "bootstrapOperationUuid", "prepareWriteUuid", "certifyWriteUuid",
            "schema", "configurationSha256", "recipeSha256");

    public record Record(UUID installationUuid, UUID serverUuid, UUID bindingUuid, UUID provisioningUuid,
                         UUID creationOperationUuid, UUID bootstrapOperationUuid,
                         UUID prepareWriteUuid, UUID certifyWriteUuid, String schema,
                         String configurationSha256, String recipeSha256) {
        public Record {
            List<UUID> ids = List.of(installationUuid, serverUuid, bindingUuid, provisioningUuid,
                    creationOperationUuid, bootstrapOperationUuid, prepareWriteUuid, certifyWriteUuid);
            if (ids.stream().anyMatch(Objects::isNull) || ids.stream().anyMatch(id -> id.equals(new UUID(0L, 0L)))
                    || ids.stream().distinct().count() != ids.size())
                throw new IllegalArgumentException("managed installation UUID identities must be nonzero and distinct");
            if (schema == null || !SCHEMA.matcher(schema).matches())
                throw new IllegalArgumentException("managed installation schema identifier is invalid");
            if (configurationSha256 == null || !SHA256.matcher(configurationSha256).matches()
                    || recipeSha256 == null || !SHA256.matcher(recipeSha256).matches())
                throw new IllegalArgumentException("managed installation digests must be lowercase SHA-256");
        }

        @Override public String toString() { return "ManagedInstallationIdentity.Record[non-secret DATA; no qualification]"; }
    }

    private ManagedInstallationIdentity() { }

    /** 重启先读受控本地身份；不为了发现既有 server UUID 再登录 installer。 */
    public static java.util.Optional<Record> readExisting(ManagedInstallationFiles files) throws IOException {
        Objects.requireNonNull(files, "managed installation files");
        Path path = files.directory().resolve(FILE_NAME);
        BasicFileAttributes before = attributesIfPresent(path);
        if (before == null) {
            if (attributesIfPresent(files.directory().resolve(ManagedInstallationJournal.FILE_NAME)) != null)
                throw new IOException("managed pending journal exists without its installation identity");
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(read(files));
    }

    /**
     * 读取精确的持久化身份；仅在身份与 pending 日志均不存在时才创建新身份。
     * 即使预期配置已漂移，也不重写既有字节。
     */
    public static Record openOrCreate(ManagedInstallationFiles files, UUID observedServerUuid, String schema,
                                      String expectedNonsecretConfigurationSha256,
                                      String expectedRecipeSha256) throws IOException {
        Objects.requireNonNull(files, "managed installation files");
        validateExpected(observedServerUuid, schema, expectedNonsecretConfigurationSha256, expectedRecipeSha256);
        Path directory = files.directory();
        if (directory == null || !directory.isAbsolute()) throw new IllegalArgumentException("managed directory must be absolute");
        Path identityPath = directory.resolve(FILE_NAME);
        Path journalPath = directory.resolve(ManagedInstallationJournal.FILE_NAME);
        BasicFileAttributes identityAttributes = attributesIfPresent(identityPath);
        BasicFileAttributes journalAttributes = attributesIfPresent(journalPath);

        if (identityAttributes != null) {
            Record actual = read(files);
            requireMatches(actual, observedServerUuid, schema, expectedNonsecretConfigurationSha256, expectedRecipeSha256);
            return actual;
        }
        if (journalAttributes != null)
            throw new IOException("managed pending journal exists without its stable installation identity; automatic reissuance is forbidden");

        Record created = createFresh(observedServerUuid, schema, expectedNonsecretConfigurationSha256,
                expectedRecipeSha256, new java.security.SecureRandom());
        byte[] encoded = encode(created);
        try {
            // 在 CREATE_NEW 前重查；竞争中的合规初始化器只能赢得该文件名或使本次调用失败。
            // 不承接其他身份，也不覆盖既有字节。
            if (attributesIfPresent(identityPath) != null)
                throw new IOException("managed identity appeared during first creation; reopen to inspect the winner");
            if (attributesIfPresent(journalPath) != null)
                throw new IOException("managed pending journal appeared before identity creation; refusing to mint a new identity");
            files.publishNewFile(FILE_NAME, encoded);
        } catch (java.nio.file.FileAlreadyExistsException raced) {
            throw new IOException("managed identity was concurrently created; reopen and compare the persisted identity", raced);
        } finally { Arrays.fill(encoded, (byte) 0); }

        BasicFileAttributes persisted = attributesIfPresent(identityPath);
        if (persisted == null) throw new IOException("managed identity creation returned without a persisted file");
        Record readback = read(files);
        if (!readback.equals(created)) throw new IOException("managed identity force/readback differs from the generated record");
        return readback;
    }

    private static void validateExpected(UUID server, String schema, String configurationSha256, String recipeSha256) {
        if (server == null || server.equals(new UUID(0L, 0L))) throw new IllegalArgumentException("observed nonzero MySQL server UUID is required");
        if (schema == null || !SCHEMA.matcher(schema).matches()) throw new IllegalArgumentException("existing target schema identifier is invalid");
        if (configurationSha256 == null || !SHA256.matcher(configurationSha256).matches()
                || recipeSha256 == null || !SHA256.matcher(recipeSha256).matches())
            throw new IllegalArgumentException("configuration and recipe must have canonical lowercase SHA-256");
    }

    private static Record createFresh(UUID server, String schema, String configurationSha256,
                                      String recipeSha256, java.security.SecureRandom random) {
        List<UUID> ids = new ArrayList<>(7);
        ids.add(server);
        while (ids.size() < 8) {
            byte[] raw = new byte[16];
            try {
                random.nextBytes(raw);
                raw[6] = (byte) ((raw[6] & 0x0f) | 0x40); // RFC 4122 version 4
                raw[8] = (byte) ((raw[8] & 0x3f) | 0x80);
                ByteBuffer buffer = ByteBuffer.wrap(raw);
                UUID candidate = new UUID(buffer.getLong(), buffer.getLong());
                if (!candidate.equals(new UUID(0L, 0L)) && !ids.contains(candidate)) ids.add(candidate);
            } finally { Arrays.fill(raw, (byte) 0); }
        }
        return new Record(ids.get(1), ids.get(0), ids.get(2), ids.get(3), ids.get(4), ids.get(5), ids.get(6), ids.get(7),
                schema, configurationSha256, recipeSha256);
    }

    private static void requireMatches(Record actual, UUID server, String schema, String configurationSha256,
                                       String recipeSha256) throws IOException {
        if (!actual.serverUuid().equals(server) || !actual.schema().equals(schema)
                || !actual.configurationSha256().equals(configurationSha256)
                || !actual.recipeSha256().equals(recipeSha256))
            throw new IOException("persisted managed installation identity does not match the observed server/schema/configuration/recipe");
    }

    private static BasicFileAttributes attributesIfPresent(Path path) throws IOException {
        try {
            BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (attributes.isSymbolicLink() || !attributes.isRegularFile())
                throw new IOException("managed identity or pending journal must be a regular non-link file");
            if (attributes.size() > MAX_BYTES && path.getFileName().toString().equals(FILE_NAME))
                throw new IOException("managed identity exceeds its fixed byte bound");
            return attributes;
        } catch (java.nio.file.NoSuchFileException absent) { return null; }
    }

    private static Record read(ManagedInstallationFiles files) throws IOException {
        byte[] bytes = files.readPrivateFile(FILE_NAME, MAX_BYTES);
        try { return decode(bytes); }
        finally { Arrays.fill(bytes, (byte) 0); }
    }

    private static byte[] encode(Record record) throws IOException {
        StringBuilder body = new StringBuilder(512).append(FORMAT).append('\n');
        Map<String, String> values = values(record);
        for (String key : ORDER) body.append(key).append('=').append(values.get(key)).append('\n');
        byte[] canonicalBody = body.toString().getBytes(StandardCharsets.US_ASCII);
        String checksum = HexFormat.of().formatHex(sha256(canonicalBody));
        body.append("recordSha256=").append(checksum).append('\n');
        byte[] encoded = body.toString().getBytes(StandardCharsets.US_ASCII);
        Arrays.fill(canonicalBody, (byte) 0);
        if (encoded.length > MAX_BYTES) {
            Arrays.fill(encoded, (byte) 0);
            throw new IOException("managed identity canonical encoding exceeds its fixed byte bound");
        }
        return encoded;
    }

    private static Record decode(byte[] bytes) throws IOException {
        if (bytes.length > MAX_BYTES) throw new IOException("managed identity exceeds its fixed byte bound");
        if (bytes.length >= 3 && bytes[0] == (byte) 0xef && bytes[1] == (byte) 0xbb && bytes[2] == (byte) 0xbf)
            throw new IOException("managed identity UTF-8 BOM is forbidden");
        String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw new IOException("managed identity is not strict UTF-8", malformed);
        }
        if (!text.endsWith("\n") || text.indexOf('\r') >= 0) throw new IOException("managed identity newline framing is invalid");
        String[] lines = text.split("\n", -1);
        if (lines.length != ORDER.size() + 3 || !FORMAT.equals(lines[0]) || !lines[lines.length - 1].isEmpty())
            throw new IOException("managed identity field framing or version is invalid");
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        StringBuilder body = new StringBuilder().append(FORMAT).append('\n');
        for (int i = 0; i < ORDER.size(); i++) {
            String expected = ORDER.get(i);
            String line = lines[i + 1];
            int equals = line.indexOf('=');
            if (equals <= 0 || line.indexOf('=', equals + 1) >= 0 || !line.substring(0, equals).equals(expected))
                throw new IOException("managed identity contains an unknown, duplicate, or misordered field");
            String value = line.substring(equals + 1);
            if (value.isEmpty() || values.putIfAbsent(expected, value) != null)
                throw new IOException("managed identity contains an empty or duplicate field");
            body.append(expected).append('=').append(value).append('\n');
        }
        String checksumLine = lines[ORDER.size() + 1];
        if (!checksumLine.matches("recordSha256=[0-9a-f]{64}")) throw new IOException("managed identity checksum field is invalid");
        byte[] canonicalBody = body.toString().getBytes(StandardCharsets.US_ASCII);
        try {
            String actual = HexFormat.of().formatHex(sha256(canonicalBody));
            if (!MessageDigest.isEqual(actual.getBytes(StandardCharsets.US_ASCII),
                    checksumLine.substring("recordSha256=".length()).getBytes(StandardCharsets.US_ASCII)))
                throw new IOException("managed identity checksum is invalid");
        } finally { Arrays.fill(canonicalBody, (byte) 0); }
        try {
            Record record = new Record(uuid(values, "installationUuid"), uuid(values, "serverUuid"), uuid(values, "bindingUuid"),
                    uuid(values, "provisioningUuid"), uuid(values, "creationOperationUuid"), uuid(values, "bootstrapOperationUuid"),
                    uuid(values, "prepareWriteUuid"), uuid(values, "certifyWriteUuid"), values.get("schema"),
                    values.get("configurationSha256"), values.get("recipeSha256"));
            if (!Arrays.equals(bytes, encode(record))) throw new IOException("managed identity is not in canonical encoding");
            return record;
        } catch (IllegalArgumentException malformed) {
            throw new IOException("managed identity values are invalid", malformed);
        }
    }

    private static UUID uuid(Map<String, String> values, String name) {
        String raw = values.get(name);
        UUID parsed = UUID.fromString(raw);
        if (!parsed.toString().equals(raw)) throw new IllegalArgumentException("managed identity UUID is not canonical");
        return parsed;
    }

    private static Map<String, String> values(Record record) {
        var values = new LinkedHashMap<String, String>();
        values.put("installationUuid", record.installationUuid().toString());
        values.put("serverUuid", record.serverUuid().toString());
        values.put("bindingUuid", record.bindingUuid().toString());
        values.put("provisioningUuid", record.provisioningUuid().toString());
        values.put("creationOperationUuid", record.creationOperationUuid().toString());
        values.put("bootstrapOperationUuid", record.bootstrapOperationUuid().toString());
        values.put("prepareWriteUuid", record.prepareWriteUuid().toString());
        values.put("certifyWriteUuid", record.certifyWriteUuid().toString());
        values.put("schema", record.schema());
        values.put("configurationSha256", record.configurationSha256());
        values.put("recipeSha256", record.recipeSha256());
        return values;
    }

    private static byte[] sha256(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError("SHA-256 must be available", impossible); }
    }
}
