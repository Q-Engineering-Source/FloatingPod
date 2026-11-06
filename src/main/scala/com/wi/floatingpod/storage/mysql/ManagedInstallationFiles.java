package com.wi.floatingpod.storage.mysql;

import com.sun.nio.file.ExtendedOpenOption;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryFlag;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 私有且不可覆盖的安装文件。秘密文件只保存生成的 runtime 凭据，不读取或保存管理员的 installer 密码。
 * 调用方必须先通过 ManagedInstallationJournal 持久化安装身份与计划，才能将返回的凭据路径用于 SQL。
 *
 * 不扩大父目录链的权限；只创建专用的 config/floatingpod/managed 目录。
 * 既有 managed 目录必须已经满足精确的私有权限要求。
 */
public final class ManagedInstallationFiles {
    public static final int MAX_BUNDLE_BYTES = 16 * 1024 * 1024;
    private static final int MAX_SECRET_BYTES = 4096;
    private static final Pattern ACCOUNT = Pattern.compile("[A-Za-z0-9_]{1,32}");
    private static final Pattern FILE_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    private static final Set<PosixFilePermission> PRIVATE_DIRECTORY = Set.of(
            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE);
    private static final Set<PosixFilePermission> PRIVATE_FILE = Set.of(
            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);

    private enum Platform { POSIX, WINDOWS_ACL }

    private final Path managedDirectory;
    private final Platform platform;
    private final List<UserPrincipal> windowsPrincipals;

    private ManagedInstallationFiles(Path managedDirectory, Platform platform, List<UserPrincipal> principals) {
        this.managedDirectory = managedDirectory;
        this.platform = platform;
        this.windowsPrincipals = principals;
    }

    /** 打开或创建固定 managed 目录，并验证其最终私有访问策略。 */
    public static ManagedInstallationFiles open(Path instanceRoot) throws IOException {
        Objects.requireNonNull(instanceRoot, "instanceRoot");
        if (!instanceRoot.isAbsolute()) throw new IllegalArgumentException("instance root must be absolute");
        Path root = instanceRoot.normalize();
        requireDirectory(root, "instance root");
        Path config = root.resolve("config");
        Path floatingpod = config.resolve("floatingpod");
        ensurePlainDirectory(config);
        ensurePlainDirectory(floatingpod);
        Path directory = floatingpod.resolve("managed");

        Platform platform;
        List<UserPrincipal> principals = List.of();
        if (Files.getFileAttributeView(root, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null) {
            platform = Platform.POSIX;
            createOrVerifyPosixDirectory(directory);
        } else if (Files.getFileAttributeView(root, AclFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null) {
            platform = Platform.WINDOWS_ACL;
            principals = windowsPrincipals(directory);
            createOrVerifyWindowsDirectory(directory, principals);
        } else {
            throw new IOException("managed installation storage requires verifiable POSIX permissions or filesystem ACLs");
        }
        return new ManagedInstallationFiles(directory, platform, principals);
    }

    /**
     * 返回由 32 字节密码学随机材料生成的 runtime 密码路径。首用时独占创建私有的单行 base64url
     * 文件并 force/readback。既有文件验证后原样复用，不旋转、覆盖、输出日志或将秘密摘要写入外部清单。
     */
    public synchronized Path ensureRuntimePassword(String managedAccount) throws IOException {
        if (managedAccount == null || !ACCOUNT.matcher(managedAccount).matches())
            throw new IllegalArgumentException("managed runtime account identifier is invalid");
        Path path = managedDirectory.resolve("runtime-" + managedAccount + ".secret");
        BasicFileAttributes existing = readAttributesIfPresent(path);
        if (existing != null) {
            requireRegularNoLink(existing, "existing runtime secret");
            requireStableFileKey(existing, "existing runtime secret");
            verifyPrivateFile(path);
            validateSecretFile(path, existing);
            return path;
        }

        FileChannel channel;
        try {
            channel = createNew(path, privateFileAttributes());
        } catch (FileAlreadyExistsException raced) {
            throw new IOException("runtime secret appeared during exclusive creation; inspect the existing installation state", raced);
        }

        Throwable primary = null;
        byte[] random = new byte[32];
        byte[] encoded = null;
        byte[] payload = null;
        try {
            verifyPrivateFile(path);
            BasicFileAttributes created = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!created.isRegularFile() || created.isSymbolicLink() || created.size() != 0L)
                throw new IOException("new runtime secret path changed before its first write");
            requireStableFileKey(created, "new runtime secret");
            // 避免随机缺少字符类别而被 MySQL 默认 MEDIUM 策略拒绝；不修改服务端策略或既有密钥。
            // 每个候选仍来自 32 字节 SecureRandom，保留原 base64url 文件格式。
            var randomSource = new SecureRandom();
            for (int attempt = 0; attempt < 64; attempt++) {
                randomSource.nextBytes(random);
                encoded = java.util.Base64.getUrlEncoder().withoutPadding().encode(random);
                boolean upper = false, lower = false, digit = false, special = false;
                for (byte value : encoded) {
                    upper |= value >= 'A' && value <= 'Z';
                    lower |= value >= 'a' && value <= 'z';
                    digit |= value >= '0' && value <= '9';
                    special |= value == '-' || value == '_';
                }
                if (upper && lower && digit && special) break;
                Arrays.fill(encoded, (byte) 0);
                encoded = null;
            }
            if (encoded == null) throw new IOException("runtime password generation did not meet its bounded character policy");
            payload = Arrays.copyOf(encoded, encoded.length + 1);
            payload[payload.length - 1] = '\n';
            writeFully(channel, payload);
            channel.force(true);
            verifySameFile(path, created, payload.length);
            byte[] readback = readExact(channel, payload.length);
            try {
                if (!Arrays.equals(payload, readback)) throw new IOException("runtime secret file readback differs");
                validateSecretBytes(readback);
            } finally { Arrays.fill(readback, (byte) 0); }
            return path;
        } catch (Throwable failure) {
            primary = failure;
            throw rethrow(failure);
        } finally {
            Arrays.fill(random, (byte) 0);
            if (encoded != null) Arrays.fill(encoded, (byte) 0);
            if (payload != null) Arrays.fill(payload, (byte) 0);
            closeOwned(channel, primary);
        }
    }

    /**
     * 按规范名称创建不可覆盖的非秘密载荷；写入前用 CREATE_NEW 保留最终文件名。
     * 只有本方法成功返回后才可发布 active 指针。中断留下的不完整文件保留供明确检查，不直接复用。
     */
    public synchronized Path publishNewFile(String fileName, byte[] content) throws IOException {
        if (fileName == null || !FILE_NAME.matcher(fileName).matches())
            throw new IllegalArgumentException("managed installation filename is invalid");
        if (content == null || content.length == 0 || content.length > MAX_BUNDLE_BYTES)
            throw new IllegalArgumentException("managed installation bundle is outside its fixed byte bound");
        Path path = managedDirectory.resolve(fileName);
        byte[] expected = content.clone();
        FileChannel channel = null;
        Throwable primary = null;
        try {
            channel = createNew(path, privateFileAttributes());
            verifyPrivateFile(path);
            BasicFileAttributes created = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!created.isRegularFile() || created.isSymbolicLink() || created.size() != 0L)
                throw new IOException("new managed bundle path changed before its first write");
            requireStableFileKey(created, "new managed bundle");
            writeFully(channel, expected);
            channel.force(true);
            verifySameFile(path, created, expected.length);
            byte[] readback = readExact(channel, expected.length);
            try {
                if (!Arrays.equals(expected, readback)) throw new IOException("managed bundle readback differs");
            } finally { Arrays.fill(readback, (byte) 0); }
            return path;
        } catch (Throwable failure) {
            primary = failure;
            throw rethrow(failure);
        } finally {
            Arrays.fill(expected, (byte) 0);
            if (channel != null) closeOwned(channel, primary);
        }
    }

    public Path directory() { return managedDirectory; }

    /** 读取固定受控文件；返回字节由调用者的预算/持有责任覆盖，绝不跟随路径链接。 */
    public synchronized byte[] readPrivateFile(String fileName, int maximum) throws IOException {
        if (fileName == null || !FILE_NAME.matcher(fileName).matches() || maximum <= 0 || maximum > MAX_BUNDLE_BYTES)
            throw new IllegalArgumentException("bounded canonical managed filename required");
        Path path = managedDirectory.resolve(fileName);
        try (FileChannel input = openProtectedRead(path)) {
            verifyPrivateFile(path);
            BasicFileAttributes before = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            requireStableFileKey(before, "managed immutable file");
            if (before.size() <= 0 || before.size() > maximum)
                throw new IOException("managed immutable file exceeds its byte bound");
            if (input.size() != before.size()) throw new IOException("managed immutable file changed before read");
            byte[] content = readExact(input, Math.toIntExact(before.size()));
            try {
                verifyPrivateFile(path);
                verifySameFile(path, before, content.length);
                BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (!before.lastModifiedTime().equals(after.lastModifiedTime()))
                    throw new IOException("managed immutable file changed during read");
                return content;
            } catch (Throwable failure) {
                Arrays.fill(content, (byte) 0);
                throw rethrow(failure);
            }
        }
    }

    /**
     * 将已完整 force/readback 的私有文件以同目录硬链接原子发布。createLink 不覆盖已有名字；
     * 不支持此本机文件协议时明确失败。这里不宣称经过了硬件掉电或服务端终止验收。
     */
    public synchronized Path publishAtomicLink(String existingPrivateName, String activeName) throws IOException {
        if (existingPrivateName == null || activeName == null || existingPrivateName.equals(activeName) ||
                !FILE_NAME.matcher(existingPrivateName).matches() || !FILE_NAME.matcher(activeName).matches())
            throw new IllegalArgumentException("distinct canonical managed publication filenames required");
        Path source = managedDirectory.resolve(existingPrivateName);
        Path target = managedDirectory.resolve(activeName);
        try (FileChannel guard = openProtectedRead(source)) {
            verifyPrivateFile(source);
            BasicFileAttributes before = Files.readAttributes(source, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            requireStableFileKey(before, "managed publication source");
            if (before.size() <= 0 || before.size() > MAX_BUNDLE_BYTES || guard.size() != before.size())
                throw new IOException("managed publication source is outside its byte bound");
            try { Files.createLink(target, source); }
            catch (UnsupportedOperationException failure) {
                throw new IOException("managed installation storage does not support atomic no-replace hard-link publication", failure);
            }
            verifyPrivateFile(target);
            BasicFileAttributes published = Files.readAttributes(target, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!Files.isSameFile(source, target) || before.size() != published.size())
                throw new IOException("managed publication does not reference the verified private source");
            verifySameFile(source, before, Math.toIntExact(before.size()));
            return target;
        }
    }

    @Override public String toString() { return "ManagedInstallationFiles[private directory; secret values omitted]"; }

    private FileAttribute<?>[] privateFileAttributes() throws IOException {
        return switch (platform) {
            case POSIX -> new FileAttribute<?>[]{PosixFilePermissions.asFileAttribute(PRIVATE_FILE)};
            case WINDOWS_ACL -> new FileAttribute<?>[]{aclAttribute(windowsFileAcl(windowsPrincipals))};
        };
    }

    private void verifyPrivateFile(Path path) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        requireRegularNoLink(attributes, "managed private file");
        if (platform == Platform.POSIX) {
            var view = Files.getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            if (view == null || !view.readAttributes().permissions().equals(PRIVATE_FILE))
                throw new IOException("managed file permissions are not exactly owner read/write");
            if (!Files.getOwner(path, LinkOption.NOFOLLOW_LINKS).equals(currentProcessPrincipal(path)))
                throw new IOException("managed private file owner differs from the current process principal");
        } else {
            verifyExactAcl(path, windowsPrincipals, windowsFileAcl(windowsPrincipals), false);
        }
    }

    private void validateSecretFile(Path path, BasicFileAttributes before) throws IOException {
        if (before.size() < 43L || before.size() > 44L || before.size() > MAX_SECRET_BYTES)
            throw new IOException("existing runtime secret has an invalid bounded length; no rotation was attempted");
        byte[] bytes = readBoundedFile(path, before);
        try { validateSecretBytes(bytes); }
        finally { Arrays.fill(bytes, (byte) 0); }
    }

    private static void validateSecretBytes(byte[] bytes) throws IOException {
        int length = bytes.length;
        if (length == 44 && bytes[43] == '\n') length--;
        if (length != 43) throw new IOException("runtime secret encoding is invalid; no rotation was attempted");
        byte[] encoded = Arrays.copyOf(bytes, length);
        byte[] decoded = null;
        try {
            for (byte value : encoded) {
                int c = value & 0xff;
                if (!(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_'))
                    throw new IOException("runtime secret encoding is invalid; no rotation was attempted");
            }
            decoded = java.util.Base64.getUrlDecoder().decode(encoded);
            if (decoded.length != 32 || !Arrays.equals(java.util.Base64.getUrlEncoder().withoutPadding().encode(decoded), encoded))
                throw new IOException("runtime secret encoding is invalid; no rotation was attempted");
        } catch (IllegalArgumentException malformed) {
            throw new IOException("runtime secret encoding is invalid; no rotation was attempted", malformed);
        } finally {
            Arrays.fill(encoded, (byte) 0);
            if (decoded != null) Arrays.fill(decoded, (byte) 0);
        }
    }

    private void verifySameFile(Path path, BasicFileAttributes before, int expectedSize) throws IOException {
        BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!after.isRegularFile() || after.isSymbolicLink() || after.size() != expectedSize
                || !Objects.equals(before.fileKey(), after.fileKey())
                || !before.creationTime().equals(after.creationTime()))
            throw new IOException("managed private file identity or length changed during persistence");
        verifyPrivateFile(path);
    }

    private byte[] readBoundedFile(Path path, BasicFileAttributes before) throws IOException {
        if (!before.isRegularFile() || before.isSymbolicLink() || before.size() > MAX_SECRET_BYTES)
            throw new IOException("runtime secret must be a bounded regular non-link file");
        requireStableFileKey(before, "runtime secret");
        try (FileChannel input = openProtectedRead(path)) {
            verifyPrivateFile(path);
            verifySameFile(path, before, Math.toIntExact(before.size()));
            if (input.size() != before.size()) throw new IOException("runtime secret changed before read");
            byte[] bytes = readExact(input, Math.toIntExact(before.size()));
            BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!Objects.equals(before.fileKey(), after.fileKey()) || before.size() != after.size()
                    || !before.creationTime().equals(after.creationTime())
                    || !before.lastModifiedTime().equals(after.lastModifiedTime())) {
                Arrays.fill(bytes, (byte) 0);
                throw new IOException("runtime secret changed during bounded read");
            }
            return bytes;
        }
    }

    private static void ensurePlainDirectory(Path path) throws IOException {
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) { requireDirectory(path, "managed installation parent"); return; }
        try { Files.createDirectory(path); }
        catch (FileAlreadyExistsException raced) { requireDirectory(path, "managed installation parent"); }
        requireDirectory(path, "managed installation parent");
    }

    private static void requireDirectory(Path path, String label) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory() || attributes.isSymbolicLink()) throw new IOException(label + " must be an existing non-link directory");
    }

    private static void createOrVerifyPosixDirectory(Path directory) throws IOException {
        if (Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
            requireDirectory(directory, "managed installation directory");
            var view = Files.getFileAttributeView(directory, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            if (view == null || !view.readAttributes().permissions().equals(PRIVATE_DIRECTORY))
                throw new IOException("existing managed installation directory is not exactly owner-only; it was not modified");
            if (!Files.getOwner(directory, LinkOption.NOFOLLOW_LINKS).equals(currentProcessPrincipal(directory)))
                throw new IOException("existing managed installation directory owner differs from the current process principal");
            return;
        }
        try { Files.createDirectory(directory, PosixFilePermissions.asFileAttribute(PRIVATE_DIRECTORY)); }
        catch (FileAlreadyExistsException raced) { throw new IOException("managed installation directory appeared during creation; inspect it", raced); }
        requireDirectory(directory, "managed installation directory");
        var view = Files.getFileAttributeView(directory, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (view == null || !view.readAttributes().permissions().equals(PRIVATE_DIRECTORY))
            throw new IOException("new managed installation directory is not exactly owner-only");
        if (!Files.getOwner(directory, LinkOption.NOFOLLOW_LINKS).equals(currentProcessPrincipal(directory)))
            throw new IOException("new managed installation directory owner differs from the current process principal");
    }

    private static List<UserPrincipal> windowsPrincipals(Path directory) throws IOException {
        String processUser = ProcessHandle.current().info().user()
                .orElseThrow(() -> new IOException("current process principal cannot be determined for managed ACL"));
        UserPrincipalLookupService lookup = directory.getFileSystem().getUserPrincipalLookupService();
        var principals = new ArrayList<UserPrincipal>();
        addPrincipal(principals, lookup.lookupPrincipalByName(processUser));
        addPrincipal(principals, lookup.lookupPrincipalByName("NT AUTHORITY\\SYSTEM"));
        addPrincipal(principals, lookup.lookupPrincipalByName("BUILTIN\\Administrators"));
        return List.copyOf(principals);
    }

    private static void addPrincipal(List<UserPrincipal> principals, UserPrincipal candidate) {
        if (principals.stream().noneMatch(candidate::equals)) principals.add(candidate);
    }

    private static void createOrVerifyWindowsDirectory(Path directory, List<UserPrincipal> principals) throws IOException {
        List<AclEntry> expected = windowsDirectoryAcl(principals);
        if (Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
            requireDirectory(directory, "managed installation directory");
            verifyExactAcl(directory, principals, expected, true);
            return;
        }
        try { Files.createDirectory(directory, aclAttribute(expected)); }
        catch (FileAlreadyExistsException raced) { throw new IOException("managed installation directory appeared during ACL creation; inspect it", raced); }
        requireDirectory(directory, "managed installation directory");
        verifyExactAcl(directory, principals, expected, true);
    }

    private static List<AclEntry> windowsDirectoryAcl(List<UserPrincipal> principals) {
        Set<AclEntryFlag> inheritance = EnumSet.of(AclEntryFlag.FILE_INHERIT, AclEntryFlag.DIRECTORY_INHERIT);
        return principals.stream().map(principal -> AclEntry.newBuilder().setType(AclEntryType.ALLOW)
                .setPrincipal(principal).setPermissions(EnumSet.allOf(AclEntryPermission.class)).setFlags(inheritance).build()).toList();
    }

    private static List<AclEntry> windowsFileAcl(List<UserPrincipal> principals) {
        return principals.stream().map(principal -> AclEntry.newBuilder().setType(AclEntryType.ALLOW)
                .setPrincipal(principal).setPermissions(EnumSet.allOf(AclEntryPermission.class)).build()).toList();
    }

    private static FileAttribute<List<AclEntry>> aclAttribute(List<AclEntry> acl) {
        List<AclEntry> copy = List.copyOf(acl);
        return new FileAttribute<>() {
            @Override public String name() { return "acl:acl"; }
            @Override public List<AclEntry> value() { return copy; }
        };
    }

    private static void verifyExactAcl(Path path, List<UserPrincipal> principals, List<AclEntry> expected,
                                       boolean directory) throws IOException {
        var view = Files.getFileAttributeView(path, AclFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (view == null) throw new IOException("managed private path has no verifiable ACL view");
        List<AclEntry> actual = view.getAcl();
        if (actual.size() != principals.size())
            throw new IOException("managed private ACL contains unexpected or inherited principals");
        Set<AclEntry> expectedSet = new HashSet<>(expected);
        Set<AclEntry> actualSet = new HashSet<>(actual);
        if (actualSet.size() != actual.size() || !actualSet.equals(expectedSet))
            throw new IOException("managed private ACL differs from exact process/SYSTEM/Administrators policy");
        UserPrincipal owner = Files.getOwner(path, LinkOption.NOFOLLOW_LINKS);
        if (principals.stream().noneMatch(owner::equals))
            throw new IOException("managed private path owner is outside the trusted ACL principals");
        // Java ACL 视图不暴露 ACE 的继承来源；上面的完整 entry/flags 比较负责拒绝额外权限。
        if (!directory && actual.stream().anyMatch(entry -> !entry.flags().isEmpty()))
            throw new IOException("managed private file ACL contains unexpected inheritance flags");
    }

    private static UserPrincipal currentProcessPrincipal(Path path) throws IOException {
        String name = ProcessHandle.current().info().user()
                .orElseThrow(() -> new IOException("current process principal cannot be determined"));
        return path.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByName(name);
    }

    private FileChannel createNew(Path path, FileAttribute<?>[] attributes) throws IOException {
        Set<OpenOption> options = new HashSet<>(Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS));
        addWindowsSharingGuards(options);
        return FileChannel.open(path, options, attributes);
    }

    private FileChannel openProtectedRead(Path path) throws IOException {
        Set<OpenOption> options = new HashSet<>(Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
        addWindowsSharingGuards(options);
        return FileChannel.open(path, options);
    }

    private void addWindowsSharingGuards(Set<OpenOption> options) {
        if (platform == Platform.WINDOWS_ACL) {
            // Windows JDK25 的 fileKey 固定为 null；持有此真实 handle 期间禁止替换、删除和并发写入。
            options.add(ExtendedOpenOption.NOSHARE_DELETE);
            options.add(ExtendedOpenOption.NOSHARE_WRITE);
        }
    }

    private static BasicFileAttributes readAttributesIfPresent(Path path) throws IOException {
        try { return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS); }
        catch (java.nio.file.NoSuchFileException absent) { return null; }
    }

    private static void requireRegularNoLink(BasicFileAttributes attributes, String label) throws IOException {
        if (!attributes.isRegularFile() || attributes.isSymbolicLink()) throw new IOException(label + " must be a regular non-link file");
    }

    private void requireStableFileKey(BasicFileAttributes attributes, String label) throws IOException {
        if (attributes.fileKey() == null && platform != Platform.WINDOWS_ACL)
            throw new IOException(label + " filesystem does not expose a stable file identity");
    }

    private static void writeFully(FileChannel channel, byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long position = 0L;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written <= 0) throw new IOException("managed private file write made no progress");
            position += written;
        }
    }

    private static byte[] readExact(FileChannel channel, int size) throws IOException {
        byte[] bytes = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long position = 0L;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) throw new IOException("managed private file was truncated during readback");
            if (read == 0) throw new IOException("managed private file read made no progress");
            position += read;
        }
        if (channel.size() != size) {
            Arrays.fill(bytes, (byte) 0);
            throw new IOException("managed private file has an unexpected trailing length");
        }
        return bytes;
    }

    private static void closeOwned(FileChannel channel, Throwable primary) throws IOException {
        try { channel.close(); }
        catch (Throwable cleanup) {
            if (primary != null) { if (cleanup != primary) primary.addSuppressed(cleanup); return; }
            if (cleanup instanceof IOException io) throw io;
            throw new IOException("managed private file close was not confirmed", cleanup);
        }
    }

    @SuppressWarnings("unchecked") private static <T extends Throwable> RuntimeException rethrow(Throwable failure) throws T { throw (T) failure; }
}
