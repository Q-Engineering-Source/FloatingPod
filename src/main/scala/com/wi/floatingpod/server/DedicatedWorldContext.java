package com.wi.floatingpod.server;

import net.minecraft.server.MinecraftServer;
import net.minecraft.world.storage.ISaveFormat;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/** 保留实际 dedicated server 的启动路径合同；不表示物理文件身份或 world Ready。 */
public final class DedicatedWorldContext {
    private static final String MARKER = "data/floatingpod-world-binding.bin";
    private final MinecraftServer server;
    private final ISaveFormat converter;
    private final Thread capturingOwner;
    private final String originalFolder;
    private final String selectedFolder;
    private final Path worldDirectory;
    private final Path markerPath;
    private volatile boolean pinned;

    private DedicatedWorldContext(MinecraftServer server, ISaveFormat converter,
                                  String originalFolder, String selectedFolder) {
        this.server = server;
        this.converter = converter;
        this.capturingOwner = Thread.currentThread();
        this.originalFolder = originalFolder;
        this.selectedFolder = selectedFolder;
        this.worldDirectory = converter.getFile(selectedFolder, "").toPath().toAbsolutePath();
        this.markerPath = converter.getFile(selectedFolder, MARKER).toPath().toAbsolutePath();
    }

    /** 由非 game boot owner 调用；只在没有实际 CLI folder 时读取同进程 CWD properties。 */
    public static DedicatedWorldContext capture(MinecraftServer actualServer) throws IOException {
        Objects.requireNonNull(actualServer, "actual server");
        if (!actualServer.isDedicatedServer()) throw new IllegalArgumentException("dedicated server required");
        ISaveFormat converter = Objects.requireNonNull(actualServer.getActiveAnvilConverter(), "actual converter");
        String original = actualServer.getFolderName();
        String selected = selectFolder(original, Path.of("server.properties"));
        return new DedicatedWorldContext(actualServer, converter, original, selected);
    }

    /** 唯一生产 capture 固定 CWD；显式 Path helper 只承载真实文件读取测试。 */
    static String selectFolder(String actualFolder, Path propertiesPath) throws IOException {
        if (actualFolder != null) return requireFolder(actualFolder);
        if (Files.notExists(propertiesPath)) return "world";
        Properties properties = new Properties();
        // InputStream 入口保留 vanilla ISO-8859-1 与 properties escape 语义。
        try (InputStream input = Files.newInputStream(propertiesPath)) {
            properties.load(input);
        }
        return requireFolder(properties.getProperty("level-name", "world"));
    }

    private static String requireFolder(String folder) {
        if (folder.isBlank()) throw new IllegalArgumentException("world folder must not be blank");
        return folder;
    }

    /** 必须在 original start 前、原捕获 owner 上调用；不重新读 properties。 */
    public void pinBeforeOriginalStart(MinecraftServer actualServer) {
        if (Thread.currentThread() != capturingOwner) throw new IllegalStateException("exact capturing owner required");
        if (pinned) throw new IllegalStateException("world context already pinned");
        requireServerAndConverter(actualServer);
        if (!Objects.equals(originalFolder, server.getFolderName()))
            throw new IllegalStateException("original world folder changed before pin");
        requirePaths();
        server.setFolderName(selectedFolder);
        if (!selectedFolder.equals(server.getFolderName())) throw new IllegalStateException("world folder pin was not retained");
        pinned = true;
    }

    /** AboutToStart main 只验证既有 pin；不读配置、不打开 Anvil 或 JDBC。 */
    public void validateAboutToStart(MinecraftServer actualServer) {
        requireServerAndConverter(actualServer);
        if (server.getServerThread() != Thread.currentThread()) throw new IllegalStateException("exact server Main thread required");
        if (!pinned || !selectedFolder.equals(server.getFolderName()))
            throw new IllegalStateException("pinned world folder differs at AboutToStart");
        requirePaths();
    }

    private void requireServerAndConverter(MinecraftServer actualServer) {
        if (actualServer != server || server.getActiveAnvilConverter() != converter || !server.isDedicatedServer())
            throw new IllegalStateException("actual server or converter identity changed");
    }

    private void requirePaths() {
        if (!worldDirectory.equals(converter.getFile(selectedFolder, "").toPath().toAbsolutePath()) ||
            !markerPath.equals(converter.getFile(selectedFolder, MARKER).toPath().toAbsolutePath()))
            throw new IllegalStateException("lexical world paths changed");
    }

    public MinecraftServer actualServer() { return server; }
    public ISaveFormat actualConverter() { return converter; }
    public Thread capturingOwner() { return capturingOwner; }
    public String originalFolder() { return originalFolder; }
    public String selectedFolder() { return selectedFolder; }
    public Path worldDirectory() { return worldDirectory; }
    public Path markerPath() { return markerPath; }
    @Override public String toString() { return "DedicatedWorldContext[lexical preparation context only]"; }
}
