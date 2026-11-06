package com.wi.floatingpod.bootstrap;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 启动 owner 的一次依赖装配；只验证 archive/resource，不解析 Scala/JDBC 类或取得业务权利。
 * 调用窗口必须冻结 mods 与 loader URL writer；公开发现 API 不提供 cold-cache 无副作用探测。
 */
public final class EarlyDependencyResolver {
    public static final String SQL_OUTER = "C4D0A46CA2F0DF3E44F48F73DAF8CBC741C24DEE7595262EA28485FEC5BD5EEE";
    public static final String MYSQL = "69084713593A4AA8D07C383619B9639276F08BCCF8FAF1C562178147D389B1E1";
    public static final String PROTOBUF = "D60DFE7C68A0D38A248CCA96924F289DC7E1966A887EE7CAE397701AF08575AE";
    public static final String SQLITE = "F8DCE420A6016278497F4F0560F65602B9DB858DE141B541CF91A4644EBE4639";
    private static final Pattern CANONICAL_SCALA_VERSION = Pattern.compile("(0|[1-9][0-9]{0,9})\\.(0|[1-9][0-9]{0,9})\\.(0|[1-9][0-9]{0,9})");
    private static final String MINIMUM_SCALA_VERSION = "3.8.4";

    public record ScalarPins(String outerSha256, String runtimeVersion,
                             String librarySha256, String parserSha256) {
        public ScalarPins {
            outerSha256 = hashPin(outerSha256);
            librarySha256 = hashPin(librarySha256);
            parserSha256 = hashPin(parserSha256);
            if (!isSupportedScalarRuntimeVersion(runtimeVersion))
                throw new IllegalArgumentException("approved Scalar runtime version must be canonical semantic version 3.8.4 or newer");
        }
    }
    public record FuguePins(String outerSha256, String runtimeVersion) {
        public FuguePins {
            outerSha256 = hashPin(outerSha256);
            if (runtimeVersion == null || runtimeVersion.isBlank())
                throw new IllegalArgumentException("explicit Fugue runtime version required");
        }
    }
    public record ExpectedPins(String sqlOuterSha256, Optional<ScalarPins> scalar, Optional<FuguePins> fugue) {
        /** 旧调用只保留缺失失败语义；生产 resolve 不允许省略 Fugue。 */
        public ExpectedPins(String sqlOuterSha256, Optional<ScalarPins> scalar) {
            this(sqlOuterSha256, scalar, Optional.empty());
        }
        public ExpectedPins {
            sqlOuterSha256 = hashPin(sqlOuterSha256);
            if (!SQL_OUTER.equals(sqlOuterSha256)) throw new IllegalArgumentException("unapproved SQL-JDBC artifact");
            scalar = Objects.requireNonNull(scalar);
            fugue = Objects.requireNonNull(fugue);
        }
    }
    public record ArchiveIdentity(Path path, String sha256, List<String> resources) {
        public ArchiveIdentity {
            path = Objects.requireNonNull(path);
            sha256 = hashPin(sha256);
            resources = List.copyOf(resources);
        }
    }
    public record ResourceOrigin(String resource, URL url, Path archive, String archiveSha256) {}
    /** URL 验证/装配收据，明确不是 class CodeSource、SQL、Prepared 或 Ready 证明。 */
    public record VerifiedDependencies(List<ArchiveIdentity> archives, List<ResourceOrigin> visibleResources,
                                       List<URL> addedUrls, String businessBackend) {
        public VerifiedDependencies {
            archives = List.copyOf(archives);
            visibleResources = List.copyOf(visibleResources);
            addedUrls = List.copyOf(addedUrls);
            if (!"MYSQL".equals(businessBackend)) throw new IllegalArgumentException("explicit MYSQL only");
        }
    }
    public static final class DependencyUnavailable extends IllegalStateException {
        public DependencyUnavailable(String stage) { super(stage); }
        public DependencyUnavailable(String stage, Throwable cause) { super(stage, cause); }
    }

    /** 只提供实际发现和 URL 入口；禁止在实现中试加载类。 */
    interface Discovery {
        Snapshot snapshot(List<String> resources) throws IOException;
        void addURL(URL url);
        List<ResourceOrigin> resources(List<String> resources) throws IOException;
    }
    record Snapshot(List<Path> sqlSources, List<Path> scalarSources, List<Path> fugueSources, List<Path> candidates,
                    List<Path> loaderSources, List<ResourceOrigin> visibleResources) {
        Snapshot(List<Path> sqlSources, List<Path> scalarSources, List<Path> candidates,
                 List<Path> loaderSources, List<ResourceOrigin> visibleResources) {
            this(sqlSources, scalarSources, List.of(), candidates, loaderSources, visibleResources);
        }
        Snapshot {
            sqlSources = List.copyOf(sqlSources);
            scalarSources = List.copyOf(scalarSources);
            fugueSources = List.copyOf(fugueSources);
            candidates = List.copyOf(candidates);
            loaderSources = List.copyOf(loaderSources);
            visibleResources = List.copyOf(visibleResources);
        }
    }
    record Spec(String name, String sha256, List<String> resources) {
        Spec {
            sha256 = hashPin(sha256);
            resources = List.copyOf(resources);
        }
    }
    record Plan(List<ArchiveIdentity> archives, List<ResourceOrigin> visible, List<URL> additions) {
        Plan {
            archives = List.copyOf(archives);
            visible = List.copyOf(visible);
            additions = List.copyOf(additions);
        }
    }

    private final Thread bootOwner;
    private final ExpectedPins pins;
    private boolean attempted;

    public EarlyDependencyResolver(Thread bootOwner, ExpectedPins pins) {
        this.bootOwner = Objects.requireNonNull(bootOwner);
        this.pins = Objects.requireNonNull(pins);
    }

    public VerifiedDependencies resolveAndVerifyProduction() throws IOException {
        requireOwner();
        return resolve(new ProductionEarlyDependencyDiscovery());
    }

    VerifiedDependencies resolve(Discovery discovery) throws IOException {
        requireOwner();
        if (attempted) throw new DependencyUnavailable("dependency assembly already attempted");
        attempted = true; // 失败不重试部分 URL 装配，也不制造成功。
        ScalarPins scalar = pins.scalar().orElseThrow(() -> new DependencyUnavailable("explicit Scalar artifact pins unavailable"));
        FuguePins fugue = pins.fugue().orElseThrow(() -> new DependencyUnavailable("explicit Fugue artifact pins unavailable"));
        List<Spec> specs = specs(scalar, fugue);
        List<String> anchors = specs.stream().flatMap(s -> s.resources().stream()).toList();
        Snapshot snapshot = discovery.snapshot(anchors);
        Path sql = uniqueSource(snapshot.sqlSources(), "SQL-JDBC outer");
        Path scalarOuter = uniqueSource(snapshot.scalarSources(), "Scalar outer");
        Path fugueOuter = uniqueSource(snapshot.fugueSources(), "Fugue outer");
        verifyHash(sql, pins.sqlOuterSha256());
        verifyHash(scalarOuter, scalar.outerSha256());
        verifyHash(fugueOuter, fugue.outerSha256());
        verifyMetadata(fugueOuter, "fugue", fugue.runtimeVersion());
        verifyMetadata(scalarOuter, "scalar", scalar.runtimeVersion());
        verifyMetadata(sql, "sqljdbcdriverlib", "0.2.0-rc.4");
        verifyContained(sql, pins.sqlOuterSha256(), List.of(specs.get(1), specs.get(2), specs.get(3)), Set.of());
        verifyScalarContained(scalarOuter, scalar);
        List<Path> candidates = new ArrayList<>(snapshot.candidates());
        candidates.add(sql); candidates.add(scalarOuter); candidates.add(fugueOuter);
        Plan plan = verifySnapshot(new Snapshot(snapshot.sqlSources(), snapshot.scalarSources(), snapshot.fugueSources(), candidates,
                snapshot.loaderSources(), snapshot.visibleResources()), specs);
        // 先完成所有 archive/resource 验证，才允许第一次 addURL。外部并行 writer 不属于此入口合同。
        for (ArchiveIdentity archive : plan.archives()) verifyHash(archive.path(), archive.sha256());
        for (URL url : plan.additions()) discovery.addURL(url);
        List<ResourceOrigin> after = discovery.resources(anchors);
        validateVisible(after, plan.archives(), true);
        return new VerifiedDependencies(plan.archives(), after, plan.additions(), "MYSQL");
    }

    private void requireOwner() {
        if (Thread.currentThread() != bootOwner) throw new DependencyUnavailable("requires exact boot owner");
    }

    private static List<Spec> specs(ScalarPins scalar, FuguePins fugue) {
        return List.of(
            new Spec("SQL-JDBC outer", SQL_OUTER, List.of("com/wi/sqljdbcdriverlib/SqlJdbcDistribution.class")),
            new Spec("mysql-connector-j-26.7.0.jar", MYSQL, List.of("com/mysql/cj/jdbc/Driver.class")),
            new Spec("protobuf-java-4.31.1.jar", PROTOBUF, List.of("com/google/protobuf/Message.class")),
            new Spec("sqlite-jdbc-3.54.1.0.jar", SQLITE, List.of("org/sqlite/JDBC.class")),
            new Spec("Scalar outer", scalar.outerSha256(), List.of("com/cleanroommc/scalar/ScalaLanguageAdapter.class")),
            new Spec("scala-library-" + scalar.runtimeVersion() + ".jar", scalar.librarySha256(),
                List.of("scala/Predef.class", "scala/Option.class", "scala/deriving/Mirror.class", "scala/runtime/Scala3RunTime.class")),
            new Spec("scala-parser-combinators_3-2.4.0.jar", scalar.parserSha256(), List.of("scala/util/parsing/combinator/Parsers.class")),
            new Spec("Fugue outer", fugue.outerSha256(), List.of("com/cleanroommc/fugue/common/Fugue.class",
                "com/cleanroommc/fugue/common/FugueLoadingPlugin.class"))
        );
    }

    /** Launch inventory conversion; visible dependency resources are independently verified. */
    static List<Path> localLaunchSources(List<URL> urls) {
        List<Path> sources = new ArrayList<>();
        for (URL url : urls) {
            // Forge adds this generated-wrapper root to LaunchClassLoader. It is not an
            // archive candidate; mandatory dependency resources still pass inspectResources
            // and exact archive/hash validation, including any resource shadowing it creates.
            if ("asmgen:/".equals(url.toExternalForm())) continue;
            if ("jar".equals(url.getProtocol())) {
                sources.add(localJarSource(url));
                continue;
            }
            if (!"file".equals(url.getProtocol()))
                throw new DependencyUnavailable("nonlocal Launch source protocol=" + url.getProtocol());
            try { sources.add(Path.of(url.toURI())); }
            catch (java.net.URISyntaxException cause) { throw new DependencyUnavailable("invalid Launch source URI", cause); }
        }
        return List.copyOf(sources);
    }

    /** Extract only an ordinary local outer archive; never open a URL or a nested JAR.
     * A resource suffix is a classloader view, not proof of dependency visibility: the
     * unchanged anchor-resource check independently verifies the actual local JAR owner.
     */
    private static Path localJarSource(URL url) {
        if (url.getQuery() != null || url.getRef() != null)
            throw new DependencyUnavailable("ambiguous Launch JAR source");
        String value = url.getFile();
        int separator = value.indexOf("!/");
        if (separator < 0 || value.indexOf("!/", separator + 2) >= 0)
            throw new DependencyUnavailable("unsupported nested or malformed Launch JAR source");
        URI archive;
        try { archive = new URI(value.substring(0, separator)); }
        catch (java.net.URISyntaxException invalidInput) {
            // Invalid source syntax is an expected rejection. Do not echo URL input,
            // including a possible user-info/query secret carried by a malformed source.
            throw new DependencyUnavailable("invalid Launch JAR archive URI");
        }
        if (!"file".equals(archive.getScheme()) || archive.isOpaque() ||
                archive.getRawAuthority() != null || archive.getRawQuery() != null || archive.getRawFragment() != null)
            throw new DependencyUnavailable("Launch JAR source requires an unambiguous local file archive");
        try { return Path.of(archive); }
        catch (IllegalArgumentException invalidInput) {
            throw new DependencyUnavailable("invalid local Launch JAR archive path");
        }
    }

    /** 测试 seam：真实文件/ZIP 与 immutable snapshot 直接验证，无 loader 变更或 class resolve。 */
    static Plan verifySnapshot(Snapshot snapshot, List<Spec> specs) throws IOException {
        Set<Path> paths = new LinkedHashSet<>();
        for (Path p : snapshot.candidates()) paths.add(canonical(p));
        for (Path p : snapshot.loaderSources()) paths.add(canonical(p));
        List<ArchiveIdentity> verified = new ArrayList<>();
        List<URL> additions = new ArrayList<>();
        Set<Path> loaded = new LinkedHashSet<>();
        for (Path p : snapshot.loaderSources()) loaded.add(canonical(p));
        for (Spec spec : specs) {
            Set<Path> providers = new LinkedHashSet<>();
            for (Path path : paths) {
                if (!Files.isRegularFile(path)) {
                    if (Files.isDirectory(path)) {
                        for (String resource : spec.resources())
                            if (Files.exists(path.resolve(resource))) throw new DependencyUnavailable("unapproved directory resource owner: " + spec.name());
                    }
                    continue;
                }
                try (JarFile jar = new JarFile(path.toFile())) {
                    boolean any = false;
                    for (String resource : spec.resources()) {
                        long count = jar.stream().filter(e -> e.getName().equals(resource)).count();
                        if (count > 1) throw new DependencyUnavailable("duplicate archive entry: " + resource);
                        any |= count == 1;
                    }
                    if (any) {
                        for (String resource : spec.resources())
                            if (jar.getJarEntry(resource) == null) throw new DependencyUnavailable("incomplete resource provider: " + spec.name());
                        providers.add(path);
                    }
                }
            }
            if (providers.size() != 1) throw new DependencyUnavailable("requires unique provider: " + spec.name());
            Path provider = providers.iterator().next();
            verifyHash(provider, spec.sha256());
            verified.add(new ArchiveIdentity(provider, spec.sha256(), spec.resources()));
            if (!loaded.contains(provider)) {
                // Cleanroom 后续候选去重使用 File.equals，不能把已发现 Mod 的 ./ 拼法改成另一个 URL。
                // 文件身份、唯一 provider 与哈希仍全部使用上面的 canonical path 核验。
                Path launchSource = provider;
                List<Path> outerSources = new ArrayList<>(snapshot.sqlSources());
                outerSources.addAll(snapshot.scalarSources());
                outerSources.addAll(snapshot.fugueSources());
                for (Path original : outerSources) {
                    if (canonical(original).equals(provider)) { launchSource = original; break; }
                }
                additions.add(launchSource.toFile().getAbsoluteFile().toURI().toURL());
            }
        }
        validateVisible(snapshot.visibleResources(), verified, false);
        return new Plan(verified, snapshot.visibleResources(), additions.stream().distinct().toList());
    }

    static void validateVisible(List<ResourceOrigin> visible, List<ArchiveIdentity> archives, boolean requireAll) throws IOException {
        for (ArchiveIdentity expected : archives) {
            for (String name : expected.resources()) {
                List<ResourceOrigin> found = visible.stream().filter(r -> name.equals(r.resource())).toList();
                if (found.size() > 1 || (requireAll && found.size() != 1))
                    throw new DependencyUnavailable("duplicate or unavailable Launch resource: " + name);
                for (ResourceOrigin origin : found) {
                    if (!canonical(origin.archive()).equals(expected.path()) || !origin.archiveSha256().equals(expected.sha256()))
                        throw new DependencyUnavailable("wrong Launch resource archive: " + name);
                    verifyHash(origin.archive(), expected.sha256());
                }
            }
        }
    }

    static Path uniqueSource(List<Path> paths, String role) throws IOException {
        Set<Path> unique = new LinkedHashSet<>();
        for (Path p : paths) unique.add(canonical(p));
        if (unique.size() != 1) throw new DependencyUnavailable("requires unique discovered " + role);
        Path p = unique.iterator().next();
        if (!Files.isRegularFile(p)) throw new DependencyUnavailable("discovered outer must be regular archive: " + role);
        return p;
    }

    private static boolean isSupportedScalarRuntimeVersion(String version) {
        if (version == null) return false;
        Matcher candidate = CANONICAL_SCALA_VERSION.matcher(version);
        Matcher minimum = CANONICAL_SCALA_VERSION.matcher(MINIMUM_SCALA_VERSION);
        if (!candidate.matches() || !minimum.matches()) return false;
        for (int group = 1; group <= 3; group++) {
            int order = new BigInteger(candidate.group(group)).compareTo(new BigInteger(minimum.group(group)));
            if (order != 0) return order > 0;
        }
        return true;
    }

    static void verifyScalarContained(Path outer, ScalarPins scalar) throws IOException {
        Objects.requireNonNull(scalar);
        // Only this version-matched ancillary archive is optional; outer SHA pins its full contents.
        verifyContained(outer, scalar.outerSha256(), List.of(
                new Spec("scala-library-" + scalar.runtimeVersion() + ".jar", scalar.librarySha256(),
                    List.of("scala/Predef.class", "scala/Option.class", "scala/deriving/Mirror.class", "scala/runtime/Scala3RunTime.class")),
                new Spec("scala-parser-combinators_3-2.4.0.jar", scalar.parserSha256(),
                    List.of("scala/util/parsing/combinator/Parsers.class"))),
            Set.of("scala3-tasty-inspector_3-" + scalar.runtimeVersion() + ".jar"));
    }

    static void verifyContained(Path outer, String expectedOuterSha256, List<Spec> payloads,
                                Set<String> optionalPayloadNames) throws IOException {
        // 只有独立外层 SHA pin 覆盖的 Scalar 附加项才可信；清单与实际 nested JAR 集合必须完全一致。
        verifyHash(outer, expectedOuterSha256);
        try (JarFile jar = new JarFile(outer.toFile())) {
            if (jar.getManifest() == null) throw new DependencyUnavailable("outer manifest unavailable");
            var attrs = jar.getManifest().getMainAttributes();
            if (!"true".equalsIgnoreCase(attrs.getValue("NonModDeps")))
                throw new DependencyUnavailable("outer NonModDeps unavailable");
            String declared = attrs.getValue("ContainedDeps");
            if (declared == null || declared.isBlank())
                throw new DependencyUnavailable("outer ContainedDeps unavailable");
            Set<String> declaredEntries = new HashSet<>();
            for (String name : declared.strip().split("\\s+")) {
                if (!name.matches("[A-Za-z0-9_.+-]+\\.jar") || name.equals(".") || name.equals("..")
                        || !declaredEntries.add(name))
                    throw new DependencyUnavailable("outer ContainedDeps has an invalid or duplicate payload name");
            }
            Set<String> actualEntries = new HashSet<>();
            jar.stream().filter(entry -> !entry.isDirectory() && entry.getName().endsWith(".jar"))
                .forEach(entry -> {
                    if (!entry.getName().matches("[A-Za-z0-9_.+-]+\\.jar") || !actualEntries.add(entry.getName()))
                        throw new DependencyUnavailable("outer archive has an invalid or duplicate nested payload name");
                });
            if (!declaredEntries.equals(actualEntries))
                throw new DependencyUnavailable("outer ContainedDeps differs from actual nested archives");
            Set<String> independentlyPinned = new HashSet<>();
            for (Spec spec : payloads) {
                if (!independentlyPinned.add(spec.name()) || !declaredEntries.contains(spec.name()))
                    throw new DependencyUnavailable("independently pinned nested payload absent or duplicated");
                if (jar.stream().filter(e -> e.getName().equals(spec.name())).count() != 1)
                    throw new DependencyUnavailable("nested payload absent or duplicate: " + spec.name());
                try (InputStream in = jar.getInputStream(jar.getJarEntry(spec.name()))) {
                    if (!sha256(in).equals(spec.sha256())) throw new DependencyUnavailable("nested payload hash mismatch: " + spec.name());
                }
            }
            Set<String> expectedOptional = new HashSet<>(optionalPayloadNames);
            if (expectedOptional.stream().anyMatch(name -> !name.matches("scala3-tasty-inspector_3-[0-9]+\\.[0-9]+\\.[0-9]+\\.jar"))
                    || expectedOptional.stream().anyMatch(independentlyPinned::contains))
                throw new DependencyUnavailable("optional nested payload is not an approved versioned Tasty inspector");
            Set<String> unrecognized = new HashSet<>(declaredEntries);
            unrecognized.removeAll(independentlyPinned);
            unrecognized.removeAll(expectedOptional);
            if (!unrecognized.isEmpty())
                throw new DependencyUnavailable("outer contains an unrecognized or wrong-version nested payload");
        }
    }

    static void verifyMetadata(Path outer, String modId, String version) throws IOException {
        try (JarFile jar = new JarFile(outer.toFile())) {
            if (jar.stream().filter(e -> e.getName().equals("mcmod.info")).count() != 1)
                throw new DependencyUnavailable("unique runtime metadata unavailable");
            try (InputStream in = jar.getInputStream(jar.getJarEntry("mcmod.info"))) {
                var array = JsonParser.parseString(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)).getAsJsonArray();
                int matches = 0;
                for (var value : array) {
                    var item = value.getAsJsonObject();
                    if (!item.has("modid") || !item.get("modid").isJsonPrimitive()
                            || !item.get("modid").getAsJsonPrimitive().isString())
                        throw new DependencyUnavailable("runtime metadata mod id must be a string");
                    if (modId.equals(item.get("modid").getAsString())) {
                        matches++;
                        if (!item.has("version") || !item.get("version").isJsonPrimitive()
                                || !item.get("version").getAsJsonPrimitive().isString()
                                || !version.equals(item.get("version").getAsString()))
                            throw new DependencyUnavailable("runtime metadata version mismatch");
                    }
                }
                if (matches != 1) throw new DependencyUnavailable("unique runtime metadata mod id unavailable");
            } catch (com.google.gson.JsonParseException | IllegalStateException cause) {
                if (cause instanceof DependencyUnavailable unavailable) throw unavailable;
                throw new DependencyUnavailable("malformed runtime metadata", cause);
            }
        }
    }

    static List<ResourceOrigin> inspectResources(ClassLoader loader, List<String> anchors) throws IOException {
        List<ResourceOrigin> result = new ArrayList<>();
        for (String anchor : anchors) {
            var urls = loader.getResources(anchor); // 父资源也保留，不能忽略冲突。
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (!(url.openConnection() instanceof JarURLConnection connection))
                    throw new DependencyUnavailable("resource owner is not a local archive: " + anchor);
                connection.setUseCaches(false);
                URL archiveUrl = connection.getJarFileURL();
                if (!"file".equals(archiveUrl.getProtocol())) throw new DependencyUnavailable("resource archive is not local");
                Path path;
                try { path = canonical(Path.of(archiveUrl.toURI())); }
                catch (java.net.URISyntaxException cause) { throw new DependencyUnavailable("invalid resource archive URI", cause); }
                if (!anchor.equals(connection.getEntryName())) throw new DependencyUnavailable("resource entry identity mismatch");
                result.add(new ResourceOrigin(anchor, url, path, hashFile(path)));
            }
        }
        return List.copyOf(result);
    }

    static Path canonical(Path path) throws IOException { return Objects.requireNonNull(path).toRealPath(); }
    static void verifyHash(Path path, String expected) throws IOException {
        if (!hashFile(path).equals(expected)) throw new DependencyUnavailable("archive hash mismatch");
    }
    static String hashFile(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) { return sha256(in); }
    }
    static String sha256(InputStream in) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[16384];
            for (int n; (n = in.read(buffer)) != -1;) digest.update(buffer, 0, n);
            return HexFormat.of().withUpperCase().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException cause) { throw new IllegalStateException("JDK SHA-256 unavailable", cause); }
    }
    static String hashPin(String pin) {
        if (pin == null || !pin.matches("[0-9A-F]{64}")) throw new IllegalArgumentException("explicit uppercase SHA-256 pin required");
        return pin;
    }
}

