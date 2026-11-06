package com.wi.floatingpod.server;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/** 固定 discovery DATA 名称；不选择下一代、不证明 absence、不发行任何权限。 */
public final class B1HandoffGenerationName {
    public enum Family {
        BIRTH("birth-v2", false), PLAN("plan-v2", false),
        TERMINAL("terminal-v2", false), SUPERSESSION("supersession-v2", false),
        TRANSFER("transfer-v3", true), TARGET_RESULT("transfer-result-v3", true),
        TRANSFER_SUPERSESSION("transfer-supersession-v3", true),
        TARGET_RETIREMENT("transfer-retirement-v4", true);
        private final String tag;
        private final boolean transfer;
        Family(String tag, boolean transfer) { this.tag = tag; this.transfer = transfer; }
    }
    private static final String PREFIX = ".floatingpod-b1-handoff-";
    private final Family family;
    private final String baseKey;
    private final long generation, transferGeneration;
    private final boolean temporary;

    private B1HandoffGenerationName(Family family, String key, long g, long t, boolean temporary) {
        this.family = Objects.requireNonNull(family);
        if (key == null || key.length() != 64) throw new IllegalArgumentException("Exact lowercase baseKey64 required");
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (!(c >= '0' && c <= '9' || c >= 'a' && c <= 'f'))
                throw new IllegalArgumentException("Exact lowercase baseKey64 required");
        }
        if (g < 0 || (family.transfer ? t < 0 : t != -1))
            throw new IllegalArgumentException("Exact nonnegative generation topology required");
        this.baseKey = key; this.generation = g; this.transferGeneration = t; this.temporary = temporary;
    }
    public static B1HandoffGenerationName attempt(Family family, String key, long g) {
        if (family == null || family.transfer) throw new IllegalArgumentException("Attempt family required");
        return new B1HandoffGenerationName(family, key, g, -1, false);
    }
    public static B1HandoffGenerationName transfer(Family family, String key, long g, long t) {
        if (family == null || !family.transfer) throw new IllegalArgumentException("Transfer family required");
        return new B1HandoffGenerationName(family, key, g, t, false);
    }
    public static B1HandoffGenerationName parse(String name) throws IOException {
        if (name == null || name.length() > 255 || !name.startsWith(PREFIX))
            throw new IOException("Unknown HANDOFF discovery name");
        boolean temp = name.endsWith(".bin.tmp");
        String suffix = temp ? ".bin.tmp" : ".bin";
        if (!name.endsWith(suffix)) throw new IOException("Unknown HANDOFF discovery suffix");
        for (Family f : Family.values()) {
            String prefix = PREFIX + f.tag + "-";
            if (!name.startsWith(prefix)) continue;
            String fields = name.substring(prefix.length(), name.length() - suffix.length());
            int expected = 64 + 1 + 20 + (f.transfer ? 1 + 20 : 0);
            if (fields.length() != expected || fields.charAt(64) != '-' ||
                f.transfer && fields.charAt(85) != '-') throw new IOException("Invalid fixed HANDOFF name fields");
            try {
                var result = new B1HandoffGenerationName(f, fields.substring(0, 64),
                    decimal(fields.substring(65, 85)), f.transfer ? decimal(fields.substring(86)) : -1, temp);
                if (!result.basename().equals(name)) throw new IOException("Noncanonical HANDOFF discovery name");
                return result;
            } catch (IllegalArgumentException | ArithmeticException failure) {
                throw new IOException("Invalid HANDOFF name generation/key", failure);
            }
        }
        throw new IOException("Unknown HANDOFF discovery family");
    }
    private static long decimal(String text) {
        if (text.length() != 20) throw new IllegalArgumentException("Exact g20 required");
        long value = 0;
        for (int i = 0; i < 20; i++) {
            char c = text.charAt(i);
            if (c < '0' || c > '9') throw new IllegalArgumentException("ASCII decimal generation required");
            value = Math.addExact(Math.multiplyExact(value, 10), c - '0');
        }
        return value;
    }
    public static long checkedNext(long old) {
        if (old < 0) throw new IllegalArgumentException("Nonnegative generation required");
        return Math.addExact(old, 1);
    }
    public B1HandoffGenerationName temporaryName() {
        return new B1HandoffGenerationName(family, baseKey, generation, transferGeneration, true);
    }
    public String basename() {
        return PREFIX + family.tag + "-" + baseKey + "-" + String.format(Locale.ROOT, "%020d", generation) +
            (family.transfer ? "-" + String.format(Locale.ROOT, "%020d", transferGeneration) : "") +
            (temporary ? ".bin.tmp" : ".bin");
    }
    public Family family() { return family; }
    public String baseKey() { return baseKey; }
    public long generation() { return generation; }
    public long transferGeneration() { return transferGeneration; }
    public boolean isTemporary() { return temporary; }
    public void requireCanonical(Family expected, String key, long g, long t) throws IOException {
        if (temporary || family != expected || !baseKey.equals(key) || generation != g || transferGeneration != t)
            throw new IOException("Exact canonical artifact family/source generation required");
    }
}
