package com.wi.floatingpod.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/** 每次 fresh read，只有 caller 选定的 exact Owned-IO thread 能取得临时 secret。 */
public final class OwnedIoSecretFiles {
    public static final int MAX_SECRET_BYTES = 4096;
    record Buffers(byte[] bytes, char[] chars) { }
    private OwnedIoSecretFiles() { }
    public static <T> T withSecret(Thread exactOwnedIo, Path absolutePath, Function<char[], T> consumer) throws IOException {
        return withSecret(exactOwnedIo, absolutePath, consumer,
                new Buffers(new byte[MAX_SECRET_BYTES + 1], new char[MAX_SECRET_BYTES]));
    }
    /** 包内 allocation seam 只为验证实际 temp buffers 清零；production 每次创建全新 buffers。 */
    static <T> T withSecret(Thread exactOwnedIo, Path absolutePath, Function<char[], T> consumer, Buffers buffers) throws IOException {
        char[] exactChars = null;
        try {
            if (exactOwnedIo == null || Thread.currentThread() != exactOwnedIo)
                throw new IllegalStateException("Secret acquisition requires the exact selected Owned-IO thread");
            if (absolutePath == null || !absolutePath.isAbsolute()) throw new IllegalArgumentException("Secret file Path must be absolute");
            Objects.requireNonNull(consumer, "secret consumer is required");
            int length = 0;
            try (InputStream input = Files.newInputStream(absolutePath)) {
                while (length < buffers.bytes().length) {
                    int read = input.read(buffers.bytes(), length, buffers.bytes().length - length);
                    if (read < 0) break;
                    length += read;
                }
            }
            if (length > MAX_SECRET_BYTES) throw malformed("Secret file exceeds 4096-byte limit");
            if (length >= 3 && buffers.bytes()[0] == (byte) 0xef && buffers.bytes()[1] == (byte) 0xbb && buffers.bytes()[2] == (byte) 0xbf)
                throw malformed("Secret file UTF-8 BOM is not accepted");
            CharBuffer output = CharBuffer.wrap(buffers.chars());
            var decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            var decoded = decoder.decode(ByteBuffer.wrap(buffers.bytes(), 0, length), output, true);
            if (decoded.isError()) decoded.throwException();
            if (decoded.isOverflow()) throw new IOException("Secret UTF-8 decode exceeds temporary buffer");
            var flushed = decoder.flush(output);
            if (flushed.isError()) flushed.throwException();
            if (flushed.isOverflow()) throw new IOException("Secret UTF-8 decode exceeds temporary buffer");
            int chars = output.position();
            if (chars > 0 && buffers.chars()[chars - 1] == '\n') chars--;
            if (chars == 0) throw malformed("Secret file must not be empty");
            for (int i = 0; i < chars; i++) if (buffers.chars()[i] == '\n' || buffers.chars()[i] == '\r')
                throw malformed("Secret file must be one line with at most one optional ending LF");
            exactChars = Arrays.copyOf(buffers.chars(), chars);
            return consumer.apply(exactChars);
        } finally {
            Arrays.fill(buffers.bytes(), (byte) 0);
            Arrays.fill(buffers.chars(), '\0');
            if (exactChars != null) Arrays.fill(exactChars, '\0');
        }
    }
    private static IOException malformed(String safeReason) { return new IOException(safeReason); }
}
