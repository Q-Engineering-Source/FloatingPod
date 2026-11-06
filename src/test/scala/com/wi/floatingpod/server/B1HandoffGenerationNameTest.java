package com.wi.floatingpod.server;

import java.io.IOException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@Tag("b1-two-slot-pure")
class B1HandoffGenerationNameTest {
    private static final String KEY = "0123456789abcdef".repeat(4);
    @Test void allAttemptAndTransferFamiliesRoundTripCanonicalAndTemporary() throws Exception {
        for (var f : B1HandoffGenerationName.Family.values()) {
            boolean attempt = f == B1HandoffGenerationName.Family.BIRTH || f == B1HandoffGenerationName.Family.PLAN ||
                f == B1HandoffGenerationName.Family.TERMINAL || f == B1HandoffGenerationName.Family.SUPERSESSION;
            var value = attempt ? B1HandoffGenerationName.attempt(f, KEY, Long.MAX_VALUE) :
                B1HandoffGenerationName.transfer(f, KEY, Long.MAX_VALUE, 0);
            assertEquals(value.basename(), B1HandoffGenerationName.parse(value.basename()).basename());
            var tmp = B1HandoffGenerationName.parse(value.temporaryName().basename());
            assertTrue(tmp.isTemporary()); assertEquals(value.temporaryName().basename(), tmp.basename());
            assertThrows(IOException.class, () -> tmp.requireCanonical(f, KEY, Long.MAX_VALUE, attempt ? -1 : 0));
        }
    }
    @Test void fixedNamesDoNotContainProducerPidUuidOrObserverFields() {
        var r = B1HandoffGenerationName.transfer(B1HandoffGenerationName.Family.TARGET_RESULT, KEY, 7, 12);
        assertEquals(".floatingpod-b1-handoff-transfer-result-v3-" + KEY +
            "-00000000000000000007-00000000000000000012.bin", r.basename());
        assertEquals(".floatingpod-b1-handoff-transfer-retirement-v4-" + KEY +
            "-00000000000000000007-00000000000000000012.bin",
            B1HandoffGenerationName.transfer(B1HandoffGenerationName.Family.TARGET_RETIREMENT, KEY, 7, 12).basename());
    }
    @Test void foreignMalformedOverflowAndGenerationGapsAreNotSilentlyAccepted() throws Exception {
        String canonical = B1HandoffGenerationName.attempt(B1HandoffGenerationName.Family.PLAN, KEY, 0).basename();
        assertThrows(IOException.class, () -> B1HandoffGenerationName.parse(canonical.replace(KEY, KEY.toUpperCase())));
        assertThrows(IOException.class, () -> B1HandoffGenerationName.parse(canonical.replace("00000000000000000000", "09223372036854775808")));
        assertThrows(IOException.class, () -> B1HandoffGenerationName.parse(canonical.replace("plan-v2", "plan-v9")));
        assertThrows(IOException.class, () -> B1HandoffGenerationName.parse(canonical + ".tmp.tmp"));
        assertThrows(IOException.class, () -> B1HandoffGenerationName.parse("../" + canonical));
        assertThrows(IOException.class, () -> B1HandoffGenerationName.parse(canonical).requireCanonical(
            B1HandoffGenerationName.Family.PLAN, KEY, 1, -1));
        assertEquals(1, B1HandoffGenerationName.checkedNext(0));
        assertThrows(ArithmeticException.class, () -> B1HandoffGenerationName.checkedNext(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> B1HandoffGenerationName.checkedNext(-1));
    }
}
