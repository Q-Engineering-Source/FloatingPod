package com.wi.floatingpod.storage.mysql;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MySqlSessionBarrierNamespaceTest {
    private static final UUID SERVER = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d");
    private static final UUID PROVISIONING = UUID.fromString("16fb58b8-a867-43b7-8e8e-03554578bf14");
    private static final UUID BINDING = UUID.fromString("e83a7eb6-c7fd-4094-9751-029bdef5c0cf");
    private static String name(String schema) {
        return MySqlSessionBarrierNamespace.forIdentity(SERVER, schema, PROVISIONING, BINDING).lockName();
    }
    @Test void fixedCompleteSha256VectorHas48AsciiCharacters() {
        String actual = name("fp_test_20261007_sec07");
        assertEquals("fpb1:3pbJPAdNw4_3Swu79aHQtJ-AvQwce1Qsw_yO4KsoMkY", actual);
        assertEquals(48, actual.length());
        assertTrue(actual.chars().allMatch(c -> c <= 127));
    }
    @Test void uppercaseAndLeadingDigitLegalPlanIdentitiesNeverAlias() {
        assertEquals("fpb1:GxZBCUhauNYjwwP8htR-D_tdXEdld410RPoGrzvJQK8", name("FP_test_20261007_sec07"));
        assertEquals("fpb1:ePLfDP6UwOaBfGZVRm0nVA1wnWEWGCzlPuE2HceuQS8", name("7_source"));
        assertNotEquals(name("fp_test_20261007_sec07"), name("FP_test_20261007_sec07"));
    }
    @Test void rejectsNonAsciiAndInvalidPlanSchemaWithoutNormalization() {
        for (String schema : new String[]{"", "世界", "x-y", " x", "x ", "a".repeat(65)}) {
            assertThrows(IllegalArgumentException.class, () -> name(schema));
        }
        assertThrows(NullPointerException.class,
            () -> MySqlSessionBarrierNamespace.forIdentity(null, "x", PROVISIONING, BINDING));
    }
    @Test void everyUuidFieldChangesTheNamespaceAndSameIdentityIsStable() {
        UUID other = UUID.fromString("00000000-0000-0000-0000-000000000001");
        String baseline = name("x");
        assertEquals(baseline, name("x"));
        assertNotEquals(baseline, MySqlSessionBarrierNamespace.forIdentity(other, "x", PROVISIONING, BINDING).lockName());
        assertNotEquals(baseline, MySqlSessionBarrierNamespace.forIdentity(SERVER, "x", other, BINDING).lockName());
        assertNotEquals(baseline, MySqlSessionBarrierNamespace.forIdentity(SERVER, "x", PROVISIONING, other).lockName());
    }
}
