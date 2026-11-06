package com.wi.floatingpod.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WindowsMarkerIoPathAdapterTest {
    private static String rootedPathWithLength(String root, int targetLength) {
        if (root.length() >= targetLength || !root.endsWith("\\")) throw new IllegalArgumentException("Exact rooted path length required");
        StringBuilder value = new StringBuilder(root);
        while (value.length() < targetLength) {
            int remaining = targetLength - value.length();
            value.append("x".repeat(Math.min(100, remaining)));
            if (value.length() < targetLength) value.append('\\');
        }
        return value.toString();
    }

    @Test void mapsAbsoluteWin32PathFormsAtTheMaxPathBoundary() {
        String drive259 = rootedPathWithLength("C:\\", 259);
        String drive260 = rootedPathWithLength("C:\\", 260);
        String uncRoot = "\\\\server\\share\\";
        String unc260 = rootedPathWithLength(uncRoot, 260);
        String directory247 = rootedPathWithLength("C:\\", 247);
        String directory248 = rootedPathWithLength("C:\\", 248);

        assertEquals(drive259, WindowsMarkerIo.nativePathArgument(drive259));
        assertEquals("\\\\?\\" + drive260, WindowsMarkerIo.nativePathArgument(drive260));
        assertEquals("\\\\?\\UNC\\" + unc260.substring(2), WindowsMarkerIo.nativePathArgument(unc260));
        assertEquals(directory247, WindowsMarkerIo.nativeDirectoryPathArgument(directory247));
        assertEquals("\\\\?\\" + directory248, WindowsMarkerIo.nativeDirectoryPathArgument(directory248));

        String extendedDrive = "\\\\?\\C:\\already-extended";
        String extendedUnc = "\\\\?\\UNC\\server\\share\\already-extended";
        String extendedVolume = "\\\\?\\Volume{01234567-89ab-cdef-0123-456789abcdef}\\world";
        assertEquals(extendedDrive, WindowsMarkerIo.nativePathArgument(extendedDrive));
        assertEquals(extendedUnc, WindowsMarkerIo.nativePathArgument(extendedUnc));
        assertEquals(extendedVolume, WindowsMarkerIo.nativePathArgument(extendedVolume));

        assertThrows(IllegalArgumentException.class, () -> WindowsMarkerIo.nativePathArgument("relative\\world"));
        assertThrows(IllegalArgumentException.class, () -> WindowsMarkerIo.nativePathArgument("\\\\.\\PhysicalDrive0"));
        assertThrows(IllegalArgumentException.class, () -> WindowsMarkerIo.nativePathArgument("\\\\server\\"));
    }
}
