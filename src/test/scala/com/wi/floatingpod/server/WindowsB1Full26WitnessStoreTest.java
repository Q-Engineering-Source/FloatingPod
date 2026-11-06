package com.wi.floatingpod.server;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 原生产pre-OPEN长度边界；无native holder、文件或运行权限。 */
class WindowsB1Full26WitnessStoreTest {
    @Test void actualNativeArtifactBoundRejectsEmptyProfileOverflowAndJvmArrayOverflow()throws Exception {
        WindowsB1Full26WitnessStore.requireBodyBound(1L,1L);
        WindowsB1Full26WitnessStore.requireBodyBound(Integer.MAX_VALUE,Integer.MAX_VALUE);
        for(long length:new long[]{0L,-1L,1025L})
            assertThrows(IOException.class,()->WindowsB1Full26WitnessStore.requireBodyBound(length,1024L));
        assertThrows(IOException.class,()->WindowsB1Full26WitnessStore.requireBodyBound((long)Integer.MAX_VALUE+1L,Long.MAX_VALUE));
    }
}
