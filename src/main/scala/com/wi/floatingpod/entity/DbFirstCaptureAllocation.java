package com.wi.floatingpod.entity;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.IOException;

/** 仅一次 Main capture 的实际 shared-ledger 租约；不是 SQL/plan 资格。 */
public interface DbFirstCaptureAllocation {
    SourceLimitProfile profile();
    void beginCapture() throws IOException;
    void reserveRetained(long bytes, String responsibility) throws IOException;
    void releaseRetained(long bytes, String responsibility) throws IOException;
    void reserveTemporary(long bytes, String responsibility) throws IOException;
    void releaseTemporary(long bytes, String responsibility) throws IOException;
    /** 移交后即使 callback 随后失败，原 owner 也必须保存快照及其费用。 */
    void retainSnapshot(DbFirstPodSnapshot snapshot) throws IOException;
}
