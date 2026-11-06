package com.wi.floatingpod.server;

import java.io.IOException;

/** 跨包的完整 record 编码预检；不发行耐久、SQL 或恢复能力。 */
public final class B1ResumeRecordCanonical {
    private B1ResumeRecordCanonical() { }

    /** 文件输入只解为 DATA，不签发任何 current/native/SQL scope。 */
    public static B1ResumeRecord decodeData(byte[] canonical) throws IOException {
        return B1ResumeRecordCodec.decode(canonical);
    }
    /** 完整 attempt DATA 的唯一原始 framing；不发行任何执行或恢复资格。 */
    public static byte[] encodeAttemptData(B1ResumeRecord.AttemptAssertions attempt) throws IOException {
        return B1ResumeRecordCodec.attemptBytes(attempt);
    }
    public static byte[] encode(B1ResumeRecord record) throws IOException {
        return B1ResumeRecordCodec.encode(record);
    }
    /** 专用 schema5/Full26 结构 DATA；不签 native、SQL 或恢复资格。 */
    public static byte[] encodeSchema5Data(B1ResumeRecord record) throws IOException {
        return B1ResumeRecordCodec.encodeSchema5Data(record);
    }
    public static B1ResumeRecord decodeSchema5Data(byte[] canonical) throws IOException {
        return B1ResumeRecordCodec.decodeSchema5Data(canonical);
    }
}
