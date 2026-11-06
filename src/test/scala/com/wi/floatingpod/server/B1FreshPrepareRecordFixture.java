package com.wi.floatingpod.server;

/** 仅测试：复用合法原 receipt，容量失败必须到完整 record 总容量门。 */
public final class B1FreshPrepareRecordFixture {
    private B1FreshPrepareRecordFixture() { }

    public static B1ResumeRecord validRecord() throws Exception {
        return B1TwoSlotFixture.record(0, new byte[32], B1ResumeRecord.Kind.GENESIS,
                B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner()));
    }

    public static B1ResumeRecord oversizedValidRecord() throws Exception {
        var state = B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner());
        var a = state.pendingAttempt();
        var graph = new B1ResumeRecord.CommonGraphAssertions("TEST_ONLY_COMPLETE_MANIFEST", new byte[32000],
                "TEST_ONLY_COMPLETE_BINDING", new byte[32000], "TEST_ONLY_COMPLETE_REPOSITORY", new byte[32000]);
        var enlarged = new B1ResumeRecord.AttemptAssertions(a.attemptId(), a.phase(), a.logicalWriteUuid(),
                a.bootGeneration(), a.connectionGeneration(), a.connectionId(), a.namespace(), a.context(),
                a.beforeWriter(), a.targetWriter(), graph, graph);
        var full = new B1ResumeRecord.FullStateAssertions(state.currentOwner(), null, null, null,
                state.currentWriter(), state.targetWriter(), enlarged, null, null, null);
        return B1TwoSlotFixture.record(0, new byte[32], B1ResumeRecord.Kind.GENESIS, full);
    }
}
