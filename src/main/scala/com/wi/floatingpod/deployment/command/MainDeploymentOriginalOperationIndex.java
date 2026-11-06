package com.wi.floatingpod.deployment.command;

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.DeployRequest;
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.Request;
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.RetractRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Main 独占的 FPDC 原操作责任索引。调用方先完成认证；UUID 和阶段通知本身不发行权限。
 * 本类不访问数据库、不签发 receipt、不执行 effect；成功仅表示本地责任与预算转换成功。
 */
public final class MainDeploymentOriginalOperationIndex {
    public record Limits(int lookups, int live, int pending, int queue) {
        public Limits {
            if (lookups <= 0 || live <= 0 || pending <= 0 || queue <= 0
                    || pending > live || queue > live) {
                throw new IllegalArgumentException("positive bounded lookup/live/pending/queue limits required");
            }
        }
    }

    public enum Stage {
        LOOKING_UP, ABSENT, REGISTER_RESERVED, REGISTERING, REGISTER_UNCERTAIN,
        QUEUED, EXECUTING, RECONCILING, RESOLVED, REFUSED, SETTLED
    }

    public enum AdmissionKind { NEW_LOOKUP, EXISTING, CONFLICT, CAPACITY, RETIRED }
    public enum LookupMatch { EQUAL, CONFLICT }

    public record Admission(AdmissionKind kind, Entry entry) {
        private static Admission withoutEntry(AdmissionKind kind) { return new Admission(kind, null); }
    }

    public record Counts(int entries, int lookups, int live, int pending, int queueReservations, int queued) {}

    /** 构造权仅在该 index；原 request 是 r03 的不可变真实类型，不复制 wire 模型。 */
    public final class Entry {
        private final UUID actor;
        private final UUID operation;
        private final Request request;
        private Stage stage = Stage.LOOKING_UP;
        private Throwable cause;
        private boolean dispatched;

        private Entry(UUID actor, UUID operation, Request request) {
            this.actor = actor;
            this.operation = operation;
            this.request = request;
        }

        public UUID actor() { owner(); return actor; }
        public UUID operation() { owner(); return operation; }
        public Request request() { owner(); return request; }
        public Stage stage() { owner(); return stage; }
        public Throwable cause() { owner(); return cause; }
        public boolean dispatched() { owner(); return dispatched; }
    }

    private final Thread main;
    private final Limits limits;
    // 该实例只服务固定 FPDC namespace；其他命令族不得共用或改写此索引。
    private final Map<UUID, Entry> entries = new LinkedHashMap<>();
    private int lookups;
    private int live;
    private int pending;
    private int queueReservations;
    private int queued;
    private boolean retired;

    public MainDeploymentOriginalOperationIndex(Thread main, Limits limits) {
        this.main = Objects.requireNonNull(main, "actual Main thread");
        this.limits = Objects.requireNonNull(limits, "explicit limits");
        owner();
    }

    /** 先命中原 entry，再检查新 lookup 容量；exact 重送永不取得第二份提交机会。 */
    public Admission begin(UUID actor, Request request) {
        owner();
        Objects.requireNonNull(actor, "trusted actor UUID");
        UUID operation = mutationId(request);
        Entry original = entries.get(operation);
        if (original != null) {
            return same(original, actor, request)
                    ? new Admission(AdmissionKind.EXISTING, original)
                    : new Admission(AdmissionKind.CONFLICT, original);
        }
        if (retired) return Admission.withoutEntry(AdmissionKind.RETIRED);
        if (lookups == limits.lookups()) return Admission.withoutEntry(AdmissionKind.CAPACITY);
        Entry entry = new Entry(actor, operation, request);
        entries.put(operation, entry);
        lookups++;
        return new Admission(AdmissionKind.NEW_LOOKUP, entry);
    }

    /**
     * 只观察本索引仍保管的同 actor 原责任；pending 不限定阶段，退休和失败不隐藏原 entry。
     * 不接纳查询、不转换预算，也不发行数据库结果、receipt 或执行权限。
     */
    public Optional<Entry> observePending(UUID actor, UUID operationId) {
        owner();
        Objects.requireNonNull(actor, "trusted actor UUID");
        Objects.requireNonNull(operationId, "original operation UUID");
        Entry original = entries.get(operationId);
        return original != null && original.actor.equals(actor) ? Optional.of(original) : Optional.empty();
    }

    /** 只接收原只读查询的 identity 数据；结果相等不表示注册、授权或可执行。 */
    public LookupMatch found(Entry entry, UUID storedActor, Request storedRequest) {
        forward(entry, Stage.LOOKING_UP);
        Objects.requireNonNull(storedActor, "stored actor");
        if (!entry.operation.equals(mutationId(storedRequest))) {
            throw new IllegalArgumentException("lookup returned a foreign operation");
        }
        LookupMatch result = same(entry, storedActor, storedRequest) ? LookupMatch.EQUAL : LookupMatch.CONFLICT;
        lookups--;
        entry.stage = Stage.RESOLVED;
        entries.remove(entry.operation);
        return result;
    }

    public void absent(Entry entry) {
        forward(entry, Stage.LOOKING_UP);
        entry.stage = Stage.ABSENT;
    }

    /**
     * absent 后原子转换预算。预留未来 queue 位置，避免登记成功后才发现无处保管。
     * 不足时仅释放未 claim 的 provisional lookup，绝不消费 register 机会。
     */
    public boolean reserveRegistration(Entry entry) {
        forward(entry, Stage.ABSENT);
        lookups--;
        if (live == limits.live() || pending == limits.pending() || queueReservations == limits.queue()) {
            entry.stage = Stage.REFUSED;
            entries.remove(entry.operation);
            return false;
        }
        live++;
        pending++;
        queueReservations++;
        entry.stage = Stage.REGISTER_RESERVED;
        return true;
    }

    /** 调用方在首次实际提交时通知；不能拿该本地阶段当数据库接纳或 commit 证明。 */
    public void registerSubmitted(Entry entry) {
        forward(entry, Stage.REGISTER_RESERVED);
        entry.stage = Stage.REGISTERING;
    }

    /** 只有调用方已消费原 register 的确定确认才调用；不允许 uncertain 经此恢复 runnable。 */
    public void registrationConfirmed(Entry entry) {
        forward(entry, Stage.REGISTERING);
        pending--;
        queued++;
        entry.stage = Stage.QUEUED;
    }

    /** 确定未 claim 的提交拒绝；未知结果必须走 registerUncertain，不能释放原责任。 */
    public void registrationNotAccepted(Entry entry) {
        forward(entry, Stage.REGISTER_RESERVED, Stage.REGISTERING);
        pending--;
        live--;
        queueReservations--;
        entry.stage = Stage.REFUSED;
        entries.remove(entry.operation);
    }

    public void registerUncertain(Entry entry, Throwable cause) {
        owner();
        owned(entry);
        requireStage(entry, Stage.REGISTERING);
        retainCause(entry, cause);
        entry.stage = Stage.REGISTER_UNCERTAIN;
    }

    /** 未知登记只能移交原 reconcile 保管；此类没有从该阶段回到 QUEUED 的入口。 */
    public void beginReconciliation(Entry entry) {
        owner();
        owned(entry);
        requireStage(entry, Stage.REGISTER_UNCERTAIN);
        entry.stage = Stage.RECONCILING;
    }

    /** 实际持久 start ACK 和 fresh capture 的核验由调用方承担；这里只转换本地责任。 */
    public void startConfirmed(Entry entry) {
        forward(entry, Stage.QUEUED);
        queued--;
        queueReservations--;
        entry.stage = Stage.EXECUTING;
    }

    /** 消费本 entry 唯一 dispatch 机会，不执行 effect，也不代替调用方的即时资格重验。 */
    public void claimDispatch(Entry entry) {
        forward(entry, Stage.EXECUTING);
        if (entry.dispatched) throw new IllegalStateException("original dispatch opportunity already consumed");
        entry.dispatched = true;
    }

    /** 调用方已完成持久结果及责任移交后卸载 terminal；不对 uncertain/failure 开放。 */
    public void releaseSettled(Entry entry) {
        forward(entry, Stage.EXECUTING);
        if (!entry.dispatched) throw new IllegalStateException("undispatched operation cannot settle as executed");
        live--;
        entry.stage = Stage.SETTLED;
        entries.remove(entry.operation);
    }

    public void retainFailure(Entry entry, Throwable cause) {
        owner();
        owned(entry);
        retainCause(entry, cause);
    }

    /** 退休只封新工作，不删除 entry、释放预算或清除原 cause；晚到责任仍可保管。 */
    public void retire() { owner(); retired = true; }
    public boolean retired() { owner(); return retired; }
    public Counts counts() { owner(); return new Counts(entries.size(), lookups, live, pending, queueReservations, queued); }

    private void forward(Entry entry, Stage... allowed) {
        owner();
        owned(entry);
        if (retired) throw new IllegalStateException("index retired; original responsibilities retained");
        if (entry.cause != null) throw new IllegalStateException("original operation failed; responsibility retained", entry.cause);
        requireStage(entry, allowed);
    }

    private void requireStage(Entry entry, Stage... allowed) {
        for (Stage candidate : allowed) if (entry.stage == candidate) return;
        throw new IllegalStateException("unexpected original operation stage: " + entry.stage);
    }

    private void owned(Entry entry) {
        if (entry == null || entries.get(entry.operation) != entry) {
            throw new IllegalArgumentException("foreign or released original entry");
        }
    }

    private void retainCause(Entry entry, Throwable cause) {
        Objects.requireNonNull(cause, "original failure");
        if (entry.cause == null) entry.cause = cause;
        else if (entry.cause != cause) {
            for (Throwable existing : entry.cause.getSuppressed()) if (existing == cause) return;
            entry.cause.addSuppressed(cause);
        }
    }

    private boolean same(Entry entry, UUID actor, Request request) {
        return entry.actor.equals(actor) && entry.request.equals(request);
    }

    private static UUID mutationId(Request request) {
        Objects.requireNonNull(request, "original r03 mutation request");
        return switch (request) {
            case DeployRequest deploy -> deploy.operationId();
            case RetractRequest retract -> retract.operationId();
            default -> throw new IllegalArgumentException("query is read-only and cannot acquire a mutation entry");
        };
    }

    private void owner() {
        if (Thread.currentThread() != main) throw new IllegalStateException("index requires its original Main thread");
    }
}
