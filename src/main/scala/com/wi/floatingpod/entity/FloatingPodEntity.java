package com.wi.floatingpod.entity;

import com.wi.floatingpod.server.DbFirstBootActiveRecovery;
import net.minecraft.entity.Entity;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.util.EnumHand;
import net.minecraft.util.DamageSource;
import com.wi.floatingpod.debug.DebugItemInteractions;
import net.minecraft.entity.MoverType;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.block.material.EnumPushReaction;
import net.minecraft.network.datasync.DataParameter;
import net.minecraft.network.datasync.DataSerializers;
import net.minecraft.network.datasync.EntityDataManager;
import net.minecraft.world.World;
import net.minecraft.world.WorldServer;
import com.wi.floatingpod.vendored.geckolib3.core.IAnimatable;
import com.wi.floatingpod.vendored.geckolib3.core.PlayState;
import com.wi.floatingpod.vendored.geckolib3.core.builder.AnimationBuilder;
import com.wi.floatingpod.vendored.geckolib3.core.controller.AnimationController;
import com.wi.floatingpod.vendored.geckolib3.core.event.predicate.AnimationEvent;
import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationData;
import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.Objects;
import java.util.Optional;

/**
 * 浮游辅助单元的世界实体本体。
 *
 * <p>本类位于 common 侧，只依赖 vanilla {@link Entity} 与 GeckoLib 的 {@code core} 包。
 * {@code core} 包内的动画类型不引用任何 {@code net.minecraft.client} 符号，因此专用服务端加载本类
 * 不会牵出 Gecko 客户端类，这是 {@code doc/01-technical-baseline.md} 门禁 3
 * 「服务端路径不得加载 Gecko 客户端类」的结构前提。
 *
 * <p>本类刻意不持有渲染器、模型或贴图引用。模型与渲染只在客户端 proxy 装配，
 * 与 {@code doc/02-architecture.md}「该层不依赖客户端渲染类」一致。
 */
public class FloatingPodEntity extends Entity implements IAnimatable {

    /** 动画控制器名称。GeckoLib 以该名称作为控制器在 {@link AnimationData} 中的键。 */
    private static final String ANIMATION_CONTROLLER_NAME = "floatingpod_main_controller";

    /**
     * 待机动画名称，必须与 {@code animations/general_purpose_support_drone_001.animation.json}
     * 中的动画键完全一致；不一致时 GeckoLib 会在客户端报告找不到该动画。
     */
    private static final String IDLE_ANIMATION_NAME = "idle";

    /** 动画间过渡时长，单位为 tick。当前只有一条待机动画，过渡时长不影响表现。 */
    private static final float ANIMATION_TRANSITION_TICKS = 5.0F;

    /**
     * 实体碰撞箱边长。当前取 1.0，仅用于让实体在世界中具备可选中的体积；
     * {@code doc/} 尚未规定辅助机的碰撞尺寸，最终值待相应切片确认。
     */
    private static final float BOUNDING_BOX_SIZE = 1.0F;

    /** 新实体和尚未确认控制态的客户端镜像均冻结；服务端提交后仍由领域 mode 决定该值。 */
    private static final DataParameter<Boolean> FROZEN = EntityDataManager.createKey(FloatingPodEntity.class, DataSerializers.BOOLEAN);

    /**
     * GeckoLib 的动画数据工厂。每个实体实例持有自己的工厂，
     * 由 GeckoLib 在渲染帧中通过 {@link #getFactory()} 取出对应的 {@link AnimationData}。
     */
    private final AnimationFactory animationFactory = new AnimationFactory(this);

    /** 同一运行期受控创建收据核对字段；它们不是 NBT 或领域权威。 */
    private UUID controlledBindingUuid;
    private UUID controlledAttemptUuid;
    private UUID controlledOperationUuid;
    /** 仅运行期保留；证明此本次启动新建实体来自schema6数据库artifact。 */
    private PodArtifactIdentity dbFirstRecoveryControlArtifactIdentity;

    /** 仅物理来源指纹；标签不证明数据库提交，也不恢复领域状态。 */
    private PodArtifactIdentity artifactIdentity;
    private PodDiagnosticIdentity diagnosticIdentity;
    private boolean dbFirstRecoveryRead, dbFirstRecoveryReadAttempted, nbtReadInProgress;

    public final Optional<PodDiagnosticIdentity> diagnosticIdentity() {
        return Optional.ofNullable(diagnosticIdentity);
    }

    /** 仅供原世界观察定位旧壳；业务/world索引仍使用getUniqueID的实际物理UUID。 */
    public final UUID sourcePodUuid() {
        return diagnosticIdentity == null ? getUniqueID() : diagnosticIdentity.podUuid();
    }

    public final Optional<PodArtifactIdentity> artifactIdentity() {
        return Optional.ofNullable(artifactIdentity);
    }

    /** 真实 durable prepare 后由服务端 Main 安装；同一实体不能改写来源。 */
    public final void installArtifactIdentity(PodArtifactIdentity identity) {
        Objects.requireNonNull(identity, "Artifact identity must not be null");
        if (diagnosticIdentity != null) throw new IllegalStateException("World diagnostic body cannot acquire business artifact authority");
        if (world.isRemote || world.getMinecraftServer() == null
                || !world.getMinecraftServer().isCallingFromMinecraftThread()) {
            throw new IllegalStateException("Artifact identity installation requires logical server Main");
        }
        if (artifactIdentity != null && !artifactIdentity.equals(identity)) {
            throw new IllegalStateException("Artifact identity cannot be replaced");
        }
        artifactIdentity = identity;
    }

    /** 服务端权威控制快照；客户端只持有 FROZEN 的派生镜像。 */
    private FloatingPodControlSnapshot serverControlSnapshot;
    // 临时操作冻结不修改已提交mode/version；失败/Unknown保留此责任。
    private UUID pendingModeOperation;
    private long pendingModeVersion;
    private Thread pendingModeOwner;
    private FloatingPodControlSnapshot pendingModeSourceControl;
    private NormalStopEntityFreeze normalStopFreeze;
    private DbFirstRecallFreeze dbFirstRecallFreeze;
    private DbFirstCheckpointFreeze dbFirstCheckpointFreeze;


    /**
     * 供 Forge 实体注册与世界反序列化使用的构造器。
     *
     * @param world 实体所属世界；服务端与客户端各自持有对应实例。
     */
    public FloatingPodEntity(World world) {
        super(world);
        setSize(BOUNDING_BOX_SIZE, BOUNDING_BOX_SIZE);
    }

    /**
     * 注册从服务端不可变控制快照派生的冻结镜像。新建和尚未确认的客户端实体默认冻结。
     */
    @Override
    protected void entityInit() {
        dataManager.register(FROZEN, Boolean.TRUE);
    }

    /** 仅补 vanilla ray picking，不改变 push、伤害或生命周期规则。 */
    @Override
    public boolean canBeCollidedWith() {
        return !isDead;
    }

    /** 非生物 Entity 的唯一调试业务派发；specific interaction 维持父类 PASS。 */
    @Override
    public boolean processInitialInteract(EntityPlayer player, EnumHand hand) {
        if (DebugItemInteractions.holdsTool(player, hand)) {
            return DebugItemInteractions.handlePodInteract(this, player, hand);
        }
        if (!world.isRemote && !acceptsBusinessInteraction()) {
            return false;
        }
        return super.processInitialInteract(player, hand);
    }

    /** 原操作未完成或控制态未获确认时，不能从另一个交互入口重新取得业务资格。 */
    public final boolean acceptsBusinessInteraction() {
        if (world.isRemote) {
            throw new IllegalStateException("Business admission requires the logical server");
        }
        return diagnosticIdentity == null && !isDead && normalStopFreeze == null && dbFirstRecallFreeze == null && dbFirstCheckpointFreeze == null && pendingModeOperation == null
                && serverControlSnapshot != null
                && serverControlSnapshot.phase() == FloatingPodControlSnapshot.Phase.COMMITTED;
    }

    /** 只有未获业务控制且没有未决责任的旧壳可沿世界保存；故障/未决活动体不能借此降级。 */
    public final void requireInertDiagnosticSave() {
        if (world.isRemote || world.getMinecraftServer()==null || !world.getMinecraftServer().isCallingFromMinecraftThread()
                || isDead || world.getEntityByID(getEntityId())!=this || serverControlSnapshot!=null
                || controlledOperationUuid!=null || normalStopFreeze!=null || dbFirstRecallFreeze!=null
                || dbFirstCheckpointFreeze!=null || pendingModeOperation!=null || !isFrozen())
            throw new IllegalStateException("Only actual unadmitted inert diagnostic FloatingPod may enter final save");
    }

    public final boolean hasPendingBusinessOperation() {
        if (world.isRemote) {
            throw new IllegalStateException("Pending operation observation requires the logical server");
        }
        return normalStopFreeze != null || dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null || pendingModeOperation != null
                || (serverControlSnapshot != null
                    && serverControlSnapshot.phase() == FloatingPodControlSnapshot.Phase.PENDING_INITIALIZATION);
    }

    /** 冻结上传/未决/故障态在伤害入口就拒绝，不依赖后续伤害处理能否恢复状态。 */
    @Override
    public boolean isEntityInvulnerable(DamageSource source) {
        return isFrozen() || super.isEntityInvulnerable(source);
    }

    @Override
    public boolean canBeAttackedWithItem() {
        return !isFrozen() && super.canBeAttackedWithItem();
    }

    @Override
    public boolean attackEntityFrom(DamageSource source, float amount) {
        return !isFrozen() && super.attackEntityFrom(source, amount);
    }

    /** 服务端权威冻结镜像；客户端只能接收 EntityDataManager 同步。 */
    public final boolean isFrozen() {
        if (world.isRemote) {
            return dataManager.get(FROZEN);
        }
        FloatingPodControlSnapshot snapshot = serverControlSnapshot;
        return normalStopFreeze != null || dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null || pendingModeOperation != null || snapshot == null || snapshot.frozen();
    }

    public final FloatingPodControlSnapshot serverControlSnapshot() {
        if (world.isRemote) {
            throw new IllegalStateException("Server control snapshot is unavailable on the logical client");
        }
        return serverControlSnapshot;
    }

    private void updateFrozenMirrorFromServer() {
        if (world.isRemote) {
            throw new IllegalStateException("Only the logical server may update the FloatingPod frozen mirror");
        }
        dataManager.set(FROZEN, normalStopFreeze != null || dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null || pendingModeOperation != null || serverControlSnapshot == null || serverControlSnapshot.frozen());
    }

    public final void freezeForNormalStop(UUID request) {
        requireModeMain();
        if (dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null) throw new IllegalStateException("DB-first snapshot retains this body");
        if (isDead) throw new IllegalStateException("Normal stop cannot freeze a dead body");
        if (normalStopFreeze == null) normalStopFreeze = new NormalStopEntityFreeze(request, serverControlSnapshot);
        normalStopFreeze.requireCurrent(request, serverControlSnapshot);
        motionX = motionY = motionZ = 0.0D;
        updateFrozenMirrorFromServer();
    }

    public final void requireNormalStopFreeze(UUID request) {
        requireModeMain();
        if (isDead || normalStopFreeze == null) throw new IllegalStateException("Original normal stop freeze is absent");
        normalStopFreeze.requireCurrent(request, serverControlSnapshot);
    }

    /** 在 spawnEntity 前安装不可变 PREPARED 控制快照并记录该次创建身份。 */
    public final void initializeControlledCreation(FloatingPodControlSnapshot snapshot) {
        if (diagnosticIdentity != null) throw new IllegalStateException("Diagnostic body cannot initialize active business");
        if (dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null) throw new IllegalStateException("DB-first snapshot control cannot be overwritten");
        if (snapshot == null || snapshot.phase() != FloatingPodControlSnapshot.Phase.PENDING_INITIALIZATION
                || snapshot.bindingUuid() == null || snapshot.attemptUuid() == null || snapshot.operationUuid() == null) {
            throw new IllegalArgumentException("Pending controlled creation snapshot must carry exact receipt identity");
        }
        if (world.isRemote) {
            throw new IllegalStateException("Only the logical server may initialize a controlled FloatingPod creation");
        }
        if (!getUniqueID().equals(snapshot.podUuid())) {
            throw new IllegalArgumentException("Pending control snapshot UUID does not match the entity");
        }
        if (snapshot.dimensionId() != world.provider.getDimension()
                || !world.getWorldInfo().getWorldName().equals(snapshot.worldName())
                || posX != snapshot.posX() || posY != snapshot.posY() || posZ != snapshot.posZ()
                || rotationYaw != snapshot.yaw() || rotationPitch != snapshot.pitch()) {
            throw new IllegalArgumentException("Pending control snapshot does not match the pre-publish entity pose");
        }
        if (serverControlSnapshot != null || controlledOperationUuid != null) {
            throw new IllegalStateException("FloatingPod already has a controlled creation identity");
        }
        controlledBindingUuid = snapshot.bindingUuid();
        controlledAttemptUuid = snapshot.attemptUuid();
        controlledOperationUuid = snapshot.operationUuid();
        serverControlSnapshot = snapshot;
        updateFrozenMirrorFromServer();
    }

    public final boolean matchesControlledCreation(UUID bindingUuid, UUID attemptUuid, UUID operationUuid) {
        return bindingUuid != null && attemptUuid != null && operationUuid != null
                && bindingUuid.equals(controlledBindingUuid)
                && attemptUuid.equals(controlledAttemptUuid)
                && operationUuid.equals(controlledOperationUuid);
    }

    /** 安装schema6数据库路径恢复的新实体来源；普通NBT和调用者给出的UUID组合不能建立此运行期来源。 */
    public final void applyCommittedDbFirstRecoveryControl(FloatingPodControlSnapshot snapshot) {
        if (!(world instanceof WorldServer serverWorld) || world.isRemote || world.getMinecraftServer() == null
                || !world.getMinecraftServer().isCallingFromMinecraftThread() || isDead || !addedToChunk
                || world.getEntityByID(getEntityId()) != this || serverWorld.getEntityFromUuid(getUniqueID()) != this
                || diagnosticIdentity != null || !dbFirstRecoveryReadAttempted || dbFirstRecoveryControlArtifactIdentity != null
                || serverControlSnapshot != null || controlledBindingUuid != null || controlledAttemptUuid != null
                || controlledOperationUuid != null || pendingModeOperation != null || normalStopFreeze != null
                || dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null) {
            throw new IllegalStateException("Exact live schema6 DB recovery body/source installation required");
        }
        PodArtifactIdentity actualArtifact = artifactIdentity;
        if (snapshot == null || snapshot.phase() != FloatingPodControlSnapshot.Phase.COMMITTED || !snapshot.active()
                || !getUniqueID().equals(snapshot.podUuid()) || snapshot.bindingUuid() == null
                || snapshot.attemptUuid() == null || snapshot.operationUuid() != null
                || actualArtifact == null || !actualArtifact.bindingUuid().equals(snapshot.bindingUuid())
                || !actualArtifact.attemptUuid().equals(snapshot.attemptUuid())
                || snapshot.dimensionId() != world.provider.getDimension()
                || !world.getWorldInfo().getWorldName().equals(snapshot.worldName())
                || posX != snapshot.posX() || posY != snapshot.posY() || posZ != snapshot.posZ()
                || rotationYaw != snapshot.yaw() || rotationPitch != snapshot.pitch()) {
            throw new IllegalStateException("DB recovery control differs from the actual artifact, binding, attempt, or live body");
        }
        dbFirstRecoveryControlArtifactIdentity = actualArtifact;
        serverControlSnapshot = snapshot;
        updateFrozenMirrorFromServer();
    }

    /** 仅当快照保留真实创建stamp或DB恢复artifact来源时返回true。 */
    public final boolean hasCommittedSourceIdentity(FloatingPodControlSnapshot snapshot) {
        if (snapshot == null || snapshot.phase() != FloatingPodControlSnapshot.Phase.COMMITTED
                || !snapshot.active() || !getUniqueID().equals(snapshot.podUuid())) return false;
        if (controlledOperationUuid != null) {
            return dbFirstRecoveryControlArtifactIdentity == null
                    && matchesControlledCreation(snapshot.bindingUuid(), snapshot.attemptUuid(), snapshot.operationUuid());
        }
        if (controlledBindingUuid != null || controlledAttemptUuid != null) return false;
        if (dbFirstRecoveryControlArtifactIdentity != null) {
            return dbFirstRecoveryControlArtifactIdentity.equals(artifactIdentity)
                    && snapshot.operationUuid() == null
                    && dbFirstRecoveryControlArtifactIdentity.bindingUuid().equals(snapshot.bindingUuid())
                    && dbFirstRecoveryControlArtifactIdentity.attemptUuid().equals(snapshot.attemptUuid());
        }
        return snapshot.bindingUuid() == null && snapshot.operationUuid() == null;
    }

    public final void requireCommittedSourceIdentity() {
        if (serverControlSnapshot == null || !hasCommittedSourceIdentity(serverControlSnapshot))
            throw new IllegalStateException("Actual committed creation or DB recovery source identity is absent");
    }

    /** 精确受控创建提交；只有匹配的 PREPARED receipt 才能产生 committed mode 镜像。 */
    public final void applyCommittedControlledCreation(FloatingPodControlSnapshot snapshot) {
        if (diagnosticIdentity != null) throw new IllegalStateException("Diagnostic body cannot publish active business");
        if (dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null) throw new IllegalStateException("DB-first snapshot control cannot be overwritten");
        if (world.isRemote || snapshot == null || snapshot.phase() != FloatingPodControlSnapshot.Phase.COMMITTED
                || !getUniqueID().equals(snapshot.podUuid()) || snapshot.ownerUuid() != null
                || !"WorldIdle".equals(snapshot.operatingMode()) || !snapshot.active()) {
            throw new IllegalStateException("Committed controlled snapshot is not a valid ownerless WorldIdle creation");
        }
        if (!matchesControlledCreation(snapshot.bindingUuid(), snapshot.attemptUuid(), snapshot.operationUuid())) {
            throw new IllegalStateException("Committed controlled snapshot does not match the exact prepared receipt");
        }
        if (snapshot.dimensionId() != world.provider.getDimension()
                || !world.getWorldInfo().getWorldName().equals(snapshot.worldName())
                || posX != snapshot.posX() || posY != snapshot.posY() || posZ != snapshot.posZ()
                || rotationYaw != snapshot.yaw() || rotationPitch != snapshot.pitch()) {
            throw new IllegalStateException("Committed controlled snapshot does not match the current entity pose");
        }
        serverControlSnapshot = snapshot;
        updateFrozenMirrorFromServer();
    }

    /** manager 以精确活动行和 locator 校验后安装普通 spawn/recovery 的 committed 控制快照。 */
    public final void applyCommittedManagerState(FloatingPodControlSnapshot snapshot) {
        if (diagnosticIdentity != null) throw new IllegalStateException("Diagnostic body cannot publish manager business");
        if (dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null) throw new IllegalStateException("DB-first snapshot control cannot be overwritten");
        if (world.isRemote || snapshot == null || snapshot.phase() != FloatingPodControlSnapshot.Phase.COMMITTED
                || !getUniqueID().equals(snapshot.podUuid()) || !snapshot.active()
                || snapshot.dimensionId() != world.provider.getDimension()) {
            throw new IllegalStateException("Manager control snapshot does not match this active server entity");
        }
        if (!hasCommittedSourceIdentity(snapshot))
            throw new IllegalStateException("Manager state does not preserve the actual committed source identity");
        if (!world.getWorldInfo().getWorldName().equals(snapshot.worldName())
                || posX != snapshot.posX() || posY != snapshot.posY() || posZ != snapshot.posZ()
                || rotationYaw != snapshot.yaw() || rotationPitch != snapshot.pitch())
            throw new IllegalStateException("Manager state does not match the current entity world/pose");
        serverControlSnapshot = snapshot;
        updateFrozenMirrorFromServer();
    }

    /** Main在提交CAS前冻结exact committed body；不预先修改已提交mode。 */
    public final void beginPendingMode(UUID operation, UUID owner, long version) {
        requireModeMain();
        FloatingPodControlSnapshot prior = serverControlSnapshot;
        if (operation == null || owner == null || prior == null || !hasCommittedSourceIdentity(prior)
                || pendingModeOperation != null || dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null
                || prior.phase() != FloatingPodControlSnapshot.Phase.COMMITTED || !prior.active()
                || !owner.equals(prior.ownerUuid()) || version != prior.stateVersion() || isDead)
            throw new IllegalStateException("Mode freeze requires exact healthy committed owner/version/body");
        pendingModeOwner = Thread.currentThread();
        pendingModeOperation = operation;
        pendingModeVersion = version;
        pendingModeSourceControl = prior;
        motionX = motionY = motionZ = 0.0D;
        updateFrozenMirrorFromServer();
    }

    public final boolean hasPendingMode(UUID operation, long version) {
        requireModeMain();
        return operation != null && operation.equals(pendingModeOperation) && pendingModeVersion == version
                && pendingModeOwner == Thread.currentThread();
    }

    /** 完整ACK graph在Main复核后调用；仍核exact操作、版本与实际body/final pose。 */
    public final void finishPendingMode(UUID operation, long expectedVersion, FloatingPodControlSnapshot committed) {
        if (dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null) throw new IllegalStateException("DB-first snapshot control cannot be overwritten");
        requireModeMain();
        if (!hasPendingMode(operation, expectedVersion) || committed == null || isDead
                || pendingModeSourceControl == null || serverControlSnapshot != pendingModeSourceControl
                || committed.phase() != FloatingPodControlSnapshot.Phase.COMMITTED
                || !getUniqueID().equals(committed.podUuid()) || !committed.active()
                || !Objects.equals(committed.bindingUuid(), pendingModeSourceControl.bindingUuid())
                || !Objects.equals(committed.attemptUuid(), pendingModeSourceControl.attemptUuid())
                || !Objects.equals(committed.operationUuid(), pendingModeSourceControl.operationUuid())
                || !hasCommittedSourceIdentity(committed)
                || !Objects.equals(serverControlSnapshot.ownerUuid(), committed.ownerUuid())
                || (committed.stateVersion() != expectedVersion
                    && (expectedVersion == Long.MAX_VALUE || committed.stateVersion() != expectedVersion + 1L))
                || committed.dimensionId() != world.provider.getDimension()
                || !committed.worldName().equals(world.getWorldInfo().getWorldName())
                || committed.posX() != posX || committed.posY() != posY || committed.posZ() != posZ
                || committed.yaw() != rotationYaw || committed.pitch() != rotationPitch)
            throw new IllegalStateException("Mode publication differs from retained operation/body/final pose");
        serverControlSnapshot = committed;
        pendingModeOperation = null;
        pendingModeOwner = null;
        pendingModeSourceControl = null;
        updateFrozenMirrorFromServer();
    }

    /** 服务端内部完整checkpoint；停服捕获必须继续绑定原normal-stop request。 */
    public final void beginDbFirstCheckpoint(UUID operation, long expectedVersion, UUID normalStopRequest) {
        requireModeMain();
        if (isDead || dbFirstRecallFreeze != null || pendingModeOperation != null || serverControlSnapshot == null
                || !getUniqueID().equals(serverControlSnapshot.podUuid()) || artifactIdentity == null)
            throw new IllegalStateException("Checkpoint requires an exclusive original committed body/artifact");
        requireRecallControlBody();
        if (normalStopFreeze != null) normalStopFreeze.requireCurrent(normalStopRequest, serverControlSnapshot);
        else if (normalStopRequest != null) throw new IllegalStateException("Checkpoint cannot invent a normal-stop request");
        if (dbFirstCheckpointFreeze == null) {
            dbFirstCheckpointFreeze = new DbFirstCheckpointFreeze(operation, expectedVersion,
                    serverControlSnapshot, recallPose(), normalStopRequest);
        } else {
            requireDbFirstCheckpoint(operation, expectedVersion);
            if (!Objects.equals(normalStopRequest, dbFirstCheckpointFreeze.normalStopRequest()))
                throw new IllegalStateException("Checkpoint normal-stop responsibility changed");
        }
        motionX = motionY = motionZ = 0.0D;
        updateFrozenMirrorFromServer();
    }

    public final void requireDbFirstCheckpoint(UUID operation, long expectedVersion) {
        requireModeMain();
        if (dbFirstCheckpointFreeze == null || dbFirstRecallFreeze != null || pendingModeOperation != null || isDead
                || serverControlSnapshot == null || artifactIdentity == null || !getUniqueID().equals(serverControlSnapshot.podUuid()))
            throw new IllegalStateException("Original complete checkpoint body/freeze is absent");
        requireRecallControlBody();
        if (normalStopFreeze != null)
            normalStopFreeze.requireCurrent(dbFirstCheckpointFreeze.normalStopRequest(), serverControlSnapshot);
        else if (dbFirstCheckpointFreeze.normalStopRequest() != null)
            throw new IllegalStateException("Checkpoint lost its original normal-stop freeze");
        dbFirstCheckpointFreeze.requireCurrent(operation, expectedVersion, serverControlSnapshot, recallPose());
    }

    /** UNKNOWN或codec失败仍保留原freeze，不改active状态或赋予实体清理资格。 */
    public final DbFirstPodSnapshot captureDbFirstCheckpoint(UUID operation, long expectedVersion,
            DbFirstCaptureAllocation allocation) throws IOException {
        requireDbFirstCheckpoint(operation, expectedVersion);
        World originalWorld = world;
        NBTTagCompound nbt = serializeNBT();
        requireDbFirstCheckpoint(operation, expectedVersion);
        if (world != originalWorld) throw new IllegalStateException("Checkpoint world changed during serialization");
        DbFirstPodSnapshot snapshot = DbFirstPodSnapshot.captureCheckpoint(dbFirstCheckpointFreeze, nbt, allocation);
        requireDbFirstCheckpoint(operation, expectedVersion);
        if (world != originalWorld) throw new IllegalStateException("Checkpoint world changed during capture");
        return snapshot;
    }

    /** Main消费原完整数据库确认后安装同一活动业务的后继；不结束normal-stop冻结。 */
    public final void finishDbFirstCheckpoint(UUID operation, long expectedVersion, FloatingPodControlSnapshot committed) {
        requireDbFirstCheckpoint(operation, expectedVersion);
        DbFirstCheckpointFreeze original = dbFirstCheckpointFreeze;
        original.requireCommittedSuccessor(committed);
        serverControlSnapshot = committed;
        dbFirstCheckpointFreeze = null;
        try { updateFrozenMirrorFromServer(); }
        catch (RuntimeException | Error cause) {
            // 镜像失败不能释放原责任或假装健康；已提交后继本身不能回滚成旧版本。
            dbFirstCheckpointFreeze = original;
            serverControlSnapshot = committed.asFaulted(cause);
            throw cause;
        }
    }

    /** 仅 Main 可取得一次操作的冻结；先保存责任，再发布冻结镜像。 */
    public final void beginDbFirstRecall(UUID operation, UUID authenticatedOwner, long expectedVersion) {
        requireModeMain();
        if (dbFirstRecallFreeze != null) {
            if (dbFirstRecallFreeze.normalStopRequest() != null || dbFirstRecallFreeze.isBootRecovery())
                throw new IllegalStateException("Player recall cannot borrow original normal-stop responsibility");
            dbFirstRecallFreeze.requireCurrent(operation, expectedVersion, serverControlSnapshot);
            if (!Objects.equals(authenticatedOwner, serverControlSnapshot.ownerUuid()))
                throw new IllegalStateException("Recall owner differs from retained authenticated owner");
            requireDbFirstRecall(operation, expectedVersion);
        } else {
            if (isDead || pendingModeOperation != null || dbFirstCheckpointFreeze != null || normalStopFreeze != null
                    || serverControlSnapshot == null || !getUniqueID().equals(serverControlSnapshot.podUuid()))
                throw new IllegalStateException("Recall requires an exclusive healthy original body");
            requireRecallControlBody();
            dbFirstRecallFreeze = new DbFirstRecallFreeze(operation, authenticatedOwner, expectedVersion, serverControlSnapshot);
        }
        motionX = motionY = motionZ = 0.0D;
        updateFrozenMirrorFromServer();
    }

    /** 原Boot专用来源；调用方须先将实际manager Entry交原owner保管，不接受重试。 */
    public final void beginDbFirstRecallForBoot(UUID operation, long expectedVersion,
            DbFirstBootActiveRecovery boot) {
        requireModeMain();
        if (dbFirstRecallFreeze != null || isDead || pendingModeOperation != null || dbFirstCheckpointFreeze != null
                || normalStopFreeze != null || serverControlSnapshot == null
                || !getUniqueID().equals(serverControlSnapshot.podUuid()))
            throw new IllegalStateException("Boot recall requires exclusive healthy original body once");
        requireRecallControlBody();
        dbFirstRecallFreeze = DbFirstRecallFreeze.forBoot(operation, expectedVersion, serverControlSnapshot, boot, this);
        motionX = motionY = motionZ = 0.0D;
        updateFrozenMirrorFromServer();
    }

    /** 原停服census调用；以实体私有normal-stop freeze为资格，不接收authenticated owner。 */
    public final void beginDbFirstRecallForNormalStop(UUID operation, UUID request, long expectedVersion) {
        requireModeMain();
        if (isDead || pendingModeOperation != null || dbFirstCheckpointFreeze != null || normalStopFreeze == null
                || serverControlSnapshot == null || !getUniqueID().equals(serverControlSnapshot.podUuid()))
            throw new IllegalStateException("Shutdown recall requires exclusive original frozen body");
        requireRecallControlBody();
        normalStopFreeze.requireCurrent(request, serverControlSnapshot);
        if (dbFirstRecallFreeze == null)
            dbFirstRecallFreeze = DbFirstRecallFreeze.forNormalStop(operation, request, expectedVersion,
                    serverControlSnapshot, normalStopFreeze);
        else if (!Objects.equals(request, dbFirstRecallFreeze.normalStopRequest()))
            throw new IllegalStateException("Shutdown recall cannot replace original recall request");
        requireDbFirstRecall(operation, expectedVersion);
        motionX = motionY = motionZ = 0.0D;
        updateFrozenMirrorFromServer();
    }

    public final void requireDbFirstRecall(UUID operation, long expectedVersion) {
        requireModeMain();
        if (dbFirstRecallFreeze == null || dbFirstCheckpointFreeze != null || isDead || pendingModeOperation != null
                || serverControlSnapshot == null || !getUniqueID().equals(serverControlSnapshot.podUuid()))
            throw new IllegalStateException("Original DB-first recall freeze/body is absent");
        dbFirstRecallFreeze.requireNormalStopOwner(normalStopFreeze);
        dbFirstRecallFreeze.requireCurrent(operation, expectedVersion, serverControlSnapshot);
        requireRecallControlBody();
    }

    /** 故障诊断不丢原freeze/body；该检查不允许重新capture或直接移除实体/ticket。 */
    public final void requireRetainedDbFirstRecall(UUID operation, long expectedVersion, Throwable cause) {
        requireModeMain();
        if (dbFirstRecallFreeze == null || dbFirstCheckpointFreeze != null || isDead || pendingModeOperation != null
                || serverControlSnapshot == null || !getUniqueID().equals(serverControlSnapshot.podUuid()))
            throw new IllegalStateException("Original retained DB-first recall body is absent");
        dbFirstRecallFreeze.requireNormalStopOwner(normalStopFreeze);
        dbFirstRecallFreeze.requireRetainedFailure(operation, expectedVersion, serverControlSnapshot, cause);
        requireRecallControlBody();
    }

    private void requireRecallControlBody() {
        FloatingPodControlSnapshot control = serverControlSnapshot;
        if (!hasCommittedSourceIdentity(control) || control.dimensionId() != world.provider.getDimension()
                || !control.worldName().equals(world.getWorldInfo().getWorldName())
                || (artifactIdentity != null && (!artifactIdentity.bindingUuid().equals(control.bindingUuid())
                    || !artifactIdentity.attemptUuid().equals(control.attemptUuid()))))
            throw new IllegalStateException("Recall committed world/binding/attempt differs from original body");
    }

    private DbFirstPodSnapshot.Pose recallPose() {
        if (dimension != world.provider.getDimension() || isRiding() || isBeingRidden())
            throw new IllegalStateException("Recall capture requires original dimension and no riding/passengers");
        return new DbFirstPodSnapshot.Pose(world.getWorldInfo().getWorldName(), dimension,
                posX, posY, posZ, rotationYaw, rotationPitch);
    }

    /** 捕获失败仍保留 token；此值不授予提交、恢复或新 spawn 资格。 */
    public final DbFirstPodSnapshot captureDbFirstRecall(UUID operation, long expectedVersion,
                                                        DbFirstCaptureAllocation allocation) throws IOException {
        requireDbFirstRecall(operation, expectedVersion);
        Objects.requireNonNull(allocation, "Recall shared capture allocation");
        World originalWorld = world;
        UUID originalUuid = getUniqueID();
        DbFirstPodSnapshot.Pose originalPose = recallPose();
        FloatingPodControlSnapshot originalControl = serverControlSnapshot;
        NBTTagCompound nbt = serializeNBT();
        requireRecallCaptureBody(operation, expectedVersion, originalWorld, originalUuid, originalPose, originalControl);
        DbFirstPodSnapshot captured = DbFirstPodSnapshot.capture(dbFirstRecallFreeze, originalPose, nbt, allocation);
        requireRecallCaptureBody(operation, expectedVersion, originalWorld, originalUuid, originalPose, originalControl);
        return captured;
    }

    /** 初建事务使用的完整真实实体捕获；保持 pending 冻结，不发布活动资格。 */
    public final DbFirstPodSnapshot captureDbFirstInitialActivation(UUID operation,
            DbFirstCaptureAllocation allocation) throws IOException {
        requireModeMain();
        FloatingPodControlSnapshot pending = serverControlSnapshot;
        World originalWorld = world;
        requireInitialCaptureBody(pending, originalWorld);
        NBTTagCompound nbt = serializeNBT();
        requireInitialCaptureBody(pending, originalWorld);
        DbFirstPodSnapshot captured = DbFirstPodSnapshot.captureInitialActivation(operation, pending,
                recallPose(), nbt, allocation);
        requireInitialCaptureBody(pending, originalWorld);
        return captured;
    }

    private void requireInitialCaptureBody(FloatingPodControlSnapshot pending, World originalWorld) {
        requireModeMain();
        if (pending == null || serverControlSnapshot != pending || world != originalWorld || isDead
                || pending.phase() != FloatingPodControlSnapshot.Phase.PENDING_INITIALIZATION
                || dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null || normalStopFreeze != null || pendingModeOperation != null
                || !getUniqueID().equals(pending.podUuid())
                || !matchesControlledCreation(pending.bindingUuid(), pending.attemptUuid(), pending.operationUuid())
                || artifactIdentity == null || !artifactIdentity.bindingUuid().equals(pending.bindingUuid())
                || !artifactIdentity.attemptUuid().equals(pending.attemptUuid())
                || !recallPose().equals(new DbFirstPodSnapshot.Pose(pending.worldName(), pending.dimensionId(),
                    pending.posX(), pending.posY(), pending.posZ(), pending.yaw(), pending.pitch())))
            throw new IllegalStateException("Initial activation body/control/pose differs from original pending creation");
    }

    private void requireRecallCaptureBody(UUID operation, long expectedVersion, World originalWorld,
            UUID originalUuid, DbFirstPodSnapshot.Pose originalPose, FloatingPodControlSnapshot originalControl) {
        requireDbFirstRecall(operation, expectedVersion);
        if (world != originalWorld || !originalUuid.equals(getUniqueID()) || serverControlSnapshot != originalControl
                || !originalPose.equals(recallPose()))
            throw new IllegalStateException("Recall body identity/world/pose changed during capture");
    }

    private void requireModeMain() {
        if (world.isRemote || world.getMinecraftServer() == null
                || !world.getMinecraftServer().isCallingFromMinecraftThread())
            throw new IllegalStateException("Mode control requires actual server Main thread");
    }

    /** faulted 实体 fail-closed；原始 Throwable 留在 manager/service 的诊断责任链。 */
    public final void markControlFaulted(Throwable cause) {
        if (world.isRemote) {
            throw new IllegalStateException("Only the logical server may fault FloatingPod control state");
        }
        FloatingPodControlSnapshot snapshot = serverControlSnapshot;
        if (snapshot != null) {
            serverControlSnapshot = dbFirstRecallFreeze == null ? snapshot.asFaulted(cause)
                    : dbFirstRecallFreeze.retainFault(snapshot, cause);
        }
        motionX = motionY = motionZ = 0.0D;
        updateFrozenMirrorFromServer();
    }

    @Override
    public EnumPushReaction getPushReaction() {
        return isFrozen() ? EnumPushReaction.IGNORE : super.getPushReaction();
    }

    /** 冻结态拒绝实体移动，并清掉此前的惯性；NBT/领域状态不由客户端参与。 */
    @Override
    public void move(MoverType type, double x, double y, double z) {
        if (isFrozen()) {
            motionX = 0.0D;
            motionY = 0.0D;
            motionZ = 0.0D;
            return;
        }
        super.move(type, x, y, z);
    }

    /**
     * 从 NBT 恢复实体状态。
     *
     * <p>只恢复物理来源指纹与独立诊断体身份。无artifact标签的 legacy 仍无artifact；标签不授予接管资格，
     * 不恢复所有权、模块、业务版本、mode 或运行期 pending 责任。
     *
     * @param compound 世界存档提供的实体标签。
     */
    @Override
    protected void readEntityFromNBT(NBTTagCompound compound) {
        Optional<PodArtifactIdentity> decoded = PodArtifactNbtCodec.read(compound);
        if (artifactIdentity != null) {
            if (decoded.isEmpty() || !artifactIdentity.equals(decoded.get())) {
                throw new IllegalStateException("NBT cannot clear or replace an installed artifact identity");
            }
        } else {
            artifactIdentity = decoded.orElse(null);
        }
        if (!world.isRemote && !dbFirstRecoveryRead) {
            PodDiagnosticIdentity loaded = PodDiagnosticIdentity.fromWorldNbt(compound, getUniqueID());
            if (diagnosticIdentity != null && !diagnosticIdentity.equals(loaded))
                throw new IllegalStateException("World NBT cannot replace original diagnostic identity");
            diagnosticIdentity = loaded;
            setUniqueId(loaded.bodyUuid()); // vanilla已读UUID；世界/数字entity索引此时尚未建立。
        }
    }

    /** 普通NBT加载不能重写活动体/未决操作；校验先于vanilla写UUID/ForgeCaps。 */
    @Override
    public void readFromNBT(NBTTagCompound compound) {
        if (nbtReadInProgress) throw new IllegalStateException("FloatingPod NBT read cannot re-enter original deserialize scope");
        if (!world.isRemote && !dbFirstRecoveryRead && (serverControlSnapshot != null || controlledOperationUuid != null
                || normalStopFreeze != null || dbFirstRecallFreeze != null || dbFirstCheckpointFreeze != null || pendingModeOperation != null))
            throw new IllegalStateException("World NBT cannot overwrite admitted or pending FloatingPod control");
        nbtReadInProgress=true;
        try { super.readFromNBT(compound); }
        finally { nbtReadInProgress=false; }
    }

    /** 仅原Main恢复caller使用；完整数据库NBT保留业务UUID，新体仍冻结，发布另由原manager核验。 */
    public final void deserializeDbFirstRecovery(NBTTagCompound compound) {
        if (!(world instanceof WorldServer serverWorld) || world.isRemote || world.getMinecraftServer()==null || !world.getMinecraftServer().isCallingFromMinecraftThread()
                || dbFirstRecoveryReadAttempted || nbtReadInProgress || diagnosticIdentity!=null || serverControlSnapshot!=null || artifactIdentity!=null
                || world.getEntityByID(getEntityId())==this || serverWorld.getEntityFromUuid(getUniqueID())==this
                || compound.hasKey(PodDiagnosticIdentity.ROOT_KEY))
            throw new IllegalStateException("Exact new unpublished Main DB recovery body required");
        dbFirstRecoveryReadAttempted=true;
        dbFirstRecoveryRead=true;
        try { super.deserializeNBT(compound); }
        finally { dbFirstRecoveryRead=false; }
    }

    /**
     * 将实体状态写入 NBT。
     *
     * <p>只写 artifact/诊断身份，不写 owner、模块、版本、mode、controlled operation 或控制镜像。
     * 外层 vanilla 字段与 ForgeCaps 由各自责任边界保存。
     *
     * @param compound 世界存档提供的实体标签。
     */
    @Override
    protected void writeEntityToNBT(NBTTagCompound compound) {
        Objects.requireNonNull(compound, "NBT root must not be null");
        if (diagnosticIdentity!=null) diagnosticIdentity.write(compound);
        else if (compound.hasKey(PodDiagnosticIdentity.ROOT_KEY))
            throw new IllegalStateException("Active/unmarked body cannot write a foreign diagnostic identity");
        if (artifactIdentity != null) {
            PodArtifactNbtCodec.write(compound, artifactIdentity);
        } else if (PodArtifactNbtCodec.read(compound).isPresent()) {
            throw new IllegalStateException("Unmarked entity cannot write a foreign artifact identity");
        }
    }

    /**
     * 向 GeckoLib 登记动画控制器。GeckoLib 在首次为本实体创建 {@link AnimationData} 时回调本方法；
     * 未登记控制器时其动画判定永远不会被调用。
     *
     * @param data GeckoLib 为本实体维护的动画数据容器。
     */
    @Override
    public void registerControllers(AnimationData data) {
        data.addAnimationController(new AnimationController<>(
                this,
                ANIMATION_CONTROLLER_NAME,
                ANIMATION_TRANSITION_TICKS,
                this::selectAnimation));
    }

    /**
     * 提供 GeckoLib 所需的动画数据工厂。
     *
     * @return 本实体实例专属的动画工厂。
     */
    @Override
    public AnimationFactory getFactory() {
        return animationFactory;
    }

    /**
     * 选择当前应播放的动画。
     *
     * <p>当前只循环播放待机动画。按 {@code doc/05-lifecycle-and-interactions.md}，
     * 释放与收回的动画预设由服务端决定、客户端不得自行构造状态切换；
     * 该状态机属后续切片，本切片不引入任何客户端自主的动画选择逻辑。
     *
     * @param event GeckoLib 每渲染帧提供的动画事件。
     * @return 恒为 {@link PlayState#CONTINUE}，表示该控制器持续播放。
     */
    private PlayState selectAnimation(AnimationEvent<FloatingPodEntity> event) {
        event.getController().setAnimation(new AnimationBuilder().loop(IDLE_ANIMATION_NAME));
        return PlayState.CONTINUE;
    }
}
