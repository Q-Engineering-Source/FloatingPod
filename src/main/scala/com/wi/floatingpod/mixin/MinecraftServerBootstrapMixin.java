package com.wi.floatingpod.mixin;

import com.wi.floatingpod.bootstrap.DedicatedBootstrapBridge;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import net.minecraftforge.fml.common.FMLCommonHandler;
import com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation;
import net.minecraft.server.MinecraftServer;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.Redirect;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** common侧仅按原Installation选择专服owner；integrated server保持原vanilla路径。 */
@Mixin(MinecraftServer.class)
public abstract class MinecraftServerBootstrapMixin {
    @Unique private volatile DedicatedBootstrapBridge floatingpod$bootstrap;
    @Unique private final Object floatingpod$bootstrapLock = new Object();

    @Inject(method = "initiateShutdown()V", at = @At("HEAD"), cancellable = true)
    private void floatingpod$completeCertifiedFreshBeforeShutdown(CallbackInfo ci) {
        MinecraftServer server=(MinecraftServer)(Object)this;
        if(server.isDedicatedServer() && ProductionBootstrapInstallation.deferCertifiedFreshShutdown(server))ci.cancel();
    }

    @Inject(method = "startServerThread()V", at = @At("HEAD"), cancellable = true)
    private void floatingpod$deferDedicatedStart(CallbackInfo ci) {
        MinecraftServer server = (MinecraftServer) (Object) this;
        if (!server.isDedicatedServer()) return;
        DedicatedBootstrapBridge bridge;
        boolean first = false;
        synchronized (floatingpod$bootstrapLock) {
            bridge = floatingpod$bootstrap;
            if (bridge == null) {
                bridge = ProductionBootstrapInstallation.createInstalled(server, server::startServerThread);
                floatingpod$bootstrap = bridge;
                first = true;
            }
        }
        if (first) bridge.startDeferred();
        else if (bridge.allowOriginalStart()) {
            return;
        }
        ci.cancel();
    }

    @Redirect(method = "startServerThread()V", at = @At(value = "INVOKE", target = "Ljava/lang/Thread;start()V"))
    private void floatingpod$transferBeforeOriginalStart(Thread exactOriginalThread) {
        MinecraftServer server = (MinecraftServer) (Object) this;
        if (!server.isDedicatedServer()) { exactOriginalThread.start(); return; }
        if (exactOriginalThread != server.getServerThread())
            throw new IllegalStateException("Redirect did not receive actual MinecraftServer serverThread");
        DedicatedBootstrapBridge bridge = floatingpod$bootstrap;
        if (bridge == null) throw new IllegalStateException("Dedicated bootstrap is unavailable at original Thread.start");
        bridge.transferAndStart(exactOriginalThread, exactOriginalThread::start);
    }

    /** 四处原stopServer调用保留；仅已issued普通路径核一次原正常返回，不为失败finally重试。 */
    @WrapOperation(method="run()V",at=@At(value="INVOKE",target="Lnet/minecraft/server/MinecraftServer;stopServer()V"),require=4,allow=4)
    private void floatingpod$ordinaryStopServer(MinecraftServer actual,Operation<Void> original)throws Throwable{
        var dbFirst=actual.isDedicatedServer()?ProductionBootstrapInstallation.dbFirstRunExitOwner(actual):null;
        if(dbFirst!=null&&dbFirst.stopServerPending()){
            floatingpod$dbFirstStopServer(actual,dbFirst,original);return;
        }
        var runtime=actual.isDedicatedServer()?ProductionBootstrapInstallation.ordinaryRunExitOwner(actual):null;
        if(runtime==null||!runtime.ordinaryStopServerPending()){original.call(actual);return;}
        var callbacks=ProductionBootstrapInstallation.ordinaryRunExitCallbacks(actual,runtime);
        Throwable validation=null;
        try{runtime.requireOrdinaryStopServerEntry(callbacks);}
        catch(Throwable cause){
            validation=cause;
            try{runtime.retainOrdinaryInvocationFailure(callbacks,cause);}
            catch(Throwable recording){if(recording!=cause)cause.addSuppressed(recording);}
        }
        try{original.call(actual);}
        catch(Throwable cause){
            if(validation==null){runtime.retainOrdinaryInvocationFailure(callbacks,cause);throw cause;}
            com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(validation,cause);throw validation;
        }
        if(validation!=null)throw validation;
        try{runtime.ordinaryStopServerReturned(callbacks);}
        catch(Throwable cause){runtime.retainOrdinaryInvocationFailure(callbacks,cause);throw cause;}
    }
    /** 所有原完整Stopped dispatch正常返回之后才准结束late-save代；非普通分支不取得权限。 */
    @WrapOperation(method="run()V",at=@At(value="INVOKE",target="Lnet/minecraftforge/fml/common/FMLCommonHandler;handleServerStopped()V"),require=12,allow=12)
    private void floatingpod$ordinaryStoppedDispatch(FMLCommonHandler handler,Operation<Void> original)throws Throwable{
        MinecraftServer server=(MinecraftServer)(Object)this;
        var dbFirst=server.isDedicatedServer()?ProductionBootstrapInstallation.dbFirstRunExitOwner(server):null;
        if(dbFirst!=null&&dbFirst.stoppedDispatchPending()){
            floatingpod$dbFirstStoppedDispatch(server,handler,dbFirst,original);return;
        }
        var runtime=server.isDedicatedServer()?ProductionBootstrapInstallation.ordinaryRunExitOwner(server):null;
        if(runtime==null||!runtime.ordinaryStoppedDispatchPending()){original.call(handler);return;}
        var callbacks=ProductionBootstrapInstallation.ordinaryRunExitCallbacks(server,runtime);
        Throwable validation=null;
        try{runtime.requireOrdinaryStoppedDispatchEntry(callbacks);}
        catch(Throwable cause){
            validation=cause;
            try{runtime.retainOrdinaryInvocationFailure(callbacks,cause);}
            catch(Throwable recording){if(recording!=cause)cause.addSuppressed(recording);}
        }
        try{original.call(handler);}
        catch(Throwable cause){
            if(validation==null){runtime.retainOrdinaryInvocationFailure(callbacks,cause);throw cause;}
            com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(validation,cause);throw validation;
        }
        if(validation!=null)throw validation;
        try{runtime.ordinaryStoppedDispatchReturned(callbacks);}
        catch(Throwable cause){runtime.retainOrdinaryInvocationFailure(callbacks,cause);throw cause;}
    }

    @Unique private static void floatingpod$retainDbFirstFailure(
            com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime,
            com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks callbacks,Throwable cause){
        try{runtime.retainInvocationFailure(callbacks,cause);}
        catch(Throwable recording){if(recording!=cause)cause.addSuppressed(recording);}
    }
    @Unique private void floatingpod$dbFirstStopServer(MinecraftServer actual,
            com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime,Operation<Void> original)throws Throwable{
        var callbacks=ProductionBootstrapInstallation.dbFirstRunExitCallbacks(actual,runtime);
        Throwable validation=null;
        try{runtime.requireStopServerEntry(callbacks);}
        catch(Throwable cause){validation=cause;floatingpod$retainDbFirstFailure(runtime,callbacks,cause);}
        try{original.call(actual);}
        catch(Throwable cause){
            if(validation==null){floatingpod$retainDbFirstFailure(runtime,callbacks,cause);throw cause;}
            com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(validation,cause);throw validation;
        }
        if(validation!=null)throw validation;
        try{runtime.stopServerReturned(callbacks);}
        catch(Throwable cause){floatingpod$retainDbFirstFailure(runtime,callbacks,cause);throw cause;}
    }
    @Unique private void floatingpod$dbFirstStoppedDispatch(MinecraftServer server,FMLCommonHandler handler,
            com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime,Operation<Void> original)throws Throwable{
        var callbacks=ProductionBootstrapInstallation.dbFirstRunExitCallbacks(server,runtime);
        Throwable validation=null;
        try{runtime.requireStoppedDispatchEntry(callbacks);}
        catch(Throwable cause){validation=cause;floatingpod$retainDbFirstFailure(runtime,callbacks,cause);}
        try{original.call(handler);}
        catch(Throwable cause){
            if(validation==null){floatingpod$retainDbFirstFailure(runtime,callbacks,cause);throw cause;}
            com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(validation,cause);throw validation;
        }
        if(validation!=null)throw validation;
        try{runtime.stoppedDispatchReturned(callbacks);}
        catch(Throwable cause){floatingpod$retainDbFirstFailure(runtime,callbacks,cause);throw cause;}
    }

    @Inject(method = "run()V", at = @At("HEAD"))
    private void floatingpod$claimBeforeInit(CallbackInfo ci) {
        MinecraftServer server = (MinecraftServer) (Object) this;
        if (!server.isDedicatedServer()) return;
        DedicatedBootstrapBridge bridge = floatingpod$bootstrap;
        if (bridge == null) throw new IllegalStateException("Dedicated bootstrap is unavailable before init");
        bridge.claimTarget();
        // claimTarget 仅在精确 transfer 已消费且记录 Phase.CLAIMED 后返回。
        // 在 vanilla 进入 init 并创建 world 前保留 closed managed-save 责任。
        ProductionBootstrapInstallation.installClosedManagedBootSaveScope(server,bridge);
    }
}
