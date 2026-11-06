package com.wi.floatingpod.bootstrap;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.World;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import net.minecraft.world.gen.ChunkProviderServer;

/** 保留专服启动的真实责任；默认从固定普通配置入口创建，旧 control 合同须显式选择。 */
public final class ProductionBootstrapInstallation {
    private ProductionBootstrapInstallation() { }
    private static final class Binding {
        final DedicatedBootstrapBridge control;
        final DedicatedBootstrapBridge.Callbacks callbacks;
        boolean consumed;
        Object runtimeResponsibility;
        ManagedBootSaveScope managedBootSaveScope;
        DedicatedNormalStopBridge freshStopBridge;
        boolean normalStopRequested;
        Thread startupFailureInvoker;
        boolean startupFailureInvocationAttempted;
        boolean startupFailureEntryConsumed;
        Binding(DedicatedBootstrapBridge control, DedicatedBootstrapBridge.Callbacks callbacks) {
            this.control=control; this.callbacks=callbacks;
        }
    }
    private static final IdentityHashMap<Object,Binding> HOLDERS=new IdentityHashMap<>();
    private static final InstallationState STATE = new InstallationState();
    private static final String MODE_PROPERTY="fp.bootstrap.mode";
    private static final String CONTROL_PATH_PROPERTY="fp.bootstrap.controlPath";
    private static final String CONTROL_SHA_PROPERTY="fp.bootstrap.controlSha256";
    private static final String[] CONTROL_PROPERTIES={CONTROL_PATH_PROPERTY,CONTROL_SHA_PROPERTY,
        "fp.bootstrap.preparationMillis","fp.bootstrap.claimMillis","fp.bootstrap.finalizationMillis"};
    /** Default MANAGED route parses only the fixed ordinary config; it does not connect or start the control thread. */
    public static DedicatedBootstrapBridge createInstalled(Object actualServer, Runnable originalStart) {
        String mode=System.getProperty(MODE_PROPERTY,"MANAGED");
        if("CONTROLLED".equals(mode)) {
            for(String key:CONTROL_PROPERTIES) if(System.getProperty(key)==null)
                throw new IllegalStateException("Explicit CONTROLLED bootstrap is missing required launch property: "+key);
            var selected=launchContract();var selectedLimits=launchLimits();
            return STATE.createControlled(selected,selectedLimits,actualServer,originalStart);
        }
        if(!"MANAGED".equals(mode)) throw new IllegalArgumentException("fp.bootstrap.mode must be exactly MANAGED or CONTROLLED");
        for(String key:CONTROL_PROPERTIES) if(System.getProperty(key)!=null)
            throw new IllegalStateException("Managed and CONTROLLED bootstrap launch properties cannot be mixed: "+key);
        Path root=Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        Path configFile=root.resolve("config/floatingpod/database.properties");
        ManagedDatabaseConfiguration config=null;
        try {
            config=ManagedDatabaseConfiguration.load(root,configFile);
            var selectedLimits=ManagedBootstrapPolicy.limits(config.initializationTimeoutMillis());
            return STATE.createManaged(config,selectedLimits,actualServer,originalStart);
        } catch(RuntimeException|java.io.IOException failure) {
            if(config!=null) try { config.close(); } catch(Throwable cleanup) { if(cleanup!=failure) failure.addSuppressed(cleanup); }
            throw new IllegalStateException("Managed FloatingPod database configuration is invalid at the fixed instance path: "+configFile,failure);
        }
    }
    public static void ensureInstalled() {
        String mode=System.getProperty(MODE_PROPERTY,"MANAGED");
        if("CONTROLLED".equals(mode)) { STATE.ensureControlled(launchContract(),launchLimits());return; }
        if(!"MANAGED".equals(mode)) throw new IllegalArgumentException("fp.bootstrap.mode must be exactly MANAGED or CONTROLLED");
        for(String key:CONTROL_PROPERTIES) if(System.getProperty(key)!=null)
            throw new IllegalStateException("Managed and CONTROLLED bootstrap launch properties cannot be mixed: "+key);
        Path root=Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        Path file=root.resolve("config/floatingpod/database.properties");
        ManagedDatabaseConfiguration config=null;
        try {
            config=ManagedDatabaseConfiguration.load(root,file);
            STATE.ensureManaged(config,ManagedBootstrapPolicy.limits(config.initializationTimeoutMillis()));
        } catch(RuntimeException|java.io.IOException failure) {
            if(config!=null) try { config.close(); } catch(Throwable cleanup) { if(cleanup!=failure) failure.addSuppressed(cleanup); }
            throw new IllegalStateException("Managed FloatingPod database configuration is invalid at the fixed instance path: "+file,failure);
        }
    }
    private static ProductionBootstrapControl.LaunchContract launchContract() {
        return new ProductionBootstrapControl.LaunchContract(Path.of(required("fp.bootstrap.controlPath")), required("fp.bootstrap.controlSha256"));
    }
    private static DedicatedBootstrapBridge.Limits launchLimits() {
        return new DedicatedBootstrapBridge.Limits(duration("fp.bootstrap.preparationMillis"), duration("fp.bootstrap.claimMillis"), duration("fp.bootstrap.finalizationMillis"));
    }
    /** 包内真实状态机；生产唯一 STATE，测试可独立检查相同安装机制而不重置生产 holder。 */
    static final class InstallationState {
        private DedicatedBootstrapBridge.Installation installation;
        private ProductionBootstrapControl.LaunchContract contract;
        private DedicatedBootstrapBridge.Limits limits;
        private Path managedConfigurationPath;
        synchronized DedicatedBootstrapBridge.Installation ensure(ProductionBootstrapControl.LaunchContract selected, DedicatedBootstrapBridge.Limits selectedLimits) {
            Objects.requireNonNull(selected); Objects.requireNonNull(selectedLimits);
            if (installation != null) {
                if (managedConfigurationPath!=null || !contract.equals(selected) || !limits.equals(selectedLimits))
                    throw new IllegalStateException("Production bootstrap launch contract changed");
                return installation;
            }
            var factory = new ProductionDedicatedBootstrapFactory(selected, selectedLimits);
            // 若真实 bridge.install 失败，状态不发布；原异常原样传播。
            var accepted = DedicatedBootstrapBridge.install(factory, selectedLimits);
            installation = accepted; contract = selected; limits = selectedLimits;
            return accepted;
        }
        synchronized DedicatedBootstrapBridge.Installation ensureManaged(ManagedDatabaseConfiguration config,
                DedicatedBootstrapBridge.Limits selectedLimits) {
            Objects.requireNonNull(config);Objects.requireNonNull(selectedLimits);
            if(installation!=null) {
                if(managedConfigurationPath==null || !managedConfigurationPath.equals(config.configurationFile()) || !limits.equals(selectedLimits)) {
                    config.close();
                    throw new IllegalStateException("Managed bootstrap launch configuration changed");
                }
                config.close();
                return installation;
            }
            DedicatedBootstrapBridge.Installation accepted;
            try { accepted=DedicatedBootstrapBridge.install(new ProductionDedicatedBootstrapFactory(config,selectedLimits),selectedLimits); }
            catch(Throwable primary) {
                try { config.close(); } catch(Throwable cleanup) { if(cleanup!=primary) primary.addSuppressed(cleanup); }
                throw primary;
            }
            managedConfigurationPath=config.configurationFile();limits=selectedLimits;installation=accepted;
            return accepted;
        }
        synchronized DedicatedBootstrapBridge.Installation ensureControlled(ProductionBootstrapControl.LaunchContract selected,
                DedicatedBootstrapBridge.Limits selectedLimits) { return ensure(selected,selectedLimits); }
        synchronized DedicatedBootstrapBridge createControlled(ProductionBootstrapControl.LaunchContract selected,
                DedicatedBootstrapBridge.Limits selectedLimits,Object actualServer,Runnable originalStart) {
            ensureControlled(selected,selectedLimits);return DedicatedBootstrapBridge.create(actualServer,originalStart);
        }
        synchronized DedicatedBootstrapBridge createManaged(ManagedDatabaseConfiguration config,
                DedicatedBootstrapBridge.Limits selectedLimits,Object actualServer,Runnable originalStart) {
            ensureManaged(config,selectedLimits);return DedicatedBootstrapBridge.create(actualServer,originalStart);
        }
        synchronized DedicatedBootstrapBridge create(ProductionBootstrapControl.LaunchContract selected, DedicatedBootstrapBridge.Limits selectedLimits, Object actualServer, Runnable originalStart) {
            ensure(selected, selectedLimits);
            return DedicatedBootstrapBridge.create(actualServer, originalStart);
        }
    }
    static synchronized void retain(Object actualServer,DedicatedBootstrapBridge control,DedicatedBootstrapBridge.Callbacks callbacks) {
        Objects.requireNonNull(actualServer);Objects.requireNonNull(control);Objects.requireNonNull(callbacks);
        if (HOLDERS.containsKey(actualServer)) throw new IllegalStateException("Actual dedicated instance already has a bootstrap holder");
        HOLDERS.put(actualServer,new Binding(control,callbacks));
    }
    /** 原 run() HEAD 的精确 claim 成功返回后、原 init 开始前保留 closed 保存责任；不会签发写许可、undo permit 或 Ready。 */
    public static synchronized void installClosedManagedBootSaveScope(MinecraftServer actualServer,
                                                                       DedicatedBootstrapBridge exactControl) {
        Objects.requireNonNull(actualServer,"actual dedicated server");
        Objects.requireNonNull(exactControl,"exact bootstrap control");
        Binding binding=HOLDERS.get(actualServer);
        if(binding==null || binding.control!=exactControl)
            throw new IllegalStateException("Exact actual dedicated bootstrap holder required for closed save scope");
        if(exactControl.phase()!=DedicatedBootstrapBridge.Phase.CLAIMED ||
            Thread.currentThread()!=exactControl.originalTargetThread() ||
            Thread.currentThread()!=actualServer.getServerThread())
            throw new IllegalStateException("Successful exact original Main claim required before closed save scope");
        if(binding.managedBootSaveScope!=null)
            throw new IllegalStateException("Closed managed boot save scope is one-shot");
        if(!(binding.callbacks instanceof ManagedBootSaveCallbacks callbacks))
            throw new IllegalStateException("Production dedicated callback holder required for closed save scope");
        var context=callbacks.worldContext();
        var tag=callbacks.bootTag();
        if(context==null || tag==null || context.actualServer()!=actualServer)
            throw new IllegalStateException("Exact pinned world context and original Boot tag required");
        Path root=context.worldDirectory().toAbsolutePath().normalize();
        ManagedBootSaveScope candidate=new ManagedBootSaveScope(actualServer,exactControl,callbacks,
            actualServer.getServerThread(),root,tag.bootId(),tag.bootGeneration(),
            tag.connectionGeneration(),tag.operationId());
        try {
            callbacks.verifyClosedBootSaveScopeIssuer(actualServer);
            var retained=callbacks.retainedRecallSourceCustodyForClosedScope(actualServer);
            if(retained!=null && retained.isDefined())candidate.attachColdSourceCensus(retained.get());
        }
        catch(Throwable cause) { throw candidate.failure("SCOPE_ISSUER",cause); }
        binding.managedBootSaveScope=candidate;
    }

    /** Returns only the census attached to the original scope that enrolled this exact loader identity. */
    public static synchronized Optional<ManagedSourcePathCensus> sourcePathCensusForLoader(AnvilChunkLoader exactLoader) {
        Objects.requireNonNull(exactLoader,"actual AnvilChunkLoader");
        Path directory;
        try { directory=normalizeLoaderDirectory(exactLoader); }
        catch(Throwable cause) {
            ManagedBootSaveScope any=firstScope();
            if(any!=null)throw any.failure("SOURCE_CENSUS_LOADER_PATH",cause);
            throw cause;
        }
        ManagedSourcePathCensus found=null;
        for(Binding binding:HOLDERS.values()) {
            ManagedBootSaveScope scope=binding.managedBootSaveScope;
            if(scope==null||!scope.contains(directory))continue;
            if(!scope.ownsLoader(exactLoader))throw scope.failure("SOURCE_CENSUS_LOADER_ENROLLMENT");
            ManagedSourcePathCensus census=scope.sourceCensus();
            if(census==null)continue;
            if(found!=null&&found!=census)throw scope.failure("SOURCE_CENSUS_SCOPE_OVERLAP");
            found=census;
        }
        return Optional.ofNullable(found);
    }

    /** Exact provider owner bridge for SOURCE admission and typed per-chunk decisions. */
    public static Optional<com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission> ordinarySaveAdmissionForProvider(
            ChunkProviderServer exactProvider) {
        return sourceProviderBinding(exactProvider).map(value->value.admission);
    }

    public static boolean invokeProviderSave(ChunkProviderServer provider,boolean all,
            ManagedBootSaveScope.ProviderSaveCall original)throws Throwable {
        var selected=sourceProviderBinding(provider);
        if(selected.isEmpty()||!(selected.get().callbacks instanceof ProductionDbFirstBootstrapCallbacks))
            return original.call();
        var exact=selected.get();
        return exact.scope.invokeProviderSave(exact.loader,all,original);
    }

    /** Returns the original callbacks lease's exact typed SAVE/UNLOAD decision for this provider/chunk. */
    public static Optional<com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderDecision>
            decideSourceProviderOperation(ChunkProviderServer exactProvider,Chunk exactChunk,
                    com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.ProviderOperation operation) {
        Objects.requireNonNull(exactChunk,"actual provider Chunk");Objects.requireNonNull(operation,"typed provider operation");
        Optional<SourceProviderBinding> selected=sourceProviderBinding(exactProvider);
        if(selected.isEmpty())return Optional.empty();
        SourceProviderBinding binding=selected.get();
        if(exactChunk.getWorld()!=binding.world)
            throw binding.scope.failure("SOURCE_PROVIDER_CHUNK_IDENTITY","Chunk does not belong to the exact provider WorldServer");
        return Optional.of(binding.callbacks.decideManagedSourceProviderChunk(binding.server,binding.provider,
            binding.world,exactChunk,operation));
    }

    private static final class SourceProviderBinding {
        final MinecraftServer server;final WorldServer world;final ChunkProviderServer provider;
        final AnvilChunkLoader loader;final ManagedBootSaveScope scope;
        final ManagedBootSaveCallbacks callbacks;
        final com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission admission;
        SourceProviderBinding(MinecraftServer server,WorldServer world,ChunkProviderServer provider,
                AnvilChunkLoader loader,ManagedBootSaveScope scope,ManagedBootSaveCallbacks callbacks,
                com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission admission){
            this.server=server;this.world=world;this.provider=provider;this.loader=loader;this.scope=scope;
            this.callbacks=callbacks;this.admission=admission;
        }
    }

    private static synchronized Optional<SourceProviderBinding> sourceProviderBinding(ChunkProviderServer exactProvider) {
        Objects.requireNonNull(exactProvider,"actual ChunkProviderServer");
        List<ScopeBinding> scopes=snapshotScopes();
        World actualWorld=exactProvider.world;
        if(!(actualWorld instanceof WorldServer world)){
            if(scopes.isEmpty())return Optional.empty();
            throw firstScope(scopes).failure("SOURCE_PROVIDER_WORLD","Actual WorldServer required to classify managed provider");
        }
        MinecraftServer server;
        Path root;String folder;
        try{
            server=world.getMinecraftServer();
            root=world.getSaveHandler().getWorldDirectory().toPath().toAbsolutePath().normalize();
            folder=world.provider==null?null:world.provider.getSaveFolder();
        }catch(Throwable cause){
            if(scopes.isEmpty()){
                if(cause instanceof RuntimeException runtime)throw runtime;
                if(cause instanceof Error error)throw error;
                throw new IllegalStateException("Actual provider source facts unavailable",cause);
            }
            throw firstScope(scopes).failure("SOURCE_PROVIDER_FACTS",cause);
        }
        Object rawLoader=exactProvider.chunkLoader;
        if(!(rawLoader instanceof AnvilChunkLoader loader)){
            for(ScopeBinding candidate:scopes){
                Path scopeRoot=candidate.scope.worldRoot();
                if(root.equals(scopeRoot)||root.startsWith(scopeRoot)||scopeRoot.startsWith(root))
                    throw candidate.scope.failure("SOURCE_PROVIDER_LOADER","Managed provider requires its exact AnvilChunkLoader");
            }
            return Optional.empty();
        }
        Path directory;
        try{directory=normalizeLoaderDirectory(loader);}
        catch(Throwable cause){
            if(scopes.isEmpty()){
                if(cause instanceof RuntimeException runtime)throw runtime;
                if(cause instanceof Error error)throw error;
                throw new IllegalStateException("Actual provider loader path unavailable",cause);
            }
            throw firstScope(scopes).failure("SOURCE_PROVIDER_LOADER_PATH",cause);
        }
        if(server==null||!server.isDedicatedServer()){
            for(ScopeBinding candidate:scopes)candidate.scope.requireUnmanagedDisjoint(loader,directory,root,folder);
            return Optional.empty();
        }
        Binding holder=HOLDERS.get(server);
        if(holder==null){
            for(ScopeBinding candidate:scopes)candidate.scope.requireUnmanagedDisjoint(loader,directory,root,folder);
            return Optional.empty();
        }
        ManagedBootSaveScope scope=holder.managedBootSaveScope;
        if(scope==null)throw missingManagedSaveScope(holder);
        requireScopeBinding(holder,scope,server);
        if(Thread.currentThread()!=server.getServerThread()||world.getChunkProvider()!=exactProvider||
                exactProvider.chunkLoader!=loader||world.provider==null)
            throw scope.failure("SOURCE_PROVIDER_OWNER","Exact original Main/provider/loader installation required");
        if(!root.equals(scope.worldRoot()))throw scope.failure("SOURCE_PROVIDER_WORLD_ROOT","Provider world root differs from the pinned source scope");
        Path expected=providerRoot(scope.worldRoot(),folder,scope);
        if(!scope.ownsLoader(loader)||!directory.equals(expected))
            throw scope.failure("SOURCE_PROVIDER_LOADER","Exact enrolled source Anvil loader path required");
        if(!(holder.callbacks instanceof ManagedBootSaveCallbacks callbacks))
            throw scope.failure("SOURCE_PROVIDER_CALLBACKS","Original production callbacks holder required");
        return Optional.of(new SourceProviderBinding(server,world,exactProvider,loader,scope,callbacks,callbacks.ordinarySaveAdmission()));
    }

    private static Path providerRoot(Path root,String folder,ManagedBootSaveScope scope){
        if(folder==null||folder.isEmpty())return root;
        try{
            Path relative=Path.of(folder);
            if(relative.isAbsolute())throw scope.failure("SOURCE_PROVIDER_FOLDER","Provider folder must be relative");
            Path resolved=root.resolve(relative).normalize();
            if(!resolved.startsWith(root))throw scope.failure("SOURCE_PROVIDER_FOLDER","Provider folder escapes its pinned world root");
            return resolved;
        }catch(ManagedBootSaveScope.AdmissionException cause){throw cause;}
        catch(RuntimeException cause){throw scope.failure("SOURCE_PROVIDER_FOLDER",cause);}
    }

    /** Binds only the actual post-mutation WorldServer/Anvil loader back to its constructor enrollment. */
    public static void observeSourceWorld(MinecraftServer exactServer,WorldServer actualWorld,int dimension) {
        Objects.requireNonNull(exactServer,"actual dedicated server");
        Objects.requireNonNull(actualWorld,"actual source WorldServer");
        if(actualWorld.getMinecraftServer()!=exactServer)return;
        if(actualWorld.provider==null||actualWorld.provider.getDimension()!=dimension)
            throw new IllegalStateException("Actual source WorldServer/dimension changed during registration");
        if(!(actualWorld.getChunkProvider().chunkLoader instanceof AnvilChunkLoader loader))return;
        Optional<ManagedSourcePathCensus> census=sourcePathCensusForLoader(loader);
        census.ifPresent(value->value.bindActualWorld(value.pathBindingForLoader(loader).orElseThrow(
            ()->new IllegalStateException("Exact source loader census binding disappeared")),actualWorld,dimension,
            actualWorld.getSaveHandler().getWorldDirectory().toPath()));
    }

    /** Validate a managed WorldServer registration target before DimensionManager mutates its table. */
    public static void requireSourceWorldMutationTarget(MinecraftServer exactServer,WorldServer target,int dimension) {
        Objects.requireNonNull(target,"actual DimensionManager target WorldServer");
        Optional<SourceProviderBinding> selected=sourceProviderBinding(target.getChunkProvider());
        if(selected.isEmpty())return;
        SourceProviderBinding binding=selected.get();
        if(exactServer!=binding.server||binding.world!=target||target.provider==null||
                target.provider.getDimension()!=dimension)
            throw binding.scope.failure("SOURCE_WORLD_MUTATION_TARGET",
                "DimensionManager target differs from the exact enrolled server/world/dimension before mutation");
    }

    public static synchronized void noteSourceDimensionMutation() {
        for(Binding binding:HOLDERS.values()) {
            ManagedBootSaveScope scope=binding.managedBootSaveScope;
            if(scope==null)continue;
            ManagedSourcePathCensus census=scope.sourceCensus();
            if(census!=null)census.noteDimensionMutation();
        }
    }

    /** 在 Anvil 构造器返回处登记其真实 loader；仅登记路径处于 pinned root 内的实例。 */
    public static synchronized void observeAnvilChunkLoader(AnvilChunkLoader loader) {
        Objects.requireNonNull(loader,"actual AnvilChunkLoader");
        Path directory;
        try { directory=normalizeLoaderDirectory(loader); }
        catch(Throwable cause) {
            ManagedBootSaveScope any=firstScope();
            if(any!=null)throw any.failure("LOADER_CONSTRUCTOR_PATH",cause);
            throw cause;
        }
        ManagedBootSaveScope selected=null;
        Binding selectedBinding=null;
        for(Binding binding:HOLDERS.values()) {
            ManagedBootSaveScope scope=binding.managedBootSaveScope;
            if(scope!=null && scope.contains(directory)) {
                if(selected!=null && selected!=scope)
                    throw selected.failure("SCOPE_OVERLAP");
                selected=scope;selectedBinding=binding;
            }
        }
        if(selected==null)return; // 不相关 loader 不因路径本身而取得 managed 身份。
        requireScopeBinding(selectedBinding,selected,(MinecraftServer)selected.actualServer());
        selected.registerLoader(loader,directory);
        if(!(loader instanceof com.wi.floatingpod.server.save.ManagedSaveAccess access))
            throw selected.failure("ORDINARY_SAVE_INSTALLER","Actual Anvil save hooks unavailable");
        var callbacks=(ManagedBootSaveCallbacks)selectedBinding.callbacks;
        access.floatingpod$installOrdinaryAdmission(callbacks.ordinarySaveAdmission());
    }

    /** 原 SaveHandler.flush 后核原 handler/server/root 责任；不依赖已退休的 DATA current。 */
    private static com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission ordinarySaveOwnerForHandler(
            net.minecraft.world.chunk.storage.AnvilSaveHandler handler){
        Objects.requireNonNull(handler,"actual Anvil save handler");
        Path root=handler.getWorldDirectory().toPath().toAbsolutePath().normalize();
        IdentityHashMap<Object,Binding> holders;
        synchronized(ProductionBootstrapInstallation.class){holders=new IdentityHashMap<>(HOLDERS);}
        for(var retained:holders.entrySet()){
            if(!(retained.getKey() instanceof MinecraftServer server)||server.worlds==null)continue;
            for(WorldServer world:server.worlds)if(world!=null&&world.getSaveHandler()==handler&&retained.getValue().managedBootSaveScope==null)
                throw missingManagedSaveScope(retained.getValue());
        }
        com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission actualOwner=null;
        for(ScopeBinding selected:snapshotScopes()){
            ManagedBootSaveScope scope=selected.scope;
            MinecraftServer server=(MinecraftServer)scope.actualServer();
            boolean exactHandler=false;
            if(server.worlds!=null)for(WorldServer world:server.worlds)
                if(world!=null&&world.getSaveHandler()==handler){exactHandler=true;break;}
            boolean intersects=root.startsWith(scope.worldRoot())||scope.worldRoot().startsWith(root);
            if(!exactHandler&&!intersects)continue;
            if(!exactHandler||!root.equals(scope.worldRoot()))
                throw scope.failure("SAVE_HANDLER_FLUSH_IDENTITY","Actual save handler/root does not match retained managed instance");
            if(Thread.currentThread()!=server.getServerThread())throw scope.failure("SAVE_HANDLER_FLUSH_MAIN");
            if(!(selected.binding.callbacks instanceof ManagedBootSaveCallbacks callbacks))
                throw scope.failure("SAVE_HANDLER_FLUSH_CALLBACKS");
            actualOwner=callbacks.ordinarySaveAdmission();
        }
        return actualOwner;
    }
    public static void requireOrdinarySaveHealthy(net.minecraft.world.chunk.storage.AnvilSaveHandler handler){
        var owner=ordinarySaveOwnerForHandler(handler);
        if(owner!=null){
            owner.requireHealthyAfterFlush();
            for(ScopeBinding selected:snapshotScopes())
                if(selected.binding.callbacks instanceof ProductionDbFirstBootstrapCallbacks callbacks&&callbacks.ordinarySaveAdmission()==owner)
                    selected.scope.requireProviderSavesComplete();
        }
    }
    public static void invokeAnvilFlushWait(net.minecraft.world.chunk.storage.AnvilSaveHandler handler,
            ManagedAnvilSaveCall original)throws Throwable{
        var owner=ordinarySaveOwnerForHandler(handler);
        if(owner==null){original.call();return;} // disjoint unmanaged 保留原 wait/catch 行为。
        owner.waitForFlush(original::call);
    }
    public static void invokeAnvilHandlerFlush(net.minecraft.world.chunk.storage.AnvilSaveHandler handler,
            ManagedAnvilSaveCall original)throws Throwable{
        var owner=ordinarySaveOwnerForHandler(handler);
        if(owner==null){original.call();return;}
        owner.flush(original::call,()->requireOrdinarySaveHealthy(handler));
    }
    @FunctionalInterface
    public interface ManagedAnvilSaveCall { void call() throws Throwable; }

    /** 在 saveChunk 的 session 检查、NBT 构造、事件分发和队列变更前调用；只有与所有保留 root 不相交时才委托原实现。 */
    public static void invokeAnvilSave(AnvilChunkLoader loader, World world, net.minecraft.world.chunk.Chunk chunk,
                                       ManagedAnvilSaveCall original) throws Throwable {
        Objects.requireNonNull(loader,"actual AnvilChunkLoader");
        Objects.requireNonNull(original,"original saveChunk body");
        List<ScopeBinding> scopes=snapshotScopes();
        WorldServer serverWorld=world instanceof WorldServer selected?selected:null;
        MinecraftServer actualServer=null;
        if(serverWorld!=null) {
            try { actualServer=serverWorld.getMinecraftServer(); }
            catch(Throwable cause) {
                ManagedBootSaveScope any=firstScope(scopes);
                if(any!=null)throw any.failure("WORLD_SERVER",cause);
                throw cause;
            }
        }
        if(serverWorld!=null && actualServer!=null && actualServer.isDedicatedServer()) {
            Binding binding;
            synchronized(ProductionBootstrapInstallation.class) { binding=HOLDERS.get(actualServer); }
            if(binding==null || binding.managedBootSaveScope==null)
                throw missingManagedSaveScope(binding);
            ManagedBootSaveScope scope=binding.managedBootSaveScope;
            requireScopeBinding(binding,scope,actualServer);
            Path loaderDirectory;
            Path worldRoot;
            String providerFolder;
            Object currentLoader;
            try {
                loaderDirectory=normalizeLoaderDirectory(loader);
                worldRoot=serverWorld.getSaveHandler().getWorldDirectory().toPath().toAbsolutePath().normalize();
                providerFolder=serverWorld.provider==null?null:serverWorld.provider.getSaveFolder();
                currentLoader=serverWorld.getChunkProvider().chunkLoader;
            } catch(Throwable cause) {
                throw scope.failure("WORLD_SAVE_FACTS",cause);
            }
            Objects.requireNonNull(chunk,"actual original chunk");
            final MinecraftServer pinnedServer=actualServer;
            var callbacks=(ManagedBootSaveCallbacks)binding.callbacks;
            ManagedBootSaveScope.SaveCall recheck=()->{
                requireScopeBinding(binding,scope,pinnedServer);
                if(chunk.getWorld()!=serverWorld || serverWorld.getMinecraftServer()!=pinnedServer || serverWorld.getChunkProvider().chunkLoader!=loader ||
                    !serverWorld.getSaveHandler().getWorldDirectory().toPath().toAbsolutePath().normalize().equals(worldRoot) ||
                    serverWorld.provider==null || !Objects.equals(serverWorld.provider.getSaveFolder(),providerFolder) ||
                    !normalizeLoaderDirectory(loader).equals(loaderDirectory) || !scope.ownsLoader(loader))
                    throw scope.failure("ACTUAL_SAVE_RECHECK","Actual world/provider/loader changed before queue admission");
                callbacks.requireOrdinarySaveCurrent(pinnedServer,serverWorld.provider.getDimension(),chunk.x,chunk.z,chunk);
            };
            scope.invokeOrdinary(actualServer,loader,currentLoader,worldRoot,providerFolder,loaderDirectory,
                original::call,callbacks.ordinarySaveAdmission(),chunk.x,chunk.z,recheck,serverWorld,chunk);
            return;
        }
        if(scopes.isEmpty()) {
            original.call(); // 当前进程没有 managed boot root，保留普通保存语义。
            return;
        }
        Path loaderDirectory;
        try { loaderDirectory=normalizeLoaderDirectory(loader); }
        catch(Throwable cause) { throw firstScope(scopes).failure("LOADER_PATH",cause); }
        // 非 dedicated world 仅在已知路径全部位于 retained root 外、且未复用已登记 loader 时保留原行为。
        Path worldRoot=null;
        String providerFolder=null;
        if(serverWorld!=null) {
            try {
                worldRoot=serverWorld.getSaveHandler().getWorldDirectory().toPath().toAbsolutePath().normalize();
                providerFolder=serverWorld.provider==null?null:serverWorld.provider.getSaveFolder();
            } catch(Throwable cause) {
                throw firstScope(scopes).failure("UNMANAGED_WORLD_CLASSIFICATION",cause);
            }
        }
        for(ScopeBinding scopeBinding:scopes) {
            scopeBinding.scope.requireUnmanagedDisjoint(loader,loaderDirectory,worldRoot,providerFolder);
        }
        original.call();
    }

    private static Path normalizeLoaderDirectory(AnvilChunkLoader loader) {
        if(loader.chunkSaveLocation==null)throw new IllegalStateException("Anvil loader has no actual chunk save directory");
        return loader.chunkSaveLocation.toPath().toAbsolutePath().normalize();
    }
    private static IllegalStateException missingManagedSaveScope(Binding binding) {
        if(binding!=null && binding.callbacks instanceof ManagedBootSaveCallbacks callbacks && callbacks.bootTag()!=null) {
            var tag=callbacks.bootTag();
            return new IllegalStateException("Dedicated Anvil save lacks retained closed scope boot="+tag.bootId()+
                " bootGeneration="+tag.bootGeneration()+" connectionGeneration="+tag.connectionGeneration()+
                " operation="+tag.operationId());
        }
        return new IllegalStateException("Dedicated Anvil save lacks exact claimed closed boot scope");
    }
    private static final class ScopeBinding {
        final Binding binding;
        final ManagedBootSaveScope scope;
        ScopeBinding(Binding binding) { this.binding=binding;this.scope=binding.managedBootSaveScope; }
    }
    private static synchronized List<ScopeBinding> snapshotScopes() {
        List<ScopeBinding> result=new ArrayList<>();
        for(Binding binding:HOLDERS.values())if(binding.managedBootSaveScope!=null)result.add(new ScopeBinding(binding));
        return List.copyOf(result);
    }
    private static ManagedBootSaveScope firstScope() { return firstScope(snapshotScopes()); }
    private static ManagedBootSaveScope firstScope(List<ScopeBinding> values) {
        return values.isEmpty()?null:values.get(0).scope;
    }
    private static void requireScopeBinding(Binding binding,ManagedBootSaveScope scope,MinecraftServer server) {
        if(binding==null || scope==null || binding.managedBootSaveScope!=scope ||
            binding.control!=scope.exactControl() || binding.callbacks!=scope.exactCallbacks() ||
            scope.actualServer()!=server)
            throw new IllegalStateException("Retained closed managed save scope lost its exact holder identity");
        if(!(binding.callbacks instanceof ManagedBootSaveCallbacks callbacks))
            throw scope.failure("CALLBACK_HOLDER");
        try { callbacks.verifyClosedBootSaveScopeUse(server); }
        catch(Throwable cause) { throw scope.failure("CLAIMED_CONTEXT",cause); }
    }
    /** complete AboutToStart consumer 只能取得已由 exact target claim 的原 holder；未接入该 consumer 时不得启用 Mixin。 */
    public static synchronized DedicatedBootstrapBridge.Callbacks requireClaimedCallbacks(Object actualServer) {
        Binding value=HOLDERS.get(Objects.requireNonNull(actualServer));
        if(value==null || value.control.phase()!=DedicatedBootstrapBridge.Phase.CLAIMED || Thread.currentThread()!=value.control.originalTargetThread())
            throw new IllegalStateException("Exact claimed dedicated Main bootstrap holder required");
        return value.callbacks;
    }
    public static synchronized void requireOriginalDbFirstRuntime(MinecraftServer server,
            ProductionDbFirstBootstrapCallbacks callbacks,Object runtime){
        requireClaimedCallbacks(server);
        Binding binding=HOLDERS.get(server);
        if(callbacks==null||runtime==null||binding.callbacks!=callbacks||binding.runtimeResponsibility!=runtime)
            throw new IllegalStateException("Original claimed schema6 runtime installation required");
    }
    /** 仅原Main SOURCE装配入口；不安装Stopping hook或发行worker/native许可。 */
    public static void beginOrdinaryMainPreparation(MinecraftServer server,Object runtime,
            ProductionDedicatedBootstrapCallbacks callbacks){
        synchronized(ProductionBootstrapInstallation.class){
            if(requireClaimedCallbacks(server)!=callbacks||HOLDERS.get(server).runtimeResponsibility!=runtime||
                    callbacks.approvedMode()!=ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED)
                throw new IllegalStateException("Original claimed Ordinary runtime/callbacks required");
        }
        if(!(runtime instanceof com.wi.floatingpod.network.DedicatedProjectionRuntime actual))
            throw new IllegalStateException("Original dedicated projection runtime required");
        actual.beginOrdinaryMainPreparation(server,callbacks);
    }
    public static boolean tryFenceOrdinaryFinalSource(MinecraftServer server,ProductionDedicatedBootstrapCallbacks callbacks,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner){
        ManagedBootSaveScope scope;
        synchronized(ProductionBootstrapInstallation.class){
            if(requireClaimedCallbacks(server)!=callbacks)throw new IllegalStateException("Original final source callbacks required");
            scope=HOLDERS.get(server).managedBootSaveScope;
        }
        if(scope==null)throw new IllegalStateException("Original managed scope missing");
        Object runtime;
        synchronized(ProductionBootstrapInstallation.class){runtime=HOLDERS.get(server).runtimeResponsibility;}
        if(!(runtime instanceof com.wi.floatingpod.network.DedicatedProjectionRuntime actual))throw new IllegalStateException("Original runtime missing");
        actual.requireOrdinaryFinalSourceOwner(callbacks,sourceOwner,escrow);
        return scope.tryFenceFinalSource(callbacks,callbacks.ordinarySaveAdmission(),escrow,sourceOwner);
    }
    public static void installOrdinaryFinalSource(MinecraftServer server,ProductionDedicatedBootstrapCallbacks callbacks,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.FinalSaveValidator source){
        ManagedBootSaveScope scope;
        synchronized(ProductionBootstrapInstallation.class){
            if(requireClaimedCallbacks(server)!=callbacks)throw new IllegalStateException("Original final source callbacks required");
            scope=HOLDERS.get(server).managedBootSaveScope;
        }
        if(scope==null)throw new IllegalStateException("Original managed scope missing");
        Object runtime;
        synchronized(ProductionBootstrapInstallation.class){runtime=HOLDERS.get(server).runtimeResponsibility;}
        if(!(runtime instanceof com.wi.floatingpod.network.DedicatedProjectionRuntime actual))throw new IllegalStateException("Original runtime missing");
        actual.requireOrdinaryFinalSource(callbacks,sourceOwner,escrow,source);
        scope.installFinalSource(callbacks,callbacks.ordinarySaveAdmission(),escrow,sourceOwner,source);
    }
    /** schema6原callback保留实际publication factory；不是旧runtime的替身或停服成功许可。 */
    public static boolean tryFenceDbFirstFinalSource(MinecraftServer server,ProductionDbFirstBootstrapCallbacks callbacks,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner){
        ManagedBootSaveScope scope;
        synchronized(ProductionBootstrapInstallation.class){
            if(callbacks==null||requireClaimedCallbacks(server)!=callbacks)
                throw new IllegalStateException("Original schema6 final source callbacks required");
            scope=HOLDERS.get(server).managedBootSaveScope;
        }
        if(scope==null)throw new IllegalStateException("Original schema6 managed scope missing");
        callbacks.requireFinalSourceOwner(sourceOwner,escrow);
        return scope.tryFenceFinalSource(callbacks,callbacks.ordinarySaveAdmission(),escrow,sourceOwner);
    }
    public static void installDbFirstFinalSource(MinecraftServer server,ProductionDbFirstBootstrapCallbacks callbacks,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.FinalSaveValidator source){
        ManagedBootSaveScope scope;
        synchronized(ProductionBootstrapInstallation.class){
            if(callbacks==null||requireClaimedCallbacks(server)!=callbacks)
                throw new IllegalStateException("Original schema6 final source callbacks required");
            scope=HOLDERS.get(server).managedBootSaveScope;
        }
        if(scope==null)throw new IllegalStateException("Original schema6 managed scope missing");
        callbacks.requireFinalSource(sourceOwner,escrow,source);
        scope.installFinalSource(callbacks,callbacks.ordinarySaveAdmission(),escrow,sourceOwner,source);
    }
    static void requireOriginalFinalSourceActivation(ManagedBootSaveScope scope,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission admission,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.FinalSaveValidator source){
        Object runtime;DedicatedBootstrapBridge.Callbacks callbacks;
        synchronized(ProductionBootstrapInstallation.class){
            Binding binding=HOLDERS.get(scope.actualServer());
            if(binding==null||binding.managedBootSaveScope!=scope||binding.control!=scope.exactControl()||
                    binding.callbacks!=scope.exactCallbacks()||requireClaimedCallbacks(scope.actualServer())!=binding.callbacks)
                throw new IllegalStateException("Actual retained managed Scope/callbacks required");
            callbacks=binding.callbacks;runtime=binding.runtimeResponsibility;
        }
        if(callbacks instanceof ProductionDbFirstBootstrapCallbacks actual){
            if(actual.ordinarySaveAdmission()!=admission)throw new IllegalStateException("Original schema6 final source admission required");
            actual.requireFinalSource(sourceOwner,escrow,source);
        }else if(callbacks instanceof ProductionDedicatedBootstrapCallbacks actualCallbacks){
            if(actualCallbacks.ordinarySaveAdmission()!=admission)throw new IllegalStateException("Original final source admission required");
            if(!(runtime instanceof com.wi.floatingpod.network.DedicatedProjectionRuntime actual))throw new IllegalStateException("Original runtime missing");
            actual.requireOrdinaryFinalSource(actualCallbacks,sourceOwner,escrow,source);
        }else throw new IllegalStateException("Original production final source callbacks required");
    }

    /** exact claimed Main 一次取得装配责任；失败也不重发，原 holder 保留诊断与退出责任。 */
    public static synchronized DedicatedBootstrapBridge.Callbacks consumeClaimedCallbacks(Object actualServer) {
        DedicatedBootstrapBridge.Callbacks callbacks=requireClaimedCallbacks(actualServer);
        Binding value=HOLDERS.get(actualServer);
        if (value.consumed) throw new IllegalStateException("Claimed bootstrap assembly already consumed");
        value.consumed=true;
        return callbacks;
    }
    /** 原 holder 保留已 claimed 的 runtime 和退出责任；Stopped 后不删除。 */
    public static synchronized void retainClaimedRuntime(Object actualServer, Object runtime) {
        requireClaimedCallbacks(actualServer);
        Objects.requireNonNull(runtime);
        Binding value=HOLDERS.get(actualServer);
        if (value.runtimeResponsibility != null) throw new IllegalStateException("Claimed runtime responsibility already retained");
        value.runtimeResponsibility=runtime;
        if(value.callbacks instanceof ProductionDedicatedBootstrapCallbacks callbacks &&
                callbacks.approvedMode()==ProductionBootstrapControl.Mode.FRESH){
            var identity=callbacks.adapter().identity();
            value.freshStopBridge=new DedicatedNormalStopBridge(actualServer,Thread.currentThread(),identity.bootId(),identity.bootGeneration());
            if(value.normalStopRequested)value.freshStopBridge.request(actualServer);
        }
    }
    /** Fresh 专用接线；Ordinary 正常停止仍须其独立完整 producer，不借此路径取得许可。 */
    public static synchronized boolean deferCertifiedFreshShutdown(MinecraftServer server) {
        Binding binding=HOLDERS.get(server);
        if(binding==null)return false;
        if(binding.startupFailureInvoker==Thread.currentThread() && !binding.startupFailureEntryConsumed){
            binding.startupFailureEntryConsumed=true;return false;
        }
        if(binding.callbacks instanceof ProductionDbFirstBootstrapCallbacks dbFirst){
            if(Thread.currentThread()==server.getServerThread() &&
                    binding.runtimeResponsibility instanceof com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime &&
                    runtime.shutdownInvocationInFlight()){
                runtime.consumeShutdownEntry(dbFirst);return false;
            }
            // 外线程只在短锁内记请求，不触碰runtime/Main/world或IO。
            binding.normalStopRequested=true;
            if(binding.runtimeResponsibility==null){
                var decision=binding.control.requestCancellation(new java.util.concurrent.CancellationException("schema6 shutdown requested before runtime installation"));
                if(decision==DedicatedBootstrapBridge.CancellationDecision.ALREADY_FAILED)
                    throw new IllegalStateException("schema6 startup failed without original failure exit",binding.control.failure());
            }
            return true;
        }
        if(!(binding.callbacks instanceof ProductionDedicatedBootstrapCallbacks callbacks))return false;
        if(callbacks.approvedMode()==ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED){
            if(binding.runtimeResponsibility instanceof com.wi.floatingpod.network.DedicatedProjectionRuntime runtime &&
                    runtime.ordinaryInvocationInFlight()){
                runtime.consumeOrdinaryShutdownEntry(callbacks);return false;
            }
            binding.normalStopRequested=true;
            if(binding.runtimeResponsibility==null){
                var decision=binding.control.requestCancellation(new java.util.concurrent.CancellationException("Ordinary shutdown requested before runtime installation"));
                if(decision==DedicatedBootstrapBridge.CancellationDecision.ALREADY_FAILED)
                    throw new IllegalStateException("Ordinary startup failed without original failure exit",binding.control.failure());
            }
            return true;
        }
        if(callbacks.approvedMode()!=ProductionBootstrapControl.Mode.FRESH)return false;
        if(binding.freshStopBridge!=null){
            if(binding.freshStopBridge.consumeOriginalEntry(server))return false;
            binding.freshStopBridge.request(server);return true;
        }
        binding.normalStopRequested=true;
        var decision=binding.control.requestCancellation(new java.util.concurrent.CancellationException("Fresh shutdown requested before runtime installation"));
        if(decision==DedicatedBootstrapBridge.CancellationDecision.ALREADY_FAILED)
            throw new IllegalStateException("Fresh startup already failed without an active failure exit",binding.control.failure());
        return true;
    }
    public static synchronized boolean dbFirstShutdownRequested(MinecraftServer server,
            com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime,ProductionDbFirstBootstrapCallbacks callbacks){
        requireOriginalDbFirstRuntime(server,callbacks,runtime);
        return HOLDERS.get(server).normalStopRequested;
    }
    public static void invokeCertifiedDbFirstShutdown(MinecraftServer server,
            com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime,ProductionDbFirstBootstrapCallbacks callbacks){
        synchronized(ProductionBootstrapInstallation.class){
            requireOriginalDbFirstRuntime(server,callbacks,runtime);
            if(!HOLDERS.get(server).normalStopRequested)throw new IllegalStateException("Original schema6 stop request required");
        }
        runtime.consumeShutdownInvocation(callbacks);
        try{server.initiateShutdown();runtime.shutdownReturned(callbacks);}
        catch(Throwable cause){runtime.retainInvocationFailure(callbacks,cause);throw cause;}
    }
    public static synchronized com.wi.floatingpod.network.DbFirstDedicatedRuntime dbFirstRunExitOwner(MinecraftServer server){
        Binding binding=HOLDERS.get(server);
        if(binding==null||Thread.currentThread()!=server.getServerThread()||
                !(binding.callbacks instanceof ProductionDbFirstBootstrapCallbacks)||
                !(binding.runtimeResponsibility instanceof com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime)||
                !runtime.shutdownWasReturned())return null;
        return runtime;
    }
    public static synchronized ProductionDbFirstBootstrapCallbacks dbFirstRunExitCallbacks(MinecraftServer server,
            com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime){
        Binding binding=HOLDERS.get(server);
        if(binding==null||!(binding.callbacks instanceof ProductionDbFirstBootstrapCallbacks callbacks))
            throw new IllegalStateException("Original schema6 exit callbacks required");
        requireOriginalDbFirstRuntime(server,callbacks,runtime);
        return callbacks;
    }
    public static synchronized boolean ordinaryShutdownRequested(MinecraftServer server,Object runtime,
            ProductionDedicatedBootstrapCallbacks callbacks){
        requireClaimedCallbacks(server);
        Binding binding=HOLDERS.get(server);
        if(binding.runtimeResponsibility!=runtime||binding.callbacks!=callbacks||
                callbacks.approvedMode()!=ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED)
            throw new IllegalStateException("Original claimed Ordinary runtime required");
        return binding.normalStopRequested;
    }
    public static void invokeCertifiedOrdinaryShutdown(MinecraftServer server,
            com.wi.floatingpod.network.DedicatedProjectionRuntime runtime,ProductionDedicatedBootstrapCallbacks callbacks){
        synchronized(ProductionBootstrapInstallation.class){
            requireClaimedCallbacks(server);
            Binding binding=HOLDERS.get(server);
            if(binding.runtimeResponsibility!=runtime||binding.callbacks!=callbacks||!binding.normalStopRequested)
                throw new IllegalStateException("Original requested Ordinary stop installation required");
        }
        runtime.consumeOrdinaryShutdownInvocation(callbacks);
        try{server.initiateShutdown();runtime.ordinaryShutdownReturned(callbacks);}
        catch(Throwable cause){runtime.retainOrdinaryInvocationFailure(callbacks,cause);throw cause;}
    }
    /** 仅实际已完成普通许可调用的原server进入后继run目标；其他原路径保持一次原调用。 */
    public static synchronized com.wi.floatingpod.network.DedicatedProjectionRuntime ordinaryRunExitOwner(MinecraftServer server){
        Binding binding=HOLDERS.get(server);
        if(binding==null||!(binding.callbacks instanceof ProductionDedicatedBootstrapCallbacks callbacks)||
                callbacks.approvedMode()!=ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED||
                !(binding.runtimeResponsibility instanceof com.wi.floatingpod.network.DedicatedProjectionRuntime runtime)||
                !runtime.ordinaryShutdownWasReturned())return null;
        if(Thread.currentThread()!=server.getServerThread())return null;
        return runtime;
    }
    public static synchronized ProductionDedicatedBootstrapCallbacks ordinaryRunExitCallbacks(MinecraftServer server,
            com.wi.floatingpod.network.DedicatedProjectionRuntime runtime){
        Binding binding=HOLDERS.get(server);
        if(binding==null||binding.runtimeResponsibility!=runtime||!(binding.callbacks instanceof ProductionDedicatedBootstrapCallbacks callbacks))
            throw new IllegalStateException("Original Ordinary exit installation required");
        return callbacks;
    }
    public static synchronized void requireOriginalFinalGenerationEnd(ManagedBootSaveScope scope,
            com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission admission){
        Binding binding=HOLDERS.get(scope.actualServer());
        if(binding==null||binding.managedBootSaveScope!=scope)
            throw new IllegalStateException("Original final-generation installation required");
        requireScopeBinding(binding,scope,(MinecraftServer)scope.actualServer());
        if(binding.callbacks instanceof ProductionDbFirstBootstrapCallbacks callbacks &&
                binding.runtimeResponsibility instanceof com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime &&
                callbacks.ordinarySaveAdmission()==admission)runtime.requireFinalSaveEndOwner(callbacks);
        else if(binding.callbacks instanceof ProductionDedicatedBootstrapCallbacks callbacks &&
                binding.runtimeResponsibility instanceof com.wi.floatingpod.network.DedicatedProjectionRuntime runtime &&
                callbacks.ordinarySaveAdmission()==admission)runtime.requireOrdinaryFinalSaveEndOwner(callbacks);
        else throw new IllegalStateException("Original final-generation callback/runtime/admission required");
    }
    public static void finishDbFirstFinalSave(MinecraftServer server,
            com.wi.floatingpod.network.DbFirstDedicatedRuntime runtime,ProductionDbFirstBootstrapCallbacks callbacks){
        ManagedBootSaveScope scope;
        synchronized(ProductionBootstrapInstallation.class){
            requireOriginalDbFirstRuntime(server,callbacks,runtime);
            Binding binding=HOLDERS.get(server);
            scope=binding.managedBootSaveScope;
            if(scope==null)throw new IllegalStateException("Original schema6 final save scope required");
            requireScopeBinding(binding,scope,server);
        }
        runtime.requireFinalSaveEndOwner(callbacks);
        scope.endDbFirstFinalGeneration(callbacks,runtime);
    }
    public static void finishOrdinaryFinalSave(MinecraftServer server,
            com.wi.floatingpod.network.DedicatedProjectionRuntime runtime,ProductionDedicatedBootstrapCallbacks callbacks){
        ManagedBootSaveScope scope;
        synchronized(ProductionBootstrapInstallation.class){
            Binding binding=HOLDERS.get(server);
            if(binding==null||binding.runtimeResponsibility!=runtime||binding.callbacks!=callbacks||binding.managedBootSaveScope==null)
                throw new IllegalStateException("Original final save installation required");
            scope=binding.managedBootSaveScope;
            requireScopeBinding(binding,scope,server);
        }
        runtime.requireOrdinaryFinalSaveEndOwner(callbacks);
        scope.endOrdinaryFinalGeneration(callbacks,runtime);
    }

  public static synchronized DedicatedNormalStopBridge.Request pollFreshShutdown(MinecraftServer server,Object runtime) {
        requireClaimedCallbacks(server);
        Binding binding=HOLDERS.get(server);
        if(binding.runtimeResponsibility!=runtime || binding.freshStopBridge==null)
            throw new IllegalStateException("Original claimed Fresh runtime required");
        return binding.freshStopBridge.pollOnMain();
    }
    public static void invokeCertifiedFreshShutdown(MinecraftServer server,Object runtime,
            ProductionDedicatedBootstrapCallbacks callbacks,DedicatedNormalStopBridge.Request request) {
        DedicatedNormalStopBridge bridge;
        synchronized(ProductionBootstrapInstallation.class){
            requireClaimedCallbacks(server);
            Binding binding=HOLDERS.get(server);
            if(binding.runtimeResponsibility!=runtime || binding.callbacks!=callbacks || binding.freshStopBridge==null)
                throw new IllegalStateException("Original Fresh completion installation required");
            bridge=binding.freshStopBridge;
        }
        callbacks.requireCertifiedFreshExitCompleted(server);
        bridge.beginFreshInvocation(request);
        Throwable primary=null;
        try{server.initiateShutdown();}
        catch(Throwable failure){primary=failure;throw failure;}
        finally{
            try{bridge.finishFreshInvocation(request,primary==null);}
            catch(RuntimeException | Error failure){if(primary==null)throw failure;if(primary!=failure)primary.addSuppressed(failure);}
        }
    }
    public static synchronized void requireCertifiedFreshShutdownInvocation(MinecraftServer server,Object runtime,
            DedicatedNormalStopBridge.Request request) {
        requireClaimedCallbacks(server);
        Binding binding=HOLDERS.get(server);
        if(binding.runtimeResponsibility!=runtime || binding.freshStopBridge==null)
            throw new IllegalStateException("Original Fresh exit installation required");
        binding.freshStopBridge.requireFreshInvocation(request);
    }
    public static void invokeStartupFailureShutdown(MinecraftServer server,DedicatedBootstrapBridge.Callbacks callbacks,
            DedicatedBootstrapBridge.FailureReport report) {
        Binding binding;
        synchronized(ProductionBootstrapInstallation.class){
            binding=HOLDERS.get(server);
            if(binding==null || binding.callbacks!=callbacks)throw new IllegalStateException("Original failed startup installation required");
            if(callbacks instanceof ProductionDedicatedBootstrapCallbacks legacy)legacy.requireOriginalStartupFailure(report);
            else if(callbacks instanceof ProductionDbFirstBootstrapCallbacks dbFirst)dbFirst.requireOriginalStartupFailure(report);
            else throw new IllegalStateException("Original production startup failure owner required");
            if(binding.startupFailureInvocationAttempted)throw new IllegalStateException("Startup failure invocation is one-shot");
            binding.startupFailureInvocationAttempted=true;
            binding.startupFailureInvoker=Thread.currentThread();
        }
        try{server.initiateShutdown();}
        finally{synchronized(ProductionBootstrapInstallation.class){binding.startupFailureInvoker=null;}}
    }
    private static String required(String key) {
        String value=System.getProperty(key);
        if(value==null || value.isBlank() || value.equalsIgnoreCase("UNSET") || value.equalsIgnoreCase("UNKNOWN"))
            throw new IllegalArgumentException("Explicit production launch input required: "+key);
        return value;
    }
    private static Duration duration(String key) {
        String value=required(key);if(!value.matches("[0-9]+"))throw new IllegalArgumentException("Finite launch timeout required: "+key);
        long millis=Long.parseLong(value);if(millis<=0 || millis>1800000)throw new IllegalArgumentException("Launch timeout outside finite bound: "+key);
        return Duration.ofMillis(millis);
    }
}
