package com.wi.floatingpod.server.source;

import com.wi.floatingpod.server.DedicatedWorldContext;
import com.wi.floatingpod.config.SourceLimitProfile;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.DimensionType;
import net.minecraft.world.WorldProvider;
import net.minecraft.world.WorldProviderSurface;
import net.minecraft.world.WorldProviderHell;
import net.minecraft.world.WorldProviderEnd;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.storage.AnvilSaveHandler;
import net.minecraft.world.chunk.storage.AnvilSaveConverter;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import net.minecraftforge.common.DimensionManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import static com.wi.floatingpod.server.source.SourceDomainCapture.*;

/** 实际 Main 独占的来源域观察；private raw refs 从不交给 worker 或公开 DTO。 */
public final class SourceDomainSession {
    private final Thread owner;
    private final SourceDomainIdentity identity;
    private final SourceLimitProfile limits;
    private final Probe probe;
    private State current;
    private CatalogDomainObservation cached;
    private final List<State> retired=new ArrayList<>();
    private long generation;
    private boolean closed;
    private SourceDomainSession(Thread owner,UUID boot,long bootGeneration,SourceLimitProfile limits,Probe probe) {
        this.owner=Objects.requireNonNull(owner);if(Thread.currentThread()!=owner)throw new IllegalStateException("Exact source Main required");
        this.identity=SourceDomainIdentity.issue(boot,bootGeneration);this.limits=Objects.requireNonNull(limits);this.probe=Objects.requireNonNull(probe);
    }
    /** actual server/context gate 先于 session 发行，不接受 caller 的 ready/complete 布尔值。 */
    public static SourceDomainSession open(MinecraftServer server,DedicatedWorldContext context,UUID boot,long bootGeneration,SourceLimitProfile limits) {
        Objects.requireNonNull(server);Objects.requireNonNull(context);
        context.validateAboutToStart(server);
        return new SourceDomainSession(server.getServerThread(),boot,bootGeneration,limits,new ActualProbe(server,context,limits));
    }
    /** 实际 Recall 使用原 shared root；旧 observation/test 入口不取得此原预算桥。 */
    public static SourceDomainSession open(MinecraftServer server,DedicatedWorldContext context,UUID boot,long bootGeneration,
            SourceLimitProfile limits,SourceForestAllocationBudget allocation) {
        Objects.requireNonNull(server);Objects.requireNonNull(context);Objects.requireNonNull(allocation);
        context.validateAboutToStart(server);
        if(allocation.exactProfile()!=limits)throw new IllegalArgumentException("Original Recall source profile required");
        allocation.reserveProfileCheck();
        return new SourceDomainSession(server.getServerThread(),boot,bootGeneration,limits,new ActualProbe(server,context,limits,allocation));
    }
    public SourceDomainIdentity identity(){return identity;}
    /** 当前实际 Main 的 shared profile只读投影，无来源域资格。 */
    public SourceLimitProfile limitsForObservation(){check();return limits;}
    public CatalogDomainObservation capture(){
        check();State before=probe.observe(),after=null;Throwable primary=null;
        try {
            after=probe.observe();
            if(!before.same(after)){invalidate();throw new IllegalStateException("Source registry/loader changed during Main capture");}
            if(current!=null&&current.same(after)){State unused=after;after=current;unused.discard();}
            else {if(current!=null){invalidate();retainPrevious();}current=after;}
        } catch(Throwable cause){primary=cause;if(after!=null&&after!=current)discardAfter(after,cause);throw cause;}
        finally {discardAfter(before,primary);}
        if(cached!=null)return cached;
        List<RootPlan> roots=new ArrayList<>();List<Unresolved> unresolved=new ArrayList<>();
        for(Entry entry:after.entries){if(entry.plan!=null)roots.add(entry.plan);unresolved.addAll(entry.unresolved);}
        RegistryStamp stamp=after.stamp;
        if(!stamp.hooksSeen())unresolved.add(new Unresolved(0,Issue.GENERATION_UNOBSERVED));
        if(stamp.mutations()!=0)unresolved.add(new Unresolved(0,Issue.REGISTRY_MUTATION_IN_PROGRESS));
        if(stamp.failure()!=null)unresolved.add(new Unresolved(0,Issue.REGISTRY_MUTATION_FAILED));
        unresolved.add(new Unresolved(0,Issue.NATIVE_EXCLUSIVITY_NOT_OBSERVED));
        unresolved.add(new Unresolved(0,Issue.DIRECTORY_MEMBERSHIP_NOT_OBSERVED));
        cached=SourceDomainCapture.issue(this,generation,stamp,roots,unresolved);return cached;
    }
    /** 每次使用前刷新真实引用/版本；变化撤销旧 observation，不把同值 ABA 当同代。 */
    public void requireCurrent(CatalogDomainObservation observation){
        check();if(observation==null||!observation.belongsTo(this))throw new IllegalArgumentException("Foreign source observation");
        State actual=probe.observe();Throwable primary=null;
        try {
            if(current==null||!current.same(actual)){invalidate();if(current!=null)retainPrevious();current=actual;}
            if(observation.observedGeneration()!=generation||observation.registryGeneration()!=actual.stamp.generation())
                throw new IllegalStateException("Source observation generation revoked");
            if(actual.stamp.mutations()!=0||actual.stamp.failure()!=null)throw new IllegalStateException("Source registry mutation unresolved",actual.stamp.failure());
        } catch(Throwable cause){primary=cause;throw cause;}
        finally {if(actual!=current)discardAfter(actual,primary);}
    }
    public void closeObservation(){check();invalidate();closed=true;}
    private static void discardAfter(State state,Throwable primary){
        try{state.discard();}catch(RuntimeException|Error cleanup){
            if(primary==null)throw cleanup;
            com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);
        }
    }
    /** 仅撤回尚未交出State的比较内存；不解除已发布来源观察或任何物理效果责任。 */
    private static void discardUnpublished(PlanBudget budget,Throwable primary){
        try{budget.discard();}catch(RuntimeException|Error cleanup){
            com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);
        }
    }
    private void invalidate(){generation=Math.addExact(generation,1);cached=null;}
    private void retainPrevious(){if(current.budget!=null)current.budget.reserve(128L);retired.add(current);}
    private void check(){if(Thread.currentThread()!=owner||closed)throw new IllegalStateException("Exact active source Main required");}
    // Package-only controlled probe tests exercise observation lifecycle, never world/native authority.
    interface Probe { State observe(); }
    static SourceDomainSession forObservationTest(Thread owner,UUID boot,SourceLimitProfile limits,Probe probe){return new SourceDomainSession(owner,boot,0,limits,probe);}
    static final class State {
        final RegistryStamp stamp;final Object server,converter;final Path root;final List<Entry> entries;final PlanBudget budget;
        State(RegistryStamp stamp,Object server,Object converter,Path root,List<Entry> entries){this(stamp,server,converter,root,entries,null);}
        State(RegistryStamp stamp,Object server,Object converter,Path root,List<Entry> entries,PlanBudget budget){this.stamp=stamp;this.server=server;this.converter=converter;this.root=root;this.entries=List.copyOf(entries);this.budget=budget;}
        void discard(){if(budget!=null)budget.discard();}
        boolean same(State other){
            if(stamp.generation()!=other.stamp.generation()||stamp.mutations()!=other.stamp.mutations()||stamp.hooksSeen()!=other.stamp.hooksSeen()||stamp.failure()!=other.stamp.failure()||server!=other.server||converter!=other.converter||!root.equals(other.root)||entries.size()!=other.entries.size())return false;
            for(int i=0;i<entries.size();i++)if(!entries.get(i).same(other.entries.get(i)))return false;return true;
        }
    }
    static final class Entry {
        final int dimension;final Object type,world,provider,handler,loader;final RootPlan plan;final List<Unresolved> unresolved;
        Entry(int dimension,Object type,Object world,Object provider,Object handler,Object loader,RootPlan plan,List<Unresolved> unresolved){this.dimension=dimension;this.type=type;this.world=world;this.provider=provider;this.handler=handler;this.loader=loader;this.plan=plan;this.unresolved=List.copyOf(unresolved);}
        boolean same(Entry o){return dimension==o.dimension&&type==o.type&&world==o.world&&provider==o.provider&&handler==o.handler&&loader==o.loader&&Objects.equals(plan,o.plan)&&unresolved.equals(o.unresolved);}
    }
    private static final class ActualProbe implements Probe {
        final MinecraftServer server;final DedicatedWorldContext context;final SourceLimitProfile limits;final SourceForestAllocationBudget allocation;
        ActualProbe(MinecraftServer server,DedicatedWorldContext context,SourceLimitProfile limits){this(server,context,limits,null);}
        ActualProbe(MinecraftServer server,DedicatedWorldContext context,SourceLimitProfile limits,SourceForestAllocationBudget allocation){this.server=server;this.context=context;this.limits=limits;this.allocation=allocation;}
        public State observe(){
            context.validateAboutToStart(server);Integer[] registered=DimensionManager.getStaticDimensionIDs();Integer[] loaded=DimensionManager.getIDs();
            DimensionInventory inventory=dimensionInventory(registered,loaded,limits,allocation);
            try {
                registered=inventory.registered;List<Entry> entries=new ArrayList<>();Path root=context.worldDirectory().normalize();inventory.budget.retainRoot(root);
                for(int dimension:registered){DimensionType type=DimensionManager.getProviderType(dimension);WorldServer world=DimensionManager.getWorld(dimension);
                    WorldProvider actual=world==null?null:world.provider;Object handler=world==null?null:world.getSaveHandler();Object loader=world==null?null:world.getChunkProvider().chunkLoader;
                    List<Unresolved> unresolved=new ArrayList<>();WorldProvider standard=standardProvider(type);
                    if(world==null&&server.getActiveAnvilConverter().getClass()!=AnvilSaveConverter.class)unresolved.add(new Unresolved(dimension,Issue.SAVE_HANDLER_STORAGE_UNPROVEN));
                    if(standard==null){unresolved.add(new Unresolved(dimension,Issue.CUSTOM_PROVIDER_STORAGE_UNPROVEN));inventory.budget.retainEntry(null,unresolved);entries.add(new Entry(dimension,type,world,actual,handler,loader,null,unresolved));continue;}
                    standard.setDimension(dimension);String folder=standard.getSaveFolder();Path directory=folder==null?root:root.resolve(folder).normalize();
                    if(world!=null&&(actual==null||actual.getClass()!=standard.getClass()||actual.getDimension()!=dimension||actual.getDimensionType()!=type))unresolved.add(new Unresolved(dimension,Issue.LOADED_PROVIDER_MISMATCH));
                    if(world!=null){
                        if(world.getMinecraftServer()!=server)unresolved.add(new Unresolved(dimension,Issue.LOADED_SERVER_MISMATCH));
                        if(handler==null||handler.getClass()!=AnvilSaveHandler.class)unresolved.add(new Unresolved(dimension,Issue.SAVE_HANDLER_STORAGE_UNPROVEN));
                        else if(!root.equals(world.getSaveHandler().getWorldDirectory().toPath().toAbsolutePath().normalize()))unresolved.add(new Unresolved(dimension,Issue.SAVE_ROOT_MISMATCH));
                        if(loader==null||loader.getClass()!=AnvilChunkLoader.class)unresolved.add(new Unresolved(dimension,Issue.LOADER_STORAGE_UNPROVEN));
                        else if(!directory.equals(((AnvilChunkLoader)loader).chunkSaveLocation.toPath().toAbsolutePath().normalize()))unresolved.add(new Unresolved(dimension,Issue.LOADER_PATH_MISMATCH));
                    }
                    RootPlan plan=unresolved.isEmpty()?new RootPlan(dimension,type.getName(),directory,directory.resolve("region"),world!=null):null;
                    inventory.budget.retainEntry(plan,unresolved);entries.add(new Entry(dimension,type,world,actual,handler,loader,plan,unresolved));
                }
                for(int dimension:inventory.loadedUnregistered){
                    List<Unresolved> unresolved=List.of(new Unresolved(dimension,Issue.LOADED_UNREGISTERED_DIMENSION));
                    inventory.budget.retainEntry(null,unresolved);
                    entries.add(new Entry(dimension,null,DimensionManager.getWorld(dimension),null,null,null,null,unresolved));
                }
                return new State(REGISTRY.snapshot(),server,server.getActiveAnvilConverter(),root,entries,inventory.budget);
            } catch(RuntimeException|Error cause){
                discardUnpublished(inventory.budget,cause);throw cause;
            }
        }
    }
    /** ActualProbe uses this admission before allocating its own sorted arrays and retained entries.
     * maxMembers belongs to NBT members, not dimensions. No dimension-count policy is invented. */
    static DimensionInventory dimensionInventory(Integer[] registeredInput,Integer[] loadedInput,SourceLimitProfile limits){
        return dimensionInventory(registeredInput,loadedInput,limits,null);
    }
    static DimensionInventory dimensionInventory(Integer[] registeredInput,Integer[] loadedInput,SourceLimitProfile limits,SourceForestAllocationBudget allocation){
        Objects.requireNonNull(registeredInput);Objects.requireNonNull(loadedInput);Objects.requireNonNull(limits);
        PlanBudget budget=new PlanBudget(limits.maxRetainedBytes(),allocation);
        try {
            budget.reserve(Math.addExact(512L,Math.multiplyExact(32L,Math.addExact((long)registeredInput.length,loadedInput.length))));
            Integer[] registered=registeredInput.clone(),loaded=loadedInput.clone();
            for(Integer id:registered)Objects.requireNonNull(id,"Null registered dimension");
            for(Integer id:loaded)Objects.requireNonNull(id,"Null loaded dimension");
            Arrays.sort(registered);Arrays.sort(loaded);
            for(int i=1;i<registered.length;i++)if(registered[i].equals(registered[i-1]))throw new IllegalArgumentException("Duplicate registered dimension");
            for(int i=1;i<loaded.length;i++)if(loaded[i].equals(loaded[i-1]))throw new IllegalArgumentException("Duplicate loaded dimension");
            int orphanCount=0;for(int id:loaded)if(Arrays.binarySearch(registered,id)<0)orphanCount=Math.addExact(orphanCount,1);
            long union=Math.addExact((long)registered.length,orphanCount);
            // Reservation includes raw refs/plan slots and four simultaneously retained capture representations.
            budget.reserve(Math.multiplyExact(2048L,union));
            int[] orphan=new int[orphanCount];int index=0;for(int id:loaded)if(Arrays.binarySearch(registered,id)<0)orphan[index++]=id;
            return new DimensionInventory(registered,orphan,budget);
        } catch(RuntimeException|Error cause){
            discardUnpublished(budget,cause);throw cause;
        }
    }
    static final class DimensionInventory {
        final Integer[] registered;final int[] loadedUnregistered;final PlanBudget budget;
        private DimensionInventory(Integer[] registered,int[] loadedUnregistered,PlanBudget budget){this.registered=registered;this.loadedUnregistered=loadedUnregistered;this.budget=budget;}
    }
    /** Explicit conservative retained-byte accounting, not an exact JVM heap-size measurement. */
    static final class PlanBudget {
        private final long maximum;private long reserved;private final SourceForestAllocationBudget allocation;private boolean discarded;private Throwable cleanupFailure;
        PlanBudget(long maximum){this(maximum,null);}
        PlanBudget(long maximum,SourceForestAllocationBudget allocation){this.maximum=maximum;this.allocation=allocation==null?null:allocation.isolate();}
        void reserve(long bytes){
            if(discarded||cleanupFailure!=null)throw new IllegalStateException("Source plan budget already discarded or failed",cleanupFailure);
            long next=Math.addExact(reserved,bytes);if(bytes<0||next>maximum)throw new IllegalStateException("Source dimension plan retained-byte budget exhausted");
            try{if(allocation!=null)allocation.reserveTemporary(bytes,"actual Main source domain state BEFORE allocation");}
            catch(java.io.IOException cause){throw new java.io.UncheckedIOException(cause);}reserved=next;
        }
        void discard(){
            if(discarded||cleanupFailure!=null)throw new IllegalStateException("Source plan budget already discarded or failed",cleanupFailure);
            try{
                if(allocation!=null)allocation.releaseTemporary(reserved,"unretained Main source domain comparison dropped");
                reserved=0;
                if(allocation!=null)allocation.retire();
                discarded=true;
            }catch(java.io.IOException cause){cleanupFailure=cause;throw new java.io.UncheckedIOException(cause);}
            catch(RuntimeException|Error cause){cleanupFailure=cause;throw cause;}
        }
        void retainRoot(Path root){reserve(Math.multiplyExact(4L,textCharge(root.toString())));}
        void retainEntry(RootPlan plan,List<Unresolved> issues){
            long charge=Math.addExact(8L,Math.multiplyExact(32L,issues.size()));
            if(plan!=null)charge=Math.addExact(charge,Math.addExact(textCharge(plan.providerType()),Math.addExact(textCharge(plan.dimensionDirectory().toString()),textCharge(plan.regionDirectory().toString()))));
            reserve(Math.multiplyExact(4L,charge));
        }
        long reservedBytes(){return reserved;}
        private static long textCharge(String text){
            long utf8=0;
            for(int i=0;i<text.length();i++){char c=text.charAt(i);
                if(c<0x80)utf8=Math.addExact(utf8,1);else if(c<0x800)utf8=Math.addExact(utf8,2);
                else if(Character.isHighSurrogate(c)){if(i+1>=text.length()||!Character.isLowSurrogate(text.charAt(++i)))throw new IllegalArgumentException("Invalid plan text surrogate");utf8=Math.addExact(utf8,4);}
                else if(Character.isLowSurrogate(c))throw new IllegalArgumentException("Invalid plan text surrogate");else utf8=Math.addExact(utf8,3);
            }
            // length-framed UTF8 plus retained UTF16 string/path storage allowance, without allocating encoded bytes.
            return Math.addExact(100L,Math.addExact(utf8,Math.multiplyExact(2L,text.length())));
        }
    }
    /** 不调用 DimensionType.createDimension 的反射，也不初始化 cold world/loader/目录。 */
    static WorldProvider standardProvider(DimensionType type){
        if(type==DimensionType.OVERWORLD)return new WorldProviderSurface();
        if(type==DimensionType.NETHER)return new WorldProviderHell();
        if(type==DimensionType.THE_END)return new WorldProviderEnd();return null;
    }
}
