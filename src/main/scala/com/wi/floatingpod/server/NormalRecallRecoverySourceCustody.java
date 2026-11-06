package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.bootstrap.ManagedSourcePathCensus;
import com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks;
import com.wi.floatingpod.server.source.SourceAnvilReadAccess;
import com.wi.floatingpod.server.source.SourceDomainCapture;
import com.wi.floatingpod.server.source.SourceDomainSession;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import com.wi.floatingpod.server.source.SourceLoadManifest;
import com.wi.floatingpod.server.save.ManagedFileIoOwnerAccess;
import com.wi.floatingpod.server.save.ManagedSaveAccess;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import com.wi.floatingpod.storage.mysql.MySqlOwnedIoAdapter;
import com.wi.floatingpod.storage.mysql.MySqlBootSourceProtectionIndex;
import com.wi.floatingpod.storage.mysql.MySqlDbFirstMainReadBacking;
import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import net.minecraft.world.storage.ThreadedFileIOBase;
import net.minecraftforge.common.DimensionManager;

/** 原 Main 的来源观察责任；capture 和安装都不发行 COMPLETE/absence/write 资格。 */
public final class NormalRecallRecoverySourceCustody {
    /** Stable preinitialized identity shared only with the exact attached cold census. */
    public static final class ColdSourceObserver {
        private final NormalRecallRecoverySourceCustody issuer;
        private ColdSourceObserver(NormalRecallRecoverySourceCustody issuer){this.issuer=Objects.requireNonNull(issuer);}
        private boolean issuedBy(NormalRecallRecoverySourceCustody actual){return issuer==actual;}
    }








    /** Opaque Main-owned reservation for one finite Admission holder. */
    public static final class AdmissionCharge implements AutoCloseable {
        private final Thread main;
        private NormalRecallRecoverySourceCustody issuer;
        private MainBootProjection.SourceContinuationLease lease;
        private MainBootProjection.SourceContinuationObservation observation;
        private SourceForestAllocationBudget.Account account;
        private SourceLimitProfile profile;
        private long bytes;
        private String purpose;
        private Throwable failure;
        private boolean closed;

        private AdmissionCharge(NormalRecallRecoverySourceCustody issuer,
                MainBootProjection.SourceContinuationLease lease,
                MainBootProjection.SourceContinuationObservation observation,
                SourceForestAllocationBudget.Account account,SourceLimitProfile profile,long bytes,String purpose){
            this.issuer=Objects.requireNonNull(issuer);this.main=issuer.main;
            this.lease=Objects.requireNonNull(lease);this.observation=Objects.requireNonNull(observation);
            this.account=Objects.requireNonNull(account);this.profile=Objects.requireNonNull(profile);
            this.bytes=bytes;this.purpose=Objects.requireNonNull(purpose);
        }

        /** Retire the opaque holder on its original Main before returning its exact charged bytes. */
        @Override public synchronized void close()throws IOException{
            if(Thread.currentThread()!=main)throw new IllegalStateException("Original Main required to retire Admission source charge");
            if(failure!=null)throwOriginal(failure);
            if(closed)return;
            NormalRecallRecoverySourceCustody exactIssuer=Objects.requireNonNull(issuer,"Admission charge issuer");
            MainBootProjection.SourceContinuationLease exactLease=Objects.requireNonNull(lease,"Admission charge continuation lease");
            MainBootProjection.SourceContinuationObservation exactObservation=Objects.requireNonNull(observation,"Admission charge source observation");
            SourceForestAllocationBudget.Account exactAccount=Objects.requireNonNull(account,"Admission charge account");
            SourceLimitProfile exactProfile=Objects.requireNonNull(profile,"Admission charge profile");
            String exactPurpose=Objects.requireNonNull(purpose,"Admission charge purpose");
            long exactBytes=bytes;
            exactIssuer.requireAdmissionChargeCloseOwner(exactLease,exactObservation,exactAccount,exactProfile);
            // Drop the retained observation/lease and holder-account references before the refund boundary.
            issuer=null;lease=null;observation=null;account=null;profile=null;purpose=null;bytes=0L;closed=true;
            try{
                exactAccount.releaseTemporary(exactBytes,exactPurpose);
            }catch(IOException|RuntimeException|Error cause){failure=cause;throwOriginal(cause);}
        }

        private static void throwOriginal(Throwable cause)throws IOException{
            if(cause instanceof IOException io)throw io;
            if(cause instanceof RuntimeException runtime)throw runtime;
            if(cause instanceof Error error)throw error;
            throw new IOException("Admission source charge retirement failed",cause);
        }
    }




    private final Thread main;
    private final MinecraftServer server;
    private final DedicatedWorldContext context;
    private final MySqlOwnedIoAdapter.RecallRecoveryLink link;
    private final MySqlDbFirstMainReadBacking dbFirstBacking;
    private volatile MainBootProjection.SourceContinuationLease continuationLease;
    private boolean allocationBound;
    private final SourceLimitProfile profile;
    private SourceForestAllocationBudget allocation;
    private final SourceForestAllocationBudget.Root allocationRoot;
    private final ColdSourceObserver coldSourceObserver;
    private ManagedSourcePathCensus coldCensus,partialColdCensus;
    private SourceForestAllocationBudget.Account coldCensusAccount,partialColdCensusAccount;
    private MySqlBootSourceProtectionIndex.RetainedData retainedBootProtectionData;
    private MainBootProjection retainedProtectionProjection;
    private MainBootProjection.SourceContinuationLease retainedProtectionLease;
    private final IdentityHashMap<AnvilChunkLoader,LoaderSlot> loaders=new IdentityHashMap<>();
    private final List<LoaderSlot> partials=new ArrayList<>();
    private SourceDomainSession domain;
    private SourceDomainCapture.CatalogDomainObservation observation;
    private boolean initializationAttempted;
    private boolean stoppingSourceWork;
    private Throwable stoppingSourceFailure;
    private Throwable readDetachFailure;
    private Throwable saveDetachFailure;
    private volatile Throwable primary;
    private NormalRecallRecoverySourceCustody(MinecraftServer server,DedicatedWorldContext context,
            MySqlOwnedIoAdapter.RecallRecoveryLink link,SourceLimitProfile profile,SourceForestAllocationBudget.Root allocationRoot){
        this(server,context,link,null,profile,allocationRoot);
    }
    private NormalRecallRecoverySourceCustody(MinecraftServer server,DedicatedWorldContext context,
            MySqlOwnedIoAdapter.RecallRecoveryLink link,MySqlDbFirstMainReadBacking dbFirstBacking,
            SourceLimitProfile profile,SourceForestAllocationBudget.Root allocationRoot){
        this.main=server.getServerThread();this.server=server;this.context=context;
        if((link==null)==(dbFirstBacking==null))throw new IllegalArgumentException("Exactly one original source owner family required");
        this.link=link;this.dbFirstBacking=dbFirstBacking;this.profile=profile;this.allocationRoot=allocationRoot;this.coldSourceObserver=new ColdSourceObserver(this);
    }
    static NormalRecallRecoverySourceCustody retain(MainBootProjection projection,MinecraftServer server,DedicatedWorldContext context,
            MySqlOwnedIoAdapter.RecallRecoveryLink link,SourceLimitProfile profile,SourceForestAllocationBudget.Root rootAllocation)throws IOException{
        Objects.requireNonNull(projection);Objects.requireNonNull(server);Objects.requireNonNull(context);Objects.requireNonNull(link);Objects.requireNonNull(rootAllocation);
        if(Thread.currentThread()!=server.getServerThread() || context.actualServer()!=server || rootAllocation.exactProfile()!=profile)
            throw new IllegalStateException("Original Recall Main/world/root profile required");
        projection.requireCurrentReads();context.validateAboutToStart(server);
        rootAllocation.prepareCustody(server,context);
        NormalRecallRecoverySourceCustody actual=new NormalRecallRecoverySourceCustody(server,context,link,profile,rootAllocation);
        try{
            actual.allocation=rootAllocation.bindCustody(actual);
            actual.allocationBound=true;
            return actual;
        }catch(RuntimeException|Error cause){actual.retainFailure(cause);throw cause;}
    }
    /** 原schema6 callback已经领用的同一backing/root；没有伪造RecallRecoveryLink。 */
    public static NormalRecallRecoverySourceCustody retainDbFirst(ProductionDbFirstBootstrapCallbacks callbacks,
            MySqlDbFirstMainReadBacking backing,MinecraftServer server,DedicatedWorldContext context,
            SourceForestAllocationBudget.Root root)throws IOException{
        Objects.requireNonNull(callbacks);Objects.requireNonNull(backing);Objects.requireNonNull(root);
        callbacks.requireOriginalSourceCustodyRetention(backing,root,server,context);
        root.prepareCustody(server,context);
        NormalRecallRecoverySourceCustody actual=new NormalRecallRecoverySourceCustody(server,context,null,backing,root.exactProfile(),root);
        try{
            actual.allocation=root.bindCustody(actual);
            actual.allocationBound=true;
            return actual;
        }catch(RuntimeException|Error cause){actual.retainFailure(cause);throw cause;}
    }
    /** Projection binds the exact retained continuation only after this custody/root is cached. */
    synchronized void bindSourceContinuationLease(MainBootProjection.SourceContinuationLease exactLease){
        if(Thread.currentThread()!=main||dbFirstBacking!=null||!allocationBound||exactLease==null||continuationLease!=null)
            throw new IllegalStateException("One exact Main-bound source continuation lease is required after custody/root retention");
        try{
            exactLease.requireOriginalCustodyBinding(server,link,this);
            continuationLease=exactLease;
        }catch(RuntimeException|Error cause){retainFailure(cause);throw cause;}
    }
    /** 仅用于首次Root.bindCustody，不能用来复核已经成功绑定的子账户签发。 */
    public void requireAllocationBinding(SourceForestAllocationBudget.Root original,MinecraftServer actualServer,DedicatedWorldContext actualContext){
        requireMain();if(primary!=null)throw new IllegalStateException("Original source responsibility failed",primary);
        if(allocationBound||original!=allocationRoot||actualServer!=server||actualContext!=context||
                Thread.currentThread()!=server.getServerThread()||context.actualServer()!=server)
            throw new IllegalStateException("Original custody/root binding required");
    }
    /** cold census子账户沿用已绑定的原custody；不再次执行初次绑定或发行新root。 */
    public void requireBoundAllocation(SourceForestAllocationBudget.Root original,MinecraftServer actualServer,DedicatedWorldContext actualContext){
        requireMain();if(primary!=null)throw new IllegalStateException("Original source responsibility failed",primary);
        if(!allocationBound||allocation==null||original!=allocationRoot||actualServer!=server||actualContext!=context||
                Thread.currentThread()!=server.getServerThread()||context.actualServer()!=server)
            throw new IllegalStateException("Original bound custody/root required");
    }
    /** Attach the one real path-census owner to this preinitialized custody and its original account tree. */
    public synchronized SourceForestAllocationBudget.Account bindColdSourceCensus(ManagedSourcePathCensus exactCensus)throws IOException{
        if(stoppingSourceWork)throw new IllegalStateException("Original source custody is stopping");
        current();Objects.requireNonNull(exactCensus);
        if(coldCensus==exactCensus&&coldCensusAccount!=null)return coldCensusAccount;
        if(coldCensus!=null)throw new IllegalStateException("Original custody can bind one cold source census only");
        exactCensus.requireExactCustody(this);
        coldCensus=exactCensus;partialColdCensus=exactCensus;
        try{
            SourceForestAllocationBudget.Account account=allocationRoot.isolateForColdCensus(this,exactCensus);
            partialColdCensusAccount=account;coldCensusAccount=account;partialColdCensus=null;partialColdCensusAccount=null;
            return account;
        }catch(IOException|RuntimeException|Error cause){retainFailure(cause);throw cause;}
    }
    /** Return the one preinitialized cold observation identity only to this custody's attached census. */
    public synchronized ColdSourceObserver requireColdSourceObserver(ManagedSourcePathCensus exactCensus){
        current();Objects.requireNonNull(exactCensus);
        if(coldCensus!=exactCensus||coldCensusAccount==null||!coldSourceObserver.issuedBy(this))
            throw new IllegalStateException("Exact attached cold source census and original custody observer required");
        exactCensus.requireExactCustody(this);
        return coldSourceObserver;
    }

    /** Charge a finite Admission holder to the original custody account without exposing that account. */
    synchronized AutoCloseable retainAdmissionCharge(
            MainBootProjection.SourceContinuationLease exactLease,
            MainBootProjection.SourceContinuationObservation exactObservation,long bytes,String purpose)throws IOException{
        requireMain();Objects.requireNonNull(exactLease);Objects.requireNonNull(exactObservation);
        Objects.requireNonNull(purpose);
        if(bytes<=0L||purpose.isBlank())throw new IllegalArgumentException("Positive original-account Admission charge and purpose required");
        current();
        if(!allocationBound||exactLease!=continuationLease)
            throw new IllegalStateException("Exact registered source continuation lease required for Admission charge");
        exactLease.requireAdmissionChargeObservation(this,exactObservation);
        exactLease.requireOriginalCustodyBinding(server,link,this);
        exactLease.requireResponsibilityOwner(server,link);
        if(!(allocation instanceof SourceForestAllocationBudget.Account exactAccount))
            throw new IllegalStateException("Original custody Account required for Admission charge");
        if(exactAccount.exactProfile()!=profile)throw new IllegalStateException("Original Admission charge profile changed");
        try{
            exactAccount.reserveTemporary(bytes,purpose);
            try{return new AdmissionCharge(this,exactLease,exactObservation,exactAccount,profile,bytes,purpose);}
            catch(RuntimeException|Error cause){
                try{exactAccount.releaseTemporary(bytes,purpose);}
                catch(IOException|RuntimeException|Error cleanup){if(cleanup!=cause)ManagedSourceSaveLedger.suppressSecondary(cause,cleanup);}
                retainFailure(cause);throw cause;
            }
        }catch(IOException|RuntimeException|Error cause){retainFailure(cause);throw cause;}
    }

    private synchronized void requireAdmissionChargeCloseOwner(
            MainBootProjection.SourceContinuationLease exactLease,
            MainBootProjection.SourceContinuationObservation exactObservation,
            SourceForestAllocationBudget.Account exactAccount,SourceLimitProfile exactProfile){
        requireMain();current();
        if(exactLease==null||exactObservation==null||exactAccount==null||exactProfile!=profile||
                exactLease!=continuationLease||allocation!=exactAccount||exactAccount.exactProfile()!=profile)
            throw new IllegalStateException("Exact original continuation, observation, profile, and custody charge account required");
        exactLease.requireOriginalCustodyBinding(server,link,this);
        exactLease.requireAdmissionChargeObservation(this,exactObservation);
        exactLease.requireResponsibilityOwner(server,link);
    }
    /** Worker/IO accounts only inspect retained responsibility failure; Main revalidation stays in current(). */
    public void requireAllocationResponsibility(){if(primary!=null)throw new IllegalStateException("Original source custody failed",primary);}
    public void requireAllocationOwner(){
        requireMain();if(primary!=null)throw new IllegalStateException("Original source responsibility failed",primary);
        if(!allocationBound){
            if(Thread.currentThread()!=server.getServerThread())throw new IllegalStateException("Original custody/root Main required");
            context.validateAboutToStart(server);return;
        }
        current();
    }
    private void current(){
        requireMain();
        if(primary!=null)throw new IllegalStateException("Original Recall source responsibility failed",primary);
        if(dbFirstBacking!=null)dbFirstBacking.requireSourceAllocationOwner(allocationRoot);
        else {
            MainBootProjection.SourceContinuationLease actual=continuationLease;
            if(actual==null)throw new IllegalStateException("Original source continuation lease is not registered");
            actual.requireResponsibilityOwner(server,link);
        }
        context.validateAboutToStart(server);
    }
    private void requireMain(){if(Thread.currentThread()!=main)throw new IllegalStateException("Original Recall source Main required");}
    /** 第一次真实 END 才观察实际 loaded worlds；初始化失败的原槽不会再次发行。 */
    public void captureCurrentDomain()throws IOException{
        if(stoppingSourceWork)throw new IllegalStateException("Original source custody is stopping");
        current();
        try{
            if(!initializationAttempted){
                initializationAttempted=true;
                domain=SourceDomainSession.open(server,context,sourceBootId(),sourceBootGeneration(),profile,allocation);
                captureDomainActual(domain.capture());
            }else requireCurrentDomain();
        }catch(IOException|RuntimeException|Error cause){retainFailure(cause);throw cause;}
    }
    public void captureDomain(MySqlOwnedIoAdapter.RecallRecoveryLink original,SourceDomainCapture.CatalogDomainObservation actual)throws IOException{
        current();
        if(link==null||original!=link)throw new IllegalStateException("Original Recall source capture owner required");
        captureDomainActual(actual);
    }
    private void captureDomainActual(SourceDomainCapture.CatalogDomainObservation actual)throws IOException{
        if(stoppingSourceWork)throw new IllegalStateException("Original source custody is stopping");
        current();
        if(domain==null || observation!=null)throw new IllegalStateException("Original one-shot Main source capture required");
        domain.requireCurrent(actual);observation=Objects.requireNonNull(actual);
        for(SourceDomainCapture.RootPlan root:actual.roots())if(root.loaded())installLoaded(root);
        requireCurrentDomain();
    }
    private void installLoaded(SourceDomainCapture.RootPlan root)throws IOException{
        if(stoppingSourceWork)throw new IllegalStateException("Original source custody is stopping");
        domain.requireCurrent(observation);
        WorldServer world=DimensionManager.getWorld(root.dimension());
        if(world==null || world.getMinecraftServer()!=server || !(world.getChunkProvider().chunkLoader instanceof AnvilChunkLoader loader))
            throw new IllegalStateException("Actual captured source world/Anvil loader changed");
        if(!loader.chunkSaveLocation.toPath().toAbsolutePath().normalize().equals(root.dimensionDirectory()))
            throw new IllegalStateException("Actual captured source loader path changed");
        if(loaders.containsKey(loader))throw new IllegalStateException("Captured dimensions share an actual loader");
        Object io=ThreadedFileIOBase.getThreadedIOInstance();
        if(!(io instanceof ThreadedFileIOBase) || !(io instanceof ManagedFileIoOwnerAccess owner) ||
                !((Object)loader instanceof ManagedSaveAccess saves) ||
                !((Object)loader instanceof SourceAnvilReadAccess reads))
            throw new IllegalStateException("Actual transformed source read/save/File IO owners required");
        Thread actualIo=owner.floatingpod$actualStartedIoThread();
        allocation.reserveTemporary(512L,"original actual loader source installation partial slot BEFORE allocation");
        LoaderSlot slot=new LoaderSlot(world,loader,root);partials.add(slot);loaders.put(loader,slot);
        slot.ledger=ManagedSourceSaveLedger.forRecall(sourceBootId(),main,loader,actualIo,profile,allocation);
        saves.floatingpod$installSaveLedger(slot.ledger);
        slot.manifest=SourceLoadManifest.install(server,world,loader,domain,observation,allocation);
        reads.floatingpod$installSourceLoadManifest(slot.manifest);
        domain.requireCurrent(observation);
    }
    /** 原Main封所有已保留的安装槽，包括partial；失败仍尝试封其余槽，绝不卸载或退款。 */
    public void beginStoppingSourceWork(){
        requireMain();
        if(stoppingSourceWork){
            if(stoppingSourceFailure!=null)throw new IllegalStateException("Original source admission stop failed",stoppingSourceFailure);
            return;
        }
        stoppingSourceWork=true;
        Throwable failure=null;
        // census覆盖尚未进入loaded manifest的cold/Forge工作；封门失败也要继续封实际槽。
        ManagedSourcePathCensus census=coldCensus!=null?coldCensus:partialColdCensus;
        if(census!=null)try{census.beginClosing();}
        catch(RuntimeException|Error cause){failure=cause;}
        for(LoaderSlot slot:partials){
            if(slot.manifest!=null)try{slot.manifest.beginStopping(slot.loader);}
            catch(RuntimeException|Error cause){if(failure==null)failure=cause;else ManagedSourceSaveLedger.suppressSecondary(failure,cause);}
            if(slot.ledger!=null)try{slot.ledger.beginStoppingObservation();}
            catch(RuntimeException|Error cause){if(failure==null)failure=cause;else ManagedSourceSaveLedger.suppressSecondary(failure,cause);}
        }
        if(failure!=null){stoppingSourceFailure=failure;retainFailure(failure);if(failure instanceof RuntimeException cause)throw cause;throw (Error)failure;}
    }
    /** 只撤回实际loader的读consumer；原manifest、观察及费用继续由本custody保留。 */
    public boolean tryDetachStoppedReadConsumers(){
        requireMain();
        if(!stoppingSourceWork||stoppingSourceFailure!=null)
            throw new IllegalStateException("Original source admission has not stopped successfully",stoppingSourceFailure);
        if(readDetachFailure!=null)throw new IllegalStateException("Original source read detach failed",readDetachFailure);
        if(coldCensus==null||partialColdCensus!=null)
            throw new IllegalStateException("Original complete source census binding required before consumer detach");
        // -1是实际工作仍在途，不是结算失败；后续原Main可在真实ACK到达后再检查。
        if(coldCensus.tryExclusive()<0)return false;
        try{
            for(LoaderSlot slot:partials)if(slot.manifest!=null&&!slot.readDetached)
                if(!slot.manifest.pollStoppedReads(slot.loader))return false;
            for(LoaderSlot slot:partials)if(slot.manifest!=null&&!slot.readDetached){
                if(!((Object)slot.loader instanceof SourceAnvilReadAccess reads))
                    throw new IllegalStateException("Original transformed source loader missing during detach");
                reads.floatingpod$detachSourceLoadManifest(slot.manifest);
                slot.readDetached=true;
            }
            return true;
        }catch(RuntimeException|Error cause){readDetachFailure=cause;retainFailure(cause);throw cause;}
    }
    /** 读consumer先解除；实际save queue/IO仍在途时保留安装，成功亦不清历史写入责任。 */
    public boolean tryDetachStoppedSaveConsumers(){
        requireMain();
        if(saveDetachFailure!=null)throw new IllegalStateException("Original source save detach failed",saveDetachFailure);
        if(!tryDetachStoppedReadConsumers())return false;
        try{
            boolean complete=true;
            for(LoaderSlot slot:partials)if(slot.ledger!=null&&!slot.saveDetached){
                if(!((Object)slot.loader instanceof ManagedSaveAccess saves))
                    throw new IllegalStateException("Original transformed save loader missing during detach");
                if(saves.floatingpod$tryDetachSaveLedger(slot.ledger))slot.saveDetached=true;
                else complete=false;
            }
            return complete;
        }catch(RuntimeException|Error cause){saveDetachFailure=cause;retainFailure(cause);throw cause;}
    }
    /** Domain only retains the actual loaded worlds, registries, and manifest installation. */
    public void requireCurrentDomain(){
        current();if(domain==null || observation==null)throw new IllegalStateException("Original Main source domain not captured");
        domain.requireCurrent(observation);
        for(LoaderSlot slot:partials){
            if(slot.manifest==null || slot.ledger==null ||
                    slot.world.getChunkProvider().chunkLoader!=slot.loader || slot.world.getMinecraftServer()!=server)
                throw new IllegalStateException("Original source installation incomplete or loader changed");
            if(slot.loadedRoot==null || !slot.loadedRoot.loaded() || slot.loadedRoot.dimension()!=slot.world.provider.getDimension())
                throw new IllegalStateException("Original source loaded-root plan changed");
            slot.manifest.requireCurrentDomain();
        }
    }
    /** Copy only the original Boot protection DATA into this custody's preinitialized account. */
    synchronized MySqlBootSourceProtectionIndex.RetainedData retainProtectionForContinuation(
            MainBootProjection exactProjection,MainBootProjection.SourceContinuationLease exactLease,
            MySqlBootSourceProtectionIndex exactIndex){
        requireMain();
        try{
            Objects.requireNonNull(exactProjection);Objects.requireNonNull(exactLease);Objects.requireNonNull(exactIndex);
            if(dbFirstBacking!=null||exactLease!=continuationLease||!allocationBound)
                throw new IllegalStateException("Exact original projection/continuation/custody binding required for source protection retention");
            exactLease.requireOriginalCustodyBinding(server,link,this);
            exactLease.requireResponsibilityOwner(server,link);
            context.validateAboutToStart(server);
            if(retainedBootProtectionData!=null){
                if(retainedProtectionProjection==exactProjection&&retainedProtectionLease==exactLease)
                    return retainedBootProtectionData;
                throw new IllegalStateException("Original Boot source protection DATA is retained for another continuation binding");
            }
            exactProjection.requireCurrentReads();
            exactProjection.requireOriginalSourceProtectionIndex(exactIndex);
            if(!(allocation instanceof SourceForestAllocationBudget.Account exactAccount))
                throw new IllegalStateException("Original retained custody Account required for Boot source protection DATA");
            if(exactAccount.exactProfile()!=profile)throw new IllegalStateException("Original Boot source protection profile changed");
            MySqlBootSourceProtectionIndex.RetainedData retained=exactIndex.retainForContinuation(profile,exactAccount,this);
            retainedProtectionProjection=exactProjection;retainedProtectionLease=exactLease;
            retainedBootProtectionData=retained;
            return retained;
        }catch(RuntimeException|Error cause){retainFailure(cause);throw cause;}
    }
    /** Exact already-captured world/domain DATA for the continuation lease; never a Boot-row refresh. */
    SourceDomainCapture.CatalogDomainObservation currentDomainObservation(){
        requireCurrentDomain();return observation;
    }
    private java.util.UUID sourceBootId(){return dbFirstBacking==null?link.identity().bootId():dbFirstBacking.acquisitionTag().bootId();}
    private long sourceBootGeneration(){return dbFirstBacking==null?link.identity().bootGeneration():dbFirstBacking.acquisitionTag().bootGeneration();}

    /** 只复制原Full27保护DATA；SQL freshness/epoch仍由backing在每次使用时检查。 */
    public synchronized void retainDbFirstProtection(MySqlDbFirstMainReadBacking original){
        current();
        if(original==null||original!=dbFirstBacking||!allocationBound||retainedBootProtectionData!=null)
            throw new IllegalStateException("Original one-shot schema6 custody protection required");
        if(!(allocation instanceof SourceForestAllocationBudget.Account account))
            throw new IllegalStateException("Original schema6 custody account required");
        try{retainedBootProtectionData=original.data().sourceProtection().retainForContinuation(profile,account,this);}
        catch(RuntimeException|Error cause){retainFailure(cause);throw cause;}
    }
    public MySqlBootSourceProtectionIndex.Classification classifyDbFirstProtection(int dimension,int x,int z){
        current();
        if(dbFirstBacking==null||retainedBootProtectionData==null)throw new IllegalStateException("Original schema6 protection DATA missing");
        dbFirstBacking.data(); // 原读资格失效时不以旧index继续授予保存判定。
        return retainedBootProtectionData.classify(dimension,x,z);
    }






    private void retainFailure(Throwable cause){if(primary==null)primary=cause;else ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
    private static final class LoaderSlot {
        final WorldServer world;final AnvilChunkLoader loader;final SourceDomainCapture.RootPlan loadedRoot;
        ManagedSourceSaveLedger ledger;SourceLoadManifest manifest;
        boolean readDetached;
        boolean saveDetached;
        LoaderSlot(WorldServer world,AnvilChunkLoader loader,SourceDomainCapture.RootPlan loadedRoot){
            this.world=world;this.loader=loader;this.loadedRoot=loadedRoot;
        }
    }
}
