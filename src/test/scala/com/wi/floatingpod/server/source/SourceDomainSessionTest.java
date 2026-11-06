package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import net.minecraft.world.DimensionType;
import net.minecraft.world.WorldProvider;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static com.wi.floatingpod.server.source.SourceDomainCapture.*;

/** 实际 production observation lifecycle 与标准 provider API；非 Forge/world/native 集成。 */
final class SourceDomainSessionTest {
    private static final SourceLimitProfile LIMITS=SourceLimitProfile.of(1048576,65536,65536,64,64,1048576,65536,65536);
    private static final class Fixture {
        final Object server=new Object(),converter=new Object(),type=new Object();
        final RegistryClock clock=new RegistryClock();final Path root=Path.of("D:/source-domain-fixture");
        final AtomicReference<SourceDomainSession.State> state=new AtomicReference<>();
        final SourceDomainSession session;
        Fixture(){state.set(snapshot(new Object()));session=SourceDomainSession.forObservationTest(Thread.currentThread(),UUID.randomUUID(),LIMITS,state::get);}
        SourceDomainSession.State snapshot(Object loader){return new SourceDomainSession.State(clock.snapshot(),server,converter,root,List.of(
            new SourceDomainSession.Entry(0,type,new Object(),null,null,loader,new RootPlan(0,"overworld",root,root.resolve("region"),true),List.of()),
            new SourceDomainSession.Entry(-1,type,null,null,null,null,new RootPlan(-1,"the_nether",root.resolve("DIM-1"),root.resolve("DIM-1/region"),false),List.of())));}
    }
    @Test void actualInventoryAdmissionDoesNotUseNbtMembersAndIncludesLoadedUnregisteredUnion(){
        SourceLimitProfile oneMember=SourceLimitProfile.of(1048576,65536,65536,64,1,1048576,65536,65536);
        Integer[] registered={37,1,0,-1},loaded={0,99};
        SourceDomainSession.DimensionInventory inventory=SourceDomainSession.dimensionInventory(registered,loaded,oneMember);
        assertArrayEquals(new Integer[]{-1,0,1,37},inventory.registered);assertArrayEquals(new int[]{99},inventory.loadedUnregistered);
        assertArrayEquals(new Integer[]{37,1,0,-1},registered,"production admission must not mutate getter arrays");
        Path root=Path.of("D:/domain-standard");inventory.budget.retainRoot(root);
        for(int dimension:inventory.registered){
            DimensionType type=dimension==-1?DimensionType.NETHER:dimension==1?DimensionType.THE_END:DimensionType.OVERWORLD;
            WorldProvider actualStandard=SourceDomainSession.standardProvider(type);actualStandard.setDimension(dimension);String folder=actualStandard.getSaveFolder();
            Path directory=folder==null?root:root.resolve(folder);
            inventory.budget.retainEntry(new RootPlan(dimension,type.getName(),directory,directory.resolve("region"),dimension==0),List.of());
        }
        inventory.budget.retainEntry(null,List.of(new Unresolved(99,Issue.LOADED_UNREGISTERED_DIMENSION)));
        assertTrue(inventory.budget.reservedBytes()<=oneMember.maxRetainedBytes());
    }
    @Test void actualInventoryAdmissionRejectsInsufficientRetainedBytesEvenWithManyNbtMembers(){
        SourceLimitProfile bytesTooSmall=SourceLimitProfile.of(1048576,65536,65536,64,1000000,1,65536,65536);
        IllegalStateException cause=assertThrows(IllegalStateException.class,()->SourceDomainSession.dimensionInventory(new Integer[]{-1,0,1},new Integer[]{0},bytesTooSmall));
        assertEquals("Source dimension plan retained-byte budget exhausted",cause.getMessage());
    }
    @Test void failedInventoryReturnsUnpublishedPayloadAndTemporaryAccount() {
        Accounting allocation=new Accounting();
        assertThrows(IllegalArgumentException.class,()->SourceDomainSession.dimensionInventory(
                new Integer[]{0,0},new Integer[]{0},LIMITS,allocation));
        assertEquals(0L,allocation.held,"invalid inventory must drop its unpublished comparison bytes");
        assertEquals(1,allocation.retires,"the isolated temporary account must be retired");
        assertThrows(NullPointerException.class,()->SourceDomainSession.dimensionInventory(
                new Integer[]{null},new Integer[]{0},LIMITS,allocation));
        assertEquals(0L,allocation.held);assertEquals(2,allocation.retires);
    }
    @Test void failedInventoryPreservesValidationCauseAndFailedRefund() {
        Accounting allocation=new Accounting();
        var cleanup=new java.io.IOException("exact inventory refund failure");allocation.releaseFailure=cleanup;
        var original=assertThrows(IllegalArgumentException.class,()->SourceDomainSession.dimensionInventory(
                new Integer[]{0,0},new Integer[]{0},LIMITS,allocation));
        assertEquals("Duplicate registered dimension",original.getMessage());
        assertEquals(1,original.getSuppressed().length);
        assertSame(cleanup,original.getSuppressed()[0].getCause());
        assertTrue(allocation.held>0L);assertEquals(0,allocation.retires);
    }
    @Test void actualEntryRetentionChargesRealPathAndIssueBytesBeforeRetainingEntry(){
        SourceLimitProfile bytesLimited=SourceLimitProfile.of(1048576,65536,65536,64,1,10000,65536,65536);
        SourceDomainSession.DimensionInventory inventory=SourceDomainSession.dimensionInventory(new Integer[]{0},new Integer[]{0},bytesLimited);
        Path oversized=Path.of("D:/"+"segment".repeat(1000));long before=inventory.budget.reservedBytes();
        assertThrows(IllegalStateException.class,()->inventory.budget.retainEntry(new RootPlan(0,"overworld",oversized,oversized.resolve("region"),true),List.of()));
        assertEquals(before,inventory.budget.reservedBytes(),"failed retention must not claim reserved admission");
    }
    @Test void standardProviderSaveFoldersUseActualApiIncludingColdAdditionalDimension(){
        for(DimensionType type:List.of(DimensionType.OVERWORLD,DimensionType.NETHER,DimensionType.THE_END)){
            WorldProvider provider=SourceDomainSession.standardProvider(type);assertNotNull(provider);provider.setDimension(0);assertNull(provider.getSaveFolder());
            provider.setDimension(-1);assertEquals("DIM-1",provider.getSaveFolder());provider.setDimension(37);assertEquals("DIM37",provider.getSaveFolder());
        }
    }
    @Test void loadedAndColdPlansAreImmutableButGenerationAndNativeRemainUnresolved(){
        Fixture f=new Fixture();CatalogDomainObservation observation=f.session.capture();assertEquals(2,observation.roots().size());assertTrue(observation.roots().getFirst().loaded());assertFalse(observation.roots().get(1).loaded());
        assertEquals(GenerationObservation.GENERATION_UNOBSERVED,observation.generationObservation());
        assertTrue(observation.unresolved().stream().anyMatch(x->x.issue()==Issue.NATIVE_EXCLUSIVITY_NOT_OBSERVED));
        assertTrue(observation.unresolved().stream().anyMatch(x->x.issue()==Issue.DIRECTORY_MEMBERSHIP_NOT_OBSERVED));
        assertThrows(UnsupportedOperationException.class,()->observation.roots().clear());f.session.requireCurrent(observation);
    }
    @Test void registryAbaRevokesEvenWhenAllRawRefsAndPlansReturnToSameValues(){
        Fixture f=new Fixture();SourceDomainSession.State original=f.state.get();CatalogDomainObservation observation=f.session.capture();
        f.clock.beginMutation();f.clock.endMutation(null);f.clock.beginMutation();f.clock.endMutation(null);
        f.state.set(new SourceDomainSession.State(f.clock.snapshot(),original.server,original.converter,original.root,original.entries));
        assertThrows(IllegalStateException.class,()->f.session.requireCurrent(observation));
        CatalogDomainObservation next=f.session.capture();assertTrue(next.observedGeneration()>observation.observedGeneration());assertTrue(next.registryGeneration()>observation.registryGeneration());
    }
    @Test void rawLoaderReplacementRevokesOldObservation(){
        Fixture f=new Fixture();CatalogDomainObservation old=f.session.capture();SourceDomainSession.State state=f.state.get();SourceDomainSession.Entry e=state.entries.getFirst();
        SourceDomainSession.Entry replacement=new SourceDomainSession.Entry(e.dimension,e.type,e.world,e.provider,e.handler,new Object(),e.plan,e.unresolved);
        f.state.set(new SourceDomainSession.State(state.stamp,state.server,state.converter,state.root,List.of(replacement,state.entries.get(1))));
        assertThrows(IllegalStateException.class,()->f.session.requireCurrent(old));assertNotNull(f.session.capture());
    }
    @Test void customStorageIsUnresolvedWithoutLosingKnownStandardPlan(){
        Fixture f=new Fixture();SourceDomainSession.State s=f.state.get();SourceDomainSession.Entry custom=new SourceDomainSession.Entry(27,new Object(),null,null,null,null,null,List.of(new Unresolved(27,Issue.CUSTOM_PROVIDER_STORAGE_UNPROVEN)));
        f.state.set(new SourceDomainSession.State(s.stamp,s.server,s.converter,s.root,List.of(s.entries.getFirst(),custom)));
        CatalogDomainObservation observation=f.session.capture();assertEquals(1,observation.roots().size());assertTrue(observation.unresolved().stream().anyMatch(x->x.dimension()==27&&x.issue()==Issue.CUSTOM_PROVIDER_STORAGE_UNPROVEN));
    }
    @Test void captureChangesBetweenTwoActualObservationsRejectInsteadOfPartialSuccess(){
        Fixture f=new Fixture();SourceDomainSession.State a=f.state.get(),b=f.snapshot(new Object());int[] calls={0};
        SourceDomainSession session=SourceDomainSession.forObservationTest(Thread.currentThread(),UUID.randomUUID(),LIMITS,()->calls[0]++==0?a:b);
        assertThrows(IllegalStateException.class,session::capture);assertEquals(2,calls[0]);
    }
    @Test void foreignObservationClosedSessionAndWrongThreadAreExplicitlyRejected()throws Exception {
        Fixture a=new Fixture(),b=new Fixture();CatalogDomainObservation observation=a.session.capture();assertThrows(IllegalArgumentException.class,()->b.session.requireCurrent(observation));
        AtomicReference<Throwable> failure=new AtomicReference<>();Thread actor=new Thread(()->{try{a.session.capture();}catch(Throwable cause){failure.set(cause);}});
        actor.start();actor.join(2000);assertFalse(actor.isAlive(),"test actor must terminate");assertInstanceOf(IllegalStateException.class,failure.get());
        a.session.closeObservation();assertThrows(IllegalStateException.class,()->a.session.requireCurrent(observation));
    }
    @Test void mutationInProgressAndOriginalFailureNeverBecomeCurrentPermission(){
        Fixture f=new Fixture();SourceDomainSession.State s=f.state.get();f.clock.beginMutation();
        f.state.set(new SourceDomainSession.State(f.clock.snapshot(),s.server,s.converter,s.root,s.entries));CatalogDomainObservation pending=f.session.capture();
        assertTrue(pending.unresolved().stream().anyMatch(x->x.issue()==Issue.REGISTRY_MUTATION_IN_PROGRESS));assertThrows(IllegalStateException.class,()->f.session.requireCurrent(pending));
        RuntimeException cause=new RuntimeException("actual mutation failed");f.clock.endMutation(cause);f.state.set(new SourceDomainSession.State(f.clock.snapshot(),s.server,s.converter,s.root,s.entries));
        CatalogDomainObservation failed=f.session.capture();assertSame(cause,failed.mutationFailure());assertThrows(IllegalStateException.class,()->f.session.requireCurrent(failed));
    }
    // Controlled PlanBudget boundary recorder only; it does not model Root, Account, or custody authorization.
    private static final class Accounting implements SourceForestAllocationBudget {
        final Accounting ledger;long held;int reserves,releases,isolates,retires;java.io.IOException reserveFailure,releaseFailure;
        Accounting(){ledger=this;}
        private Accounting(Accounting ledger){this.ledger=ledger;}
        public SourceLimitProfile exactProfile(){return LIMITS;}
        public SourceForestAllocationBudget isolate(){ledger.isolates++;return new Accounting(ledger);}
        public void retire(){ledger.retires++;}
        public void reserveTemporary(long bytes,String context)throws java.io.IOException{ledger.reserves++;if(ledger.reserveFailure!=null)throw ledger.reserveFailure;ledger.held=Math.addExact(ledger.held,bytes);}
        public void releaseTemporary(long bytes,String context)throws java.io.IOException{ledger.releases++;if(ledger.releaseFailure!=null)throw ledger.releaseFailure;ledger.held=Math.subtractExact(ledger.held,bytes);}
    }
    // This strict numerical backend exercises the shared production AccountBook algorithm; it grants no ownership authority.
    private static final class NumericFees implements SourceAccountBook.Fees {
        long held;int reserves,releases;java.io.IOException reserveFailure,releaseFailure;
        public void reserve(long bytes,String context)throws java.io.IOException{reserves++;if(reserveFailure!=null)throw reserveFailure;held=Math.addExact(held,bytes);}
        public void release(long bytes,String context)throws java.io.IOException{releases++;if(releaseFailure!=null)throw releaseFailure;if(bytes<0||bytes>held)throw new IllegalStateException("Test fee release exceeds held bytes");held-=bytes;}
    }
    @Test void planBudgetRejectsCapacityBeforeAdapterAndPreservesOriginalAllocationFailure(){
        Accounting allocation=new Accounting();var budget=new SourceDomainSession.PlanBudget(32,allocation);
        budget.reserve(24);assertEquals(24,allocation.held);
        assertThrows(IllegalStateException.class,()->budget.reserve(9));assertEquals(1,allocation.reserves);assertEquals(24,budget.reservedBytes());
        var original=new java.io.IOException("original root allocation rejected");allocation.reserveFailure=original;
        assertSame(original,assertThrows(java.io.UncheckedIOException.class,()->budget.reserve(8)).getCause());
        assertEquals(24,budget.reservedBytes());assertEquals(24,allocation.held);
        allocation.reserveFailure=null;budget.discard();assertEquals(0,allocation.held);assertEquals(0,budget.reservedBytes());
    }
    @Test void stableCurrentnessDiscardsOnlyComparisonFeesAndRetainsFailedCleanupSlot(){
        Fixture f=new Fixture();var original=f.state.get();Accounting allocation=new Accounting();
        AtomicReference<SourceDomainSession.PlanBudget> latestBudget=new AtomicReference<>();
        var session=SourceDomainSession.forObservationTest(Thread.currentThread(),UUID.randomUUID(),LIMITS,()->{
            var budget=new SourceDomainSession.PlanBudget(1024,allocation);budget.reserve(64);latestBudget.set(budget);
            return new SourceDomainSession.State(original.stamp,original.server,original.converter,original.root,original.entries,budget);
        });
        var observation=session.capture();assertEquals(64,allocation.held);assertEquals(1,allocation.releases);
        session.requireCurrent(observation);assertEquals(64,allocation.held);assertSame(observation,session.capture());assertEquals(64,allocation.held);
        var cleanup=new java.io.IOException("original comparison release failed");allocation.releaseFailure=cleanup;
        assertSame(cleanup,assertThrows(java.io.UncheckedIOException.class,()->session.requireCurrent(observation)).getCause());
        assertEquals(128,allocation.held);assertEquals(64,latestBudget.get().reservedBytes(),"failed cleanup retains the exact comparison reservation");
    }

    @Test void accountBookRepeatedTemporaryAccountsReturnMetadataAndPayloadFeesToSameParentBaseline()throws Exception {
        NumericFees fees=new NumericFees();SourceAccountBook book=new SourceAccountBook(fees);
        // Numeric prepayment only; this does not exercise actual custody binding or authorization.
        fees.reserve(SourceAccountBook.CUSTODY_METADATA_BYTES,"test-only simulated prepaid numeric fee");book.custodyMetadataPrepaid();var parent=book.registerPrepaidCustody();
        long baseline=fees.held;int baseSlots=book.slotCount();
        for(int probe=0;probe<4;probe++){
            var temporary=book.retain(parent,true);assertEquals(baseSlots+1,book.slotCount());assertEquals(baseline+SourceAccountBook.ACCOUNT_METADATA_BYTES,fees.held);
            book.reserve(temporary,73L,"stable domain comparison payload BEFORE allocation");assertEquals(baseline+SourceAccountBook.ACCOUNT_METADATA_BYTES+73L,fees.held);
            book.release(temporary,73L,"stable domain comparison payload released");book.retire(temporary);
            assertEquals(baseline,fees.held);assertEquals(baseSlots,book.slotCount());assertEquals(0,book.childCount(parent));assertTrue(book.contains(parent));assertFalse(book.isPartialSlot(temporary));
        }
        assertEquals(9,fees.reserves);assertEquals(8,fees.releases);
    }

    @Test void accountBookRequiresNestedConsumersToRetireBeforeTheirParent()throws Exception {
        NumericFees fees=new NumericFees();SourceAccountBook book=new SourceAccountBook(fees);var parent=book.retain(null,true);var child=book.retain(parent,true);
        assertEquals(1,book.childCount(parent));int releases=fees.releases;
        assertThrows(IllegalStateException.class,()->book.retire(parent));assertEquals(releases,fees.releases);assertTrue(book.contains(parent));
        book.retire(child);assertEquals(0,book.childCount(parent));assertNull(book.parentOf(child));assertFalse(book.contains(child));
        book.retire(parent);assertEquals(0,book.slotCount());assertEquals(0L,fees.held);
    }

    @Test void accountBookRejectsHeldAndRetiredSlotReuse()throws Exception {
        NumericFees fees=new NumericFees();SourceAccountBook book=new SourceAccountBook(fees);var held=book.retain(null,true);book.reserve(held,9L,"held payload");
        int releases=fees.releases;assertThrows(IllegalStateException.class,()->book.retire(held));assertEquals(releases,fees.releases);assertEquals(9L,book.heldBytes(held));
        book.release(held,9L,"held payload settled");book.retire(held);assertFalse(book.contains(held));
        assertEquals("Source account retired or cleanup failed",assertThrows(IllegalStateException.class,()->book.requireActive(held)).getMessage());
    }

    @Test void accountBookRejectsForeignSlotsAndKeepsPrepaidCustodyNonretirableWithoutAnotherMetadataFee()throws Exception {
        NumericFees firstFees=new NumericFees(),secondFees=new NumericFees();SourceAccountBook first=new SourceAccountBook(firstFees),second=new SourceAccountBook(secondFees);
        firstFees.reserve(SourceAccountBook.CUSTODY_METADATA_BYTES,"test-only simulated prepaid numeric fee");first.custodyMetadataPrepaid();
        var prepaid=first.registerPrepaidCustody();assertEquals(SourceAccountBook.CUSTODY_METADATA_BYTES,firstFees.held);assertEquals(1,first.slotCount());assertTrue(first.isPartialSlot(prepaid));
        assertThrows(IllegalStateException.class,()->first.retire(prepaid));assertEquals(0,firstFees.releases);
        assertThrows(IllegalStateException.class,()->second.retire(prepaid));assertEquals(0,secondFees.releases);assertTrue(first.contains(prepaid));
    }

    @Test void accountBookMetadataReserveFailureDoesNotCreateSlotOrIncrementParent()throws Exception {
        NumericFees fees=new NumericFees();SourceAccountBook book=new SourceAccountBook(fees);var parent=book.retain(null,false);
        int slots=book.slotCount();long held=fees.held;var cause=new java.io.IOException("original metadata reserve failure");fees.reserveFailure=cause;
        assertSame(cause,assertThrows(java.io.IOException.class,()->book.retain(parent,true)));
        assertEquals(slots,book.slotCount());assertEquals(held,fees.held);assertEquals(0,book.childCount(parent));assertTrue(book.contains(parent));assertTrue(book.isPartialSlot(parent));
    }

    @Test void accountBookPayloadReleaseFailureIsStickyAndRetainsTheSlotAndParent()throws Exception {
        NumericFees fees=new NumericFees();SourceAccountBook book=new SourceAccountBook(fees);var parent=book.retain(null,false);var child=book.retain(parent,true);
        book.reserve(child,31L,"payload held before failed release");var cause=new java.io.IOException("original payload release failure");fees.releaseFailure=cause;
        assertSame(cause,assertThrows(java.io.IOException.class,()->book.release(child,31L,"payload release")));
        assertSame(cause,book.failureCause(child));assertTrue(book.contains(child));assertTrue(book.isPartialSlot(child));assertSame(parent,book.parentOf(child));assertEquals(1,book.childCount(parent));assertEquals(31L,book.heldBytes(child));
        assertSame(cause,assertThrows(IllegalStateException.class,()->book.release(child,31L,"payload release retry")).getCause());
        assertEquals(1,fees.releases);assertTrue(book.contains(child));assertEquals(1,book.childCount(parent));
    }

    @Test void accountBookMetadataReleaseFailureIsStickyAndDoesNotDropFailedSlotOrRetry()throws Exception {
        NumericFees fees=new NumericFees();SourceAccountBook book=new SourceAccountBook(fees);var parent=book.retain(null,false);var child=book.retain(parent,true);
        var cause=new java.io.IOException("original metadata release failure");fees.releaseFailure=cause;int slots=book.slotCount();long held=fees.held;
        assertSame(cause,assertThrows(java.io.IOException.class,()->book.retire(child)));
        assertSame(cause,book.failureCause(child));assertEquals(slots,book.slotCount());assertEquals(held,fees.held);assertTrue(book.contains(child));assertTrue(book.isPartialSlot(child));assertSame(parent,book.parentOf(child));assertEquals(1,book.childCount(parent));
        assertSame(cause,assertThrows(IllegalStateException.class,()->book.retire(child)).getCause());assertEquals(1,fees.releases);
        assertThrows(IllegalStateException.class,()->book.retire(parent));assertEquals(1,book.childCount(parent));assertTrue(book.contains(child));
    }

    @Test void planBudgetRepeatedComparisonProbeCallsIsolatePayloadReleaseAndRetireExactlyOnce() {
        // This counts PlanBudget's adapter calls only; SourceAccountBook behavior is tested separately above.
        Accounting allocation=new Accounting();
        for(int probe=0;probe<4;probe++){
            var budget=new SourceDomainSession.PlanBudget(128,allocation);budget.reserve(64);budget.discard();assertEquals(0,allocation.held);
        }
        assertEquals(4,allocation.isolates);assertEquals(4,allocation.reserves);assertEquals(4,allocation.releases);assertEquals(4,allocation.retires);
    }

}
