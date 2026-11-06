package com.wi.floatingpod.bootstrap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** 直接验证生产 closed gate；不声称覆盖 mixin、数据库、文件身份或 Ready。 */
class ManagedBootSaveScopeTest {
    @TempDir Path temporaryDirectory;

    @Test void providerIncompleteSurvivesOtherLoaderAndPartialPassUntilExactFullPass() throws Throwable {
        var root=temporaryDirectory.resolve("world");var scope=scope(new Object(),root);
        Object first=new Object(),second=new Object();
        scope.registerLoader(first,root);scope.registerLoader(second,root.resolve("DIM1"));
        assertFalse(scope.invokeProviderSave(first,true,()->false));
        assertThrows(ManagedBootSaveScope.AdmissionException.class,scope::requireProviderSavesComplete);
        assertTrue(scope.invokeProviderSave(second,true,()->true));
        assertTrue(scope.invokeProviderSave(first,false,()->true));
        assertThrows(ManagedBootSaveScope.AdmissionException.class,scope::requireProviderSavesComplete);
        assertTrue(scope.invokeProviderSave(first,true,()->true));
        scope.requireProviderSavesComplete();
    }

    @Test void nestedIncompleteCannotBeClearedByOuterReturnOrInFlightFlush() throws Throwable {
        var root=temporaryDirectory.resolve("world");var scope=scope(new Object(),root);Object loader=new Object();
        scope.registerLoader(loader,root);
        assertTrue(scope.invokeProviderSave(loader,true,()->{
            assertThrows(ManagedBootSaveScope.AdmissionException.class,scope::requireProviderSavesComplete);
            assertFalse(scope.invokeProviderSave(loader,true,()->false));
            return true;
        }));
        assertThrows(ManagedBootSaveScope.AdmissionException.class,scope::requireProviderSavesComplete);
        scope.invokeProviderSave(loader,true,()->true);scope.requireProviderSavesComplete();
    }

    @Test void providerFailureKeepsOriginalCauseAfterLaterPass() throws Throwable {
        var root=temporaryDirectory.resolve("world");var scope=scope(new Object(),root);Object loader=new Object();
        scope.registerLoader(loader,root);var original=new IOException("actual provider failure");
        assertSame(original,assertThrows(IOException.class,()->scope.invokeProviderSave(loader,true,()->{throw original;})));
        scope.invokeProviderSave(loader,true,()->true);
        assertSame(original,assertThrows(ManagedBootSaveScope.AdmissionException.class,scope::requireProviderSavesComplete).getCause());
        assertThrows(ManagedBootSaveScope.AdmissionException.class,()->scope.invokeProviderSave(new Object(),true,()->true));
    }

    private ManagedBootSaveScope scope(Object server,Path root) {
        return new ManagedBootSaveScope(server,new Object(),new Object(),Thread.currentThread(),root,
            UUID.fromString("11111111-1111-4111-8111-111111111111"),7L,11L,
            UUID.fromString("22222222-2222-4222-8222-222222222222"));
    }

    @Test void exactManagedWorldAndEnrolledLoaderAreRejectedBeforeOriginalSaveBody() throws Throwable {
        Object server=new Object(),loader=new Object();
        Path root=temporaryDirectory.resolve("pinned-world").toAbsolutePath().normalize();
        Path dimension=root.resolve("DIM-1");
        ManagedBootSaveScope scope=scope(server,root);
        scope.registerLoader(loader,dimension);
        AtomicInteger originalCalls=new AtomicInteger();

        ManagedBootSaveScope.AdmissionException failure=assertThrows(
            ManagedBootSaveScope.AdmissionException.class,
            () -> scope.invoke(server,loader,loader,root,"DIM-1",dimension,originalCalls::incrementAndGet)
        );

        assertEquals("CLOSED_BOOT_SCOPE",failure.stage());
        assertEquals(UUID.fromString("11111111-1111-4111-8111-111111111111"),failure.bootId());
        assertEquals(7L,failure.bootGeneration());
        assertEquals(11L,failure.connectionGeneration());
        assertEquals(UUID.fromString("22222222-2222-4222-8222-222222222222"),failure.operationId());
        assertEquals(0,originalCalls.get());
    }

    @Test void exactServerWithUnknownLoaderOrEscapingProviderFailsClosed() throws Throwable {
        Object server=new Object(),registeredLoader=new Object(),unknownLoader=new Object();
        Path root=temporaryDirectory.resolve("pinned-world").toAbsolutePath().normalize();
        ManagedBootSaveScope scope=scope(server,root);
        Path expected=root.resolve("DIM-2");
        scope.registerLoader(registeredLoader,expected);
        AtomicInteger originalCalls=new AtomicInteger();

        ManagedBootSaveScope.AdmissionException unknown=assertThrows(
            ManagedBootSaveScope.AdmissionException.class,
            () -> scope.invoke(server,unknownLoader,unknownLoader,root,"DIM-2",expected,originalCalls::incrementAndGet)
        );
        assertEquals("LOADER_ENROLLMENT",unknown.stage());

        ManagedBootSaveScope.AdmissionException escape=assertThrows(
            ManagedBootSaveScope.AdmissionException.class,
            () -> scope.invoke(server,registeredLoader,registeredLoader,root,"../outside",
                root.resolve("../outside").normalize(),originalCalls::incrementAndGet)
        );
        assertEquals("PROVIDER_FOLDER",escape.stage());

        ManagedBootSaveScope.AdmissionException currentLoader=assertThrows(
            ManagedBootSaveScope.AdmissionException.class,
            () -> scope.invoke(server,registeredLoader,unknownLoader,root,"DIM-2",expected,
                originalCalls::incrementAndGet)
        );
        assertEquals("CURRENT_LOADER",currentLoader.stage());

        ManagedBootSaveScope.AdmissionException pathMismatch=assertThrows(
            ManagedBootSaveScope.AdmissionException.class,
            () -> scope.invoke(server,registeredLoader,registeredLoader,root,"DIM-2",
                root.resolve("DIM-3"),originalCalls::incrementAndGet)
        );
        assertEquals("LOADER_PATH",pathMismatch.stage());

        ManagedBootSaveScope.AdmissionException rootMismatch=assertThrows(
            ManagedBootSaveScope.AdmissionException.class,
            () -> scope.invoke(server,registeredLoader,registeredLoader,root.resolve("elsewhere"),
                "DIM-2",root.resolve("elsewhere").resolve("DIM-2"),originalCalls::incrementAndGet)
        );
        assertEquals("WORLD_SAVE_ROOT",rootMismatch.stage());
        assertEquals(0,originalCalls.get());
    }

    @Test void rootIntersectionCannotBeReclassifiedAsUnmanagedByAnEscapedLoader() throws Throwable {
        Object server=new Object(),foreignServer=new Object(),loader=new Object();
        Path root=temporaryDirectory.resolve("pinned-world").toAbsolutePath().normalize();
        ManagedBootSaveScope scope=scope(server,root);
        Path outside=root.getParent().resolve("outside").normalize();
        AtomicInteger originalCalls=new AtomicInteger();

        ManagedBootSaveScope.AdmissionException failure=assertThrows(
            ManagedBootSaveScope.AdmissionException.class,
            () -> scope.invoke(foreignServer,loader,loader,root,null,outside,originalCalls::incrementAndGet)
        );

        assertEquals("UNMANAGED_CLASSIFICATION",failure.stage());
        assertEquals(0,originalCalls.get());
    }

    @Test void unmanagedWorldKeepsOriginalBodyAndItsExactCauseChain() throws Throwable {
        Object server=new Object(),unmanagedServer=new Object(),loader=new Object();
        Path root=temporaryDirectory.resolve("world").toAbsolutePath().normalize();
        Path unmanagedRoot=temporaryDirectory.resolve("world-copy").toAbsolutePath().normalize();
        Path loaderDirectory=unmanagedRoot.resolve("DIM-4");
        ManagedBootSaveScope scope=scope(server,root);
        AtomicInteger originalCalls=new AtomicInteger();
        IOException primary=new IOException("original unmanaged save failure");
        IOException secondary=new IOException("original secondary");
        primary.addSuppressed(secondary);

        IOException observed=assertThrows(IOException.class,() ->
            scope.invoke(unmanagedServer,loader,loader,unmanagedRoot,"DIM-4",loaderDirectory,() -> {
                originalCalls.incrementAndGet();
                throw primary;
            })
        );

        assertSame(primary,observed);
        assertSame(secondary,observed.getSuppressed()[0]);
        assertEquals(1,originalCalls.get());
    }

    @Test void managedLoaderEnrollmentRequiresTheClaimedMainThread() throws Exception {
        Object server=new Object(),loader=new Object();
        Path root=temporaryDirectory.resolve("pinned-world").toAbsolutePath().normalize();
        Path loaderDirectory=root.resolve("region");
        ManagedBootSaveScope scope=scope(server,root);
        AtomicReference<Throwable> failure=new AtomicReference<>();
        Thread foreign=new Thread(() -> {
            try { scope.registerLoader(loader,loaderDirectory); }
            catch(Throwable cause) { failure.set(cause); }
        },"foreign-enrollment-thread");
        foreign.start();
        foreign.join(2000L);

        assertFalse(foreign.isAlive(), "foreign enrollment test thread must terminate within its bound");
        assertInstanceOf(ManagedBootSaveScope.AdmissionException.class,failure.get());
        assertFalse(scope.ownsLoader(loader));
    }
    @Test void callerSuppliedBridgeAndUnregisteredScopeCannotActivateFinalSource(){
        var admission=new com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission(Thread.currentThread(),4);
        Object owner=new Object();admission.beginFinalSealing();var escrow=admission.tryReserveFinalSource(owner,640L);
        var unregistered=scope(new Object(),temporaryDirectory.resolve("unregistered-final-source"));
        Object loader=new Object();
        unregistered.registerLoader(loader,unregistered.worldRoot());
        AtomicInteger validatorCalls=new AtomicInteger();
        var source=new com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.FinalSaveValidator(){
            @Override public void endMainConsumers(){throw new AssertionError("Negative authorization fixture must not end final consumers");}
            public void requireComplete(){validatorCalls.incrementAndGet();}
            public void requireMainSave(Object world,int x,int z,Object chunk){validatorCalls.incrementAndGet();}
            public int expectedArtifacts(Object world,Object loader,int x,int z,Object root){validatorCalls.incrementAndGet();return 0;}
            public void requireMainPayload(Object world,Object loader,int x,int z,Object root){validatorCalls.incrementAndGet();}
        };
        var failure=assertThrows(IllegalStateException.class,()->admission.activateFinalSource(unregistered,escrow,owner,source));
        assertEquals("Actual retained managed Scope/callbacks required",failure.getMessage());
        assertEquals(0,validatorCalls.get());assertFalse(admission.isFinalActive());
        assertThrows(NullPointerException.class,()->admission.activateFinalSource(null,escrow,owner,source));
        assertThrows(ManagedBootSaveScope.AdmissionException.class,()->unregistered.retireFinalEnrolledLoaders(admission));
        synchronized(admission){
            var rejected=assertThrows(IllegalStateException.class,()->unregistered.retireFinalEnrolledLoaders(admission));
            assertEquals("Actual final source consumer end required",rejected.getMessage());
        }
        assertTrue(unregistered.ownsLoader(loader),"Rejected retirement must preserve the actual loader registry");
        assertThrows(ManagedBootSaveScope.AdmissionException.class,()->unregistered.requireUnmanagedDisjoint(
            loader,temporaryDirectory.resolve("outside"),null,null));
    }

}
