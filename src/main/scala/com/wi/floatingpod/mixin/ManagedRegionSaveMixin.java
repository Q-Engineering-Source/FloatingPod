package com.wi.floatingpod.mixin;

import com.llamalad7.mixinextras.injector.wrapmethod.WrapMethod;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import com.wi.floatingpod.server.save.ManagedRegionAccess;
import com.wi.floatingpod.server.save.ManagedSaveContext;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;
import java.io.DataOutputStream;
import java.io.FileDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import net.minecraft.world.chunk.storage.RegionFile;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** 原Region算法保留。同步和inject count必须在actual transformed binary后续核，不在SOURCE冒称。 */
@Mixin(RegionFile.class)
public abstract class ManagedRegionSaveMixin implements ManagedRegionAccess {
    @Shadow private RandomAccessFile dataFile;
    @Shadow @Final private File fileName;
    @Unique private volatile ManagedSourceSaveLedger floatingpod$ledger;
    @Override public final void floatingpod$bindSaveLedger(ManagedSourceSaveLedger ledger) {
        synchronized(this) {
            if(floatingpod$ledger!=null && floatingpod$ledger!=ledger)throw new IllegalStateException("Region belongs to a different managed loader ledger");
            ledger.bindRegion(this);floatingpod$ledger=ledger;
        }
    }
    @Override public final FileDescriptor floatingpod$originalDescriptor()throws IOException {
        floatingpod$requireFlight(ManagedSaveContext.current());return dataFile.getFD();
    }
    @Override public final File floatingpod$originalFileName(){floatingpod$requireFlight(ManagedSaveContext.current());return fileName;}
    @Override public final void floatingpod$cacheSwallowed(Throwable originalCloseCause,Throwable actualCaughtCause)throws Throwable {
        ManagedSourceSaveLedger ledger=floatingpod$ledger;if(ledger==null)return;
        try {
            ledger.closeFailure(this,actualCaughtCause);
            if(originalCloseCause!=actualCaughtCause) {
                IllegalStateException mismatch=new IllegalStateException("Cache caught different original Region.close cause");
                if(originalCloseCause!=null)ManagedSourceSaveLedger.suppressSecondary(actualCaughtCause,originalCloseCause);
                ManagedSourceSaveLedger.suppressSecondary(actualCaughtCause,mismatch);throw actualCaughtCause;
            }
        } catch(Throwable observation) { ManagedSourceSaveLedger.suppressSecondary(actualCaughtCause,observation);throw actualCaughtCause; }
    }
    @WrapMethod(method="getChunkDataOutputStream(II)Ljava/io/DataOutputStream;",require=1,allow=1)
    private DataOutputStream floatingpod$output(int x,int z,Operation<DataOutputStream> original)throws Throwable {
        ManagedSaveContext c=ManagedSaveContext.current();if(c!=null)floatingpod$bindSaveLedger(c.ledger);
        return original.call(x,z);
    }
    @WrapMethod(method="write(II[BI)V",require=1,allow=1)
    private void floatingpod$write(int x,int z,byte[] data,int length,Operation<Void> original)throws Throwable {
        ManagedSaveContext c=ManagedSaveContext.current();
        if(floatingpod$ledger==null){original.call(x,z,data,length);return;}
        floatingpod$requireFlight(c);
        synchronized(this) {
            c.ledger.regionWriteBeginning(this,c.attempt);int writes=c.ledger.writes(c.attempt);
            try{original.call(x,z,data,length);c.ledger.regionReturned(c.attempt,writes);}
            catch(Throwable cause){c.failed(cause);throw cause;}
        }
    }
    @WrapOperation(method="write(II[BI)V",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFile;write(I[BI)V",ordinal=0),require=1,allow=1)
    private void floatingpod$data0(RegionFile region,int sector,byte[] bytes,int length,Operation<Void> original){
        ManagedSaveContext c=ManagedSaveContext.current();if(floatingpod$ledger!=null){floatingpod$requireFlight(c);c.ledger.mutationBeginning(c.attempt);}
        original.call(region,sector,bytes,length);if(floatingpod$ledger!=null)c.ledger.dataWriteReturned(c.attempt);
    }
    @WrapOperation(method="write(II[BI)V",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFile;write(I[BI)V",ordinal=1),require=1,allow=1)
    private void floatingpod$data1(RegionFile region,int sector,byte[] bytes,int length,Operation<Void> original){
        ManagedSaveContext c=ManagedSaveContext.current();if(floatingpod$ledger!=null){floatingpod$requireFlight(c);c.ledger.mutationBeginning(c.attempt);}
        original.call(region,sector,bytes,length);if(floatingpod$ledger!=null)c.ledger.dataWriteReturned(c.attempt);
    }
    @WrapOperation(method="write(II[BI)V",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFile;write(I[BI)V",ordinal=2),require=1,allow=1)
    private void floatingpod$data2(RegionFile region,int sector,byte[] bytes,int length,Operation<Void> original){
        ManagedSaveContext c=ManagedSaveContext.current();if(floatingpod$ledger!=null){floatingpod$requireFlight(c);c.ledger.mutationBeginning(c.attempt);}
        original.call(region,sector,bytes,length);if(floatingpod$ledger!=null)c.ledger.dataWriteReturned(c.attempt);
    }
    @WrapOperation(method="write(II[BI)V",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFile;setOffset(III)V",ordinal=0),require=1,allow=1)
    private void floatingpod$offset0(RegionFile region,int x,int z,int offset,Operation<Void> original){
        ManagedSaveContext c=ManagedSaveContext.current();if(floatingpod$ledger!=null){floatingpod$requireFlight(c);c.ledger.mutationBeginning(c.attempt);}
        original.call(region,x,z,offset);if(floatingpod$ledger!=null)c.ledger.headerWriteReturned(c.attempt);
    }
    @WrapOperation(method="write(II[BI)V",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFile;setOffset(III)V",ordinal=1),require=1,allow=1)
    private void floatingpod$offset1(RegionFile region,int x,int z,int offset,Operation<Void> original){
        ManagedSaveContext c=ManagedSaveContext.current();if(floatingpod$ledger!=null){floatingpod$requireFlight(c);c.ledger.mutationBeginning(c.attempt);}
        original.call(region,x,z,offset);if(floatingpod$ledger!=null)c.ledger.headerWriteReturned(c.attempt);
    }
    @WrapOperation(method="write(II[BI)V",at=@At(value="INVOKE",target="Lnet/minecraft/world/chunk/storage/RegionFile;setChunkTimestamp(III)V",ordinal=0),require=1,allow=1)
    private void floatingpod$timestamp(RegionFile region,int x,int z,int timestamp,Operation<Void> original){
        ManagedSaveContext c=ManagedSaveContext.current();if(floatingpod$ledger!=null){floatingpod$requireFlight(c);c.ledger.mutationBeginning(c.attempt);}
        original.call(region,x,z,timestamp);if(floatingpod$ledger!=null)c.ledger.headerWriteReturned(c.attempt);
    }
    @WrapOperation(method="write(II[BI)V",at=@At(value="INVOKE",target="Ljava/io/IOException;printStackTrace()V",ordinal=0),require=1,allow=1)
    private void floatingpod$swallowed(IOException actualCause,Operation<Void> original)throws Throwable {
        ManagedSaveContext c=ManagedSaveContext.current();if(floatingpod$ledger!=null){
            try{floatingpod$requireFlight(c);c.recordFailure(actualCause);}
            catch(Throwable observation){ManagedSourceSaveLedger.suppressSecondary(actualCause,observation);throw actualCause;}
        }
        try{original.call(actualCause);}catch(Throwable secondary){ManagedSourceSaveLedger.suppressSecondary(actualCause,secondary);throw actualCause;}
    }
    @WrapOperation(method="<init>(Ljava/io/File;)V",at=@At(value="INVOKE",target="Ljava/io/IOException;printStackTrace()V",ordinal=0),require=1,allow=1)
    private void floatingpod$constructorCatch(IOException actualCause,Operation<Void> original)throws Throwable {
        ManagedSaveContext c=ManagedSaveContext.current();if(c!=null){
            try{floatingpod$bindSaveLedger(c.ledger);c.recordFailure(actualCause);}
            catch(Throwable observation){ManagedSourceSaveLedger.suppressSecondary(actualCause,observation);throw actualCause;}
        }
        try{original.call(actualCause);}catch(Throwable secondary){ManagedSourceSaveLedger.suppressSecondary(actualCause,secondary);throw actualCause;}
    }
    @WrapMethod(method="close()V",require=1,allow=1)
    private void floatingpod$close(Operation<Void> original)throws Throwable {
        ManagedSourceSaveLedger ledger=floatingpod$ledger;if(ledger==null){original.call();return;}
        synchronized(this) {
            ledger.closeBeginning(this);
            try{original.call();ledger.closeReturned(this);}catch(Throwable cause){
                try{ledger.closeFailure(this,cause);}catch(Throwable observation){ManagedSourceSaveLedger.suppressSecondary(cause,observation);}
                throw cause;
            }
        }
    }
    @Inject(method="write(II[BI)V",at=@At(value="RETURN",ordinal=0),require=1,allow=1)
    private void floatingpod$largeDisabled(int x,int z,byte[] bytes,int length,CallbackInfo ci) {
        ManagedSaveContext c=ManagedSaveContext.current();if(floatingpod$ledger!=null){floatingpod$requireFlight(c);c.ledger.largeDisabledReturn(c.attempt);}
    }
    @Unique private void floatingpod$requireFlight(ManagedSaveContext c){if(c==null || c.ledger!=floatingpod$ledger || c.attempt==null)throw new IllegalStateException("Exact Region managed inflight scope required");}
}
