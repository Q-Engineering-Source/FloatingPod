package com.wi.floatingpod.server.source;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/** 不访问文件；仅捕获真实已注册标准 provider 的 root 计划及未决项。 */
public final class SourceDomainCapture {
    private SourceDomainCapture() { }
    public enum GenerationObservation { GENERATION_UNOBSERVED, HOOK_CALLS_OBSERVED }
    public enum Issue { CUSTOM_PROVIDER_STORAGE_UNPROVEN, LOADED_PROVIDER_MISMATCH, LOADED_SERVER_MISMATCH,
        SAVE_HANDLER_STORAGE_UNPROVEN, SAVE_ROOT_MISMATCH, LOADER_STORAGE_UNPROVEN,
        LOADER_PATH_MISMATCH, LOADED_UNREGISTERED_DIMENSION, REGISTRY_MUTATION_IN_PROGRESS,
        REGISTRY_MUTATION_FAILED, GENERATION_UNOBSERVED, NATIVE_EXCLUSIVITY_NOT_OBSERVED,
        DIRECTORY_MEMBERSHIP_NOT_OBSERVED }
    public record RootPlan(int dimension, String providerType, Path dimensionDirectory, Path regionDirectory, boolean loaded) {
        public RootPlan {Objects.requireNonNull(providerType);Objects.requireNonNull(dimensionDirectory);Objects.requireNonNull(regionDirectory);}
    }
    public record Unresolved(int dimension,Issue issue) { public Unresolved {Objects.requireNonNull(issue);} }
    /** 只有 session 可发行；正常标准路径确实有 plans，但永不签 complete/absence。 */
    public static final class CatalogDomainObservation {
        private final SourceDomainSession issuer;
        private final SourceDomainIdentity identity;
        private final long observedGeneration, registryGeneration;
        private final GenerationObservation generationObservation;
        private final List<RootPlan> roots;
        private final List<Unresolved> unresolved;
        private final Throwable mutationFailure;
        private CatalogDomainObservation(SourceDomainSession issuer,long generation,RegistryStamp stamp,
                List<RootPlan> roots,List<Unresolved> unresolved) {
            this.issuer=issuer;this.identity=issuer.identity();this.observedGeneration=generation;
            this.registryGeneration=stamp.generation;this.generationObservation=stamp.hooksSeen?GenerationObservation.HOOK_CALLS_OBSERVED:GenerationObservation.GENERATION_UNOBSERVED;
            this.roots=List.copyOf(roots);this.unresolved=List.copyOf(unresolved);this.mutationFailure=stamp.failure;
        }
        public SourceDomainIdentity identity(){return identity;}
        public long observedGeneration(){return observedGeneration;}
        public long registryGeneration(){return registryGeneration;}
        public GenerationObservation generationObservation(){return generationObservation;}
        public List<RootPlan> roots(){return roots;}
        public List<Unresolved> unresolved(){return unresolved;}
        /** 受控内部诊断，不应打印其潜在私密 message。 */
        public Throwable mutationFailure(){return mutationFailure;}
        boolean belongsTo(SourceDomainSession session){return issuer==session;}
    }
    static CatalogDomainObservation issue(SourceDomainSession session,long generation,RegistryStamp stamp,List<RootPlan> roots,List<Unresolved> unresolved){return new CatalogDomainObservation(session,generation,stamp,roots,unresolved);}
    public static CatalogDomainObservation capture(SourceDomainSession session){return Objects.requireNonNull(session).capture();}
    record RegistryStamp(long generation,int mutations,boolean hooksSeen,Throwable failure) { }
    /** 与 DimensionManager 单一静态表对应的版本观察，不是 entity/global holder registry。 */
    static final class RegistryClock {
        private long generation;
        private int mutations;
        private boolean hooksSeen;
        private Throwable failure;
        synchronized void observedRead(){hooksSeen=true;}
        synchronized void beginMutation(){hooksSeen=true;generation=Math.addExact(generation,1);mutations=Math.addExact(mutations,1);}
        synchronized void endMutation(Throwable original){
            if(mutations<=0)throw new IllegalStateException("Unmatched actual registry mutation");
            mutations--;generation=Math.addExact(generation,1);if(original!=null&&failure==null)failure=original;
        }
        synchronized RegistryStamp snapshot(){return new RegistryStamp(generation,mutations,hooksSeen,failure);}
    }
    static final RegistryClock REGISTRY=new RegistryClock();
    // 合并后的 Mixin 方法属于 Forge package；public bridge 只记录失效/观察，不能发行 observation。
    // HOOK_CALLS_OBSERVED 不代表 transformed acceptance；正式安装须独立实证。
    public static void observeDimensionRegistryRead(){REGISTRY.observedRead();}
    public static void beginDimensionRegistryMutation(){REGISTRY.beginMutation();}
    public static void endDimensionRegistryMutation(Throwable original){REGISTRY.endMutation(original);}

}
