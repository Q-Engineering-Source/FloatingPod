package com.wi.floatingpod.bootstrap;

import java.util.Objects;

/** Java early ABI；只有真实依赖验证完成后的method body才触达late Scala assembly。 */
public final class ProductionDedicatedBootstrapFactory implements DedicatedBootstrapBridge.Factory {
    private final ProductionBootstrapControl.LaunchContract contract;
    private final DedicatedBootstrapBridge.Limits limits;
    private final ManagedDatabaseConfiguration managedConfiguration;
    public ProductionDedicatedBootstrapFactory(ProductionBootstrapControl.LaunchContract contract,DedicatedBootstrapBridge.Limits limits) {
        this.contract=Objects.requireNonNull(contract);this.limits=Objects.requireNonNull(limits);this.managedConfiguration=null;
    }
    public ProductionDedicatedBootstrapFactory(ManagedDatabaseConfiguration managedConfiguration,DedicatedBootstrapBridge.Limits limits) {
        this.contract=null;this.limits=Objects.requireNonNull(limits);this.managedConfiguration=Objects.requireNonNull(managedConfiguration);
    }
    @Override public DedicatedBootstrapBridge.Callbacks open(Object actualServer,DedicatedBootstrapBridge control) throws Exception {
        Objects.requireNonNull(actualServer);Objects.requireNonNull(control);
        if(Thread.currentThread()!=control.controlThread() || control.phase()!=DedicatedBootstrapBridge.Phase.PREPARING)
            throw new IllegalStateException("Exact preparing bootstrap control required before discovery");
        if(managedConfiguration!=null) {
            if(!ManagedBootstrapPolicy.limits(managedConfiguration.initializationTimeoutMillis()).equals(limits))
                throw new IllegalStateException("Managed configuration timeout changed after installation selection");
            // Verify release pins before touching the Scala/SQL initialization chain. Factory.open never opens JDBC.
            var dependencies=new EarlyDependencyResolver(Thread.currentThread(),ManagedReleaseDependencies.expectedPins()).resolveAndVerifyProduction();
            return ProductionDbFirstBootstrapCallbacks$.MODULE$.openManaged(actualServer,control,managedConfiguration,limits,dependencies);
        }
        if(contract==null)throw new IllegalStateException("Exact controlled or managed bootstrap source required");
        var format=ProductionBootstrapControl.discoverProductionFormat(contract);
        if(format==ProductionBootstrapControl.ProductionFormat.DB_FIRST){
            var input=ProductionBootstrapControl.loadDbFirst(contract);
            if(!input.policy().limits().equals(limits))throw new IllegalArgumentException("Launch and approved control timeouts differ");
            var dependencies=new EarlyDependencyResolver(Thread.currentThread(),input.dependencyPins()).resolveAndVerifyProduction();
            return ProductionDbFirstBootstrapCallbacks$.MODULE$.open(actualServer,control,input,dependencies);
        }
        if (format==ProductionBootstrapControl.ProductionFormat.DEPLOYMENT_RECEIPT) {
            var input=ProductionBootstrapControl.loadDeploymentReceipt(contract);
            if(!input.policy().limits().equals(limits))throw new IllegalArgumentException("Launch and approved control timeouts differ");
            var dependencies=new EarlyDependencyResolver(Thread.currentThread(),input.dependencyPins()).resolveAndVerifyProduction();
            return ProductionDedicatedBootstrapCallbacks$.MODULE$.openDeploymentReceipt(actualServer,control,input,dependencies);
        }
        if (format==ProductionBootstrapControl.ProductionFormat.V3) {
            var input=ProductionBootstrapControl.loadV3(contract);
            if(!input.policy().limits().equals(limits))throw new IllegalArgumentException("Launch and approved control timeouts differ");
            var dependencies=new EarlyDependencyResolver(Thread.currentThread(),input.dependencyPins()).resolveAndVerifyProduction();
            return ProductionDedicatedBootstrapCallbacks$.MODULE$.openV3(actualServer,control,input,dependencies);
        }
        ProductionBootstrapControl input=ProductionBootstrapControl.load(contract);
        if(!input.isProductionFormatV2())
            throw new IllegalArgumentException("Production dedicated bootstrap requires the exact V2 FRESH or ORDINARY_CERTIFIED control");
        if(!input.policy().limits().equals(limits))throw new IllegalArgumentException("Launch and approved control timeouts differ");
        var dependencies=new EarlyDependencyResolver(Thread.currentThread(),input.dependencyPins()).resolveAndVerifyProduction();
        // direct late call: no reflection, context loader, public raw JDBC or caller success callback.
        return ProductionDedicatedBootstrapCallbacks$.MODULE$.open(actualServer,control,input,dependencies);
    }
}
