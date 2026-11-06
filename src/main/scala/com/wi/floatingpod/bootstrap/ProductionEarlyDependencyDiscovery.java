package com.wi.floatingpod.bootstrap;

import com.cleanroommc.discovery.CleanroomModDiscoverer;
import net.minecraft.launchwrapper.Launch;
import net.minecraftforge.fml.relauncher.libraries.LibraryManager;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;

/** normal launch discovery/setup 完成后使用；绝不重跑 setup 或尝试解析业务类。 */
final class ProductionEarlyDependencyDiscovery implements EarlyDependencyResolver.Discovery {
    private final net.minecraft.launchwrapper.LaunchClassLoader loader;
    ProductionEarlyDependencyDiscovery() {
        loader = java.util.Objects.requireNonNull(Launch.classLoader, "actual Launch loader unavailable");
    }
    @Override public EarlyDependencyResolver.Snapshot snapshot(List<String> resources) throws IOException {
        var discoverer = CleanroomModDiscoverer.instance();
        List<Path> sql = List.copyOf(discoverer.modSources("sqljdbcdriverlib")).stream().map(File::toPath).toList();
        List<Path> scalar = List.copyOf(discoverer.modSources("scalar")).stream().map(File::toPath).toList();
        List<Path> fugue = List.copyOf(discoverer.modSources("fugue")).stream().map(File::toPath).toList();
        List<Path> candidates = List.copyOf(LibraryManager.getCandidates()).stream().map(File::toPath).toList();
        List<Path> sources = EarlyDependencyResolver.localLaunchSources(List.copyOf(loader.getSources()));
        return new EarlyDependencyResolver.Snapshot(sql, scalar, fugue, candidates, sources, resources(resources));
    }
    @Override public void addURL(URL url) {
        requireSameLoader();
        loader.addURL(url);
    }
    @Override public List<EarlyDependencyResolver.ResourceOrigin> resources(List<String> anchors) throws IOException {
        requireSameLoader();
        return EarlyDependencyResolver.inspectResources(loader, anchors);
    }
    private void requireSameLoader() {
        if (Launch.classLoader != loader) throw new EarlyDependencyResolver.DependencyUnavailable("Launch loader changed during assembly");
    }
}

