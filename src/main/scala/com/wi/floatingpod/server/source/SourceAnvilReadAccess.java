package com.wi.floatingpod.server.source;

/** formal Main installer must have the actual transformed loader; uninstalled makes no source claim. */
public interface SourceAnvilReadAccess {
    void floatingpod$installSourceLoadManifest(SourceLoadManifest actualManifest);
    /** 原Main解除同一已停止的consumer；loader保留禁止重新进入的终态，不授物理结算资格。 */
    void floatingpod$detachSourceLoadManifest(SourceLoadManifest actualManifest);
}
