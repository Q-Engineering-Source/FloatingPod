package com.wi.floatingpod;

/**
 * 按物理端划分的初始化入口。
 *
 * <p>存在动机：客户端模型与渲染器只能在客户端装配，而 {@code doc/01-technical-baseline.md} 门禁 3
 * 要求「服务端路径不得加载 Gecko 客户端类」。把客户端装配收在本接口的客户端实现里，
 * 使专用服务端只加载不含渲染引用的实现，从类加载层面而非运行期判断上隔离两侧。
 *
 * <p>由 FML 的 {@code @SidedProxy} 按端实例化对应实现并注入。
 */
public interface SidedSetup {

    /**
     * 在 FML 预初始化阶段执行本端专属的装配。
     *
     * <p>公共侧的实体注册不在此进行，因为两端必须使用同一份注册代码；
     * 本方法只承担各端独有的部分。
     */
    void onPreInitialization();
}
