package com.wi.floatingpod.client.render;

/**
 * 辉光几何的绘制入口。
 *
 * <p>存在动机：Lumenized 的辉光回调发生在实体渲染之后，不在实体的矩阵栈内，
 * 因此需要在回调时重新定位并提交一次 {@code bloom} 子树的几何。该提交逻辑必须与实体渲染
 * 使用的同一套 bone 递归和缺面判空实作，不能另写一份，否则两条路径会随资产变化而分叉。
 *
 * <p>本接口是辉光桥接与渲染器之间的唯一契约。它刻意不引用任何 Lumenized 类型，
 * 使实现方（渲染器）的常量池保持不含 {@code gregtech} 前缀符号，从而在 Lumenized 缺失时
 * 仍可安全加载。
 */
public interface BloomPainter {

    /**
     * 在辉光回调的坐标系下绘制一个实体的 {@code bloom} 子树。
     *
     * <p>调用方位于 Lumenized 的辉光绘制阶段，此时 OpenGL 矩阵是世界渲染矩阵，
     * 原点为摄像机位置，因此实现方须自行按摄像机偏移定位，并自行完成矩阵的入栈与出栈。
     *
     * @param request 实体渲染阶段捕获的定位与动画参数。
     * @param cameraX 摄像机插值后的世界 X 坐标。
     * @param cameraY 摄像机插值后的世界 Y 坐标。
     * @param cameraZ 摄像机插值后的世界 Z 坐标。
     */
    void paintBloom(BloomRequest request, double cameraX, double cameraY, double cameraZ);
}
