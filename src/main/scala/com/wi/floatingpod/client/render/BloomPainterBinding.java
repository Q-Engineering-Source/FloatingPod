package com.wi.floatingpod.client.render;

import java.util.Objects;

/**
 * 辉光绘制者的延迟绑定。
 *
 * <p>存在动机：辉光桥接必须在客户端预初始化阶段就向 Lumenized 注册，而实体渲染器由
 * vanilla 的渲染工厂在首次需要渲染该实体时才创建。两个时机不同，桥接无法在注册时拿到渲染器。
 * 本类作为二者之间的可变引用点：渲染器创建时把自己写入，桥接在每帧回调时读取。
 *
 * <p>本类刻意不引用任何 Lumenized 类型，因此可被渲染器安全持有；桥接则只经本类取得绘制者，
 * 无需让渲染器认识 Lumenized 的任何接口。
 *
 * <p>只在客户端渲染线程与客户端预初始化阶段访问，二者不并发，故不作同步。
 */
public final class BloomPainterBinding {

    /** 当前已绑定的绘制者；渲染器创建前为 {@code null}。 */
    private BloomPainter painter;

    /**
     * 创建尚未绑定的实例。
     */
    public BloomPainterBinding() {
    }

    /**
     * 绑定实际执行绘制的实现。
     *
     * <p>由实体渲染器在构造时调用。vanilla 对同一实体类型只创建一个渲染器实例，
     * 因此正常情况下只会绑定一次；若渲染器被重建（例如资源重载），后一次绑定覆盖前一次，
     * 指向当前真正参与渲染的实例。
     *
     * @param painter 非空的绘制者。
     */
    public void bind(BloomPainter painter) {
        this.painter = Objects.requireNonNull(painter, "painter");
    }

    /**
     * 是否已绑定绘制者。
     *
     * @return 已绑定时返回 {@code true}。
     */
    public boolean isBound() {
        return painter != null;
    }

    /**
     * 返回已绑定的绘制者。
     *
     * <p>未绑定时直接抛出而不静默跳过：调用方在调用前必须先确认队列非空，
     * 而队列非空意味着渲染器已经运行过、必然已完成绑定。若此处为空，说明上述前提被破坏，
     * 属于需要暴露的缺陷。
     *
     * @return 已绑定的绘制者。
     */
    public BloomPainter painter() {
        if (painter == null) {
            throw new IllegalStateException("辉光绘制者尚未绑定，但辉光队列已有待绘制请求");
        }
        return painter;
    }
}
