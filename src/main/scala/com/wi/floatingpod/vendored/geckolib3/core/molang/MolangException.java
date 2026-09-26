/*
 * FloatingPod 对 Molang 求值表面的独立实现。
 * 因上游 mclib 的许可证无法取得，本文件未复制任何第三方代码。
 */
package com.wi.floatingpod.vendored.geckolib3.core.molang;

/**
 * 表示 Molang 表达式无法解析或不符合已登记上下文的受检错误。
 */
public final class MolangException extends Exception {

    /** 序列化兼容标识。 */
    private static final long serialVersionUID = 1L;

    /**
     * 使用可诊断消息创建异常。
     *
     * @param message 错误原因
     */
    public MolangException(String message) {
        super(message);
    }
}
