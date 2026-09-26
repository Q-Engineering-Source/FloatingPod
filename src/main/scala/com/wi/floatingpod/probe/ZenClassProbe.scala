package com.wi.floatingpod.probe

import crafttweaker.annotations.ZenRegister
import stanhebben.zenscript.annotations.ZenClass
import stanhebben.zenscript.annotations.ZenMethod

/**
 * 门禁 6 实作段的最小 ZenScript 互操作样本：验证 Scala 3 `object` 编译产物携带
 * `@ZenRegister`/`@ZenClass` 时，能否被 FML 的 ASM 扫描识别为可注册的 ZenClass，
 * 并验证 `@ZenMethod` 方法能否被 ZenScript 脚本实际调用。
 *
 * `@ZenMethodStatic` 专属于 `@ZenExpansion` 场景（反编译 CraftTweaker 自身
 * `crafttweaker.mc1120.util.expand.ExpandRandom` 确认），`ZenTypeNative.complete`
 * 处理常规 `@ZenClass` 成员时只识别 `@ZenMethod` 并配合 JVM 反射
 * `Modifier.isStatic` 判定是否落入 `staticMembers`；不涉及 `@ZenMethodStatic`。
 */
@ZenRegister
@ZenClass("floatingpod.probe.ZenClassProbe")
object ZenClassProbe:

  /** 供 ZenScript 脚本调用的稳定标记值，用于验证功能性正向证据而非仅"没报错"。 */
  val PROBE_MARKER: String = "floatingpod-zenclass-probe-ok"

  /**
   * 返回稳定标记值，供正常脚本引用以证明该 ZenClass 已被正确注册且可用。
   *
   * @return 稳定的探针标记字符串。
   */
  @ZenMethod
  def probeValue(): String = PROBE_MARKER
