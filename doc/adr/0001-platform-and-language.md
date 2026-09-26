# ADR-0001：平台、语言与基础依赖

- 状态：已接受
- 日期：2026-08-06
- 修订：2026-08-07 明确 Lumenized 与 CTM 的侧别限定，决策实质未变
- 修订：2026-08-08 澄清 `bloom*` bone 的降级语义：伪发光是与 Lumenized 无关的基线，Lumenized 存在时在其上叠加辉光，缺失时仅保留伪发光而非普通渲染；决策语义有实质变化
- 修订：2026-08-10 决策第 8 条被 `doc/adr/0004-embed-geckolib.md` supersede：GeckoLib 及其 eliotlash 依赖改为以源码形式嵌入本仓库并改名到 `com.wi.floatingpod.vendored.**`，不再是外部 API 前置；决策第 6 条的 GeckoLib 强制前置清单相应调整，影响段落同步重写

## 背景

FloatingPod 面向 Minecraft 1.12.2 的 Cleanroom 生态，需要实现服务端权威的浮游辅助单元，并向整合包作者提供可脚本化行为。项目选择 Scalar 作为 Scala 语言提供器，选择 ZenScript 作为受控的整合包脚本接口，并依赖 GeckoLib 与 Draconic 生态实现模型渲染、能量和护盾联动。

项目必须保持独立，不能为兼容现有 Catroom 测试服或 PrototypeControl 等项目而扩大运行平台与运行时耦合范围。

## 决策

1. 运行平台固定为 Minecraft 1.12.2 / Cleanroom Only，运行时 Java 固定为 25。
2. 使用 `CleanroomModTemplate main` 的 Cleanroom `0.6.8-alpha` 与 Unimined `1.4.27-kappa` 作为构建基线；Scalar `>=3.8.4` 是正式最低前置。第一版实例选用官方分发 artifact `3.8.4-1`，其 Scalar `mcmod.info` / FML runtime mod version 为 `3.8.4`，并由构建 writer 合并 Scala 3 配置。
3. 使用满足 Scalar `>=3.8.4` 的 Scala 3 环境实现业务逻辑；不以 Scalar 的 Scala 2.11 库作为业务实现目标。第一版以 `scala3-library_3:3.8.4` 的 `compileOnly` 路径编译，并以 `required-after:scalar@[3.8.4,)` 约束运行时 Scalar。`3.8.4-1` 新增 `scala-parser-combinators_3`，但 FloatingPod 第一版不直接调用 parser API，也不声明 parser 直接依赖。
4. Mod 身份固定为 Mod ID `floatingpod`、JVM 基础包名 `com.wi.floatingpod`、显示名称 `[FP]浮游辅助单元-FloatingPod`，许可证为 LGPL-3.0。
5. ZenScript 是面向整合包作者的脚本生态；服务端负责脚本注册、校验与执行，客户端只负责输入、配置界面以及 Gecko 模型和动画表现。
6. Baubles `1.5.2`、Scalar `>=3.8.4`、CraftTweaker、ZenUtils `>=1.25.8`、官方 GeckoLib `3.0.31`、Draconic Evolution `2.3.28`、Draconic Additions `1.17.0.45` 与 BrandonsCore `>=2.4.19` 是正式强制前置。第一版 Scalar 实例分发 artifact 固定为 `3.8.4-1`，其 FML runtime mod version 为 `3.8.4`；不得用动态版本或把两种版本语义混写。**「官方 GeckoLib `3.0.31`」一项已于 2026-08-10 随第 8 条被 `doc/adr/0004-embed-geckolib.md` supersede 而失效**：GeckoLib 改为源码嵌入本仓库并改名到 `com.wi.floatingpod.vendored.**` 后不再是需要在测试实例中安装、解析的正式强制前置。Baubles、Scalar、CraftTweaker、ZenUtils、Draconic Evolution、Draconic Additions 与 BrandonsCore 七项不变。
7. Lumenized `1.0.3` 是客户端侧可选前置；配置启用 Lumenized 辉光时，CTM 是客户端侧条件强制前置。`bloom*` bone 始终呈现伪发光效果（满亮度、不受光照影响），Lumenized 存在时在此基础上叠加辉光效果，缺失时仅保留伪发光。二者均为渲染前置，专用服务端不要求也不检查。
8. GeckoLib 以外部官方 API 前置方式使用，FloatingPod 不 shade、重定位或内置 GeckoLib。兼容 API 的整合包定制构建可经新 Cleanroom 实例验证后使用。**本条已于 2026-08-10 被 `doc/adr/0004-embed-geckolib.md` 显式 supersede**：用户裁定将 GeckoLib 及其 eliotlash 依赖以源码形式嵌入本仓库并改名到 `com.wi.floatingpod.vendored.**`，不再作为外部官方 API 前置使用，也不再适用「不 shade、重定位或内置」的禁令。详见该 ADR 的背景、决策与代价。

## 影响

- 不承诺兼容普通 Forge、Catroom、Bukkit 或其他非 Cleanroom 环境。
- 当前 Catroom 测试服及其既有 Scalar/ZenUtils 组合不构成正式兼容性或发布验收环境；验收必须使用新建的 Cleanroom 测试实例。
- `0.6.8-alpha`、Unimined `1.4.27-kappa` 与第一版 Scalar `3.8.4-1` 分发 artifact（FML runtime mod version `3.8.4`）组合必须先通过独立构建审阅与加载探针。writer 的构建和直接互操作证据不能替代 reviewer，也不能替代客户端/专用服务端加载证据。
- 任何已验证的外部项目配置只能作为参考，不得形成 FloatingPod 对 PrototypeControl 或其他项目的构建、运行时或发布依赖。
- 发布产物必须避免重复打包 Scalar 提供的 Scala 运行时库。**「且服务端路径不得加载 Gecko 客户端类」原文的字面判据已于 2026-08-10 失效并重新定义**：GeckoLib 嵌入改名后「Gecko 客户端类」字面上不再存在，该判据会空转。新判据：**专用服务端执行后，已载入类中属 `com.wi.floatingpod.vendored.**` 且其常量池符号引用触及 `net/minecraft/client/**` 的类数必须为 0。** 判据是常量池符号引用，不是包名分层；包名可作辅助说明，但不得写成门禁判据。**旧方法（统计已载入的 `software.bernie.*` 全量）已作废**，原因是独立复验发现其子包过滤不完备：曾用 `model`/`renderers`/`resource` 三个子包枚举作负向证据，但 GeckoLib artifact 全部 1070 个 `software/bernie` class 中有 30 个引用 `net/minecraft/client`，三个子包只覆盖 13 个，漏掉 7 个 geckolib3 类与整个 `software/bernie/example/`；且实测服务端实际载入 4 个 `software.bernie.*` 类而非该方法报出的 2 个（详见 `Memory/PROJECT_STATE.md` 已完成事实段 C1-F1 记录）。这是新表述改用机械判据、不依赖人工枚举子包清单的直接理由，同一重定义见 `doc/01-technical-baseline.md` 门禁 3。
- 客户端侧 Lumenized 和 CTM 的依赖检查必须在启动或配置校验阶段显式失败，不能在渲染阶段静默产生不一致行为；专用服务端不要求也不检查这两项，服务端缺少二者不构成配置错误，启动或配置校验不得因此失败。

## 未包含的决策

- Maven group、artifact name、初始版本号、依赖校验哈希，以及官方 Actions / CurseForge 分发的长期保留策略。
- Scala 与 Java 的具体互操作边界。
- ZenScript API 的具体语法、审批流程、热更新事务与行为调度模型。
- Draconic Evolution API 的具体能量、护盾容量与伤害吸收合同。

这些事项应由后续 ADR 和兼容性探针决定，不得由本 ADR 隐含推导。
