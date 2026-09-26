# 技术基线

## 已确认平台与身份

| 组成 | 约束 | 状态 |
| --- | --- | --- |
| Minecraft | 1.12.2 | 已确认 |
| 运行平台 | Cleanroom Only | 已确认 |
| Java | 25 | 已确认 |
| Scala 提供器 | Scalar `>=3.8.4`（最低前置）；第一版官方分发 artifact `3.8.4-1`，FML runtime mod version `3.8.4` | 第一版构建 writer 已完成，待独立审阅与实例加载验证 |
| 实现语言 | Scalar 提供的 Scala 3 环境 | 已确认 |
| 脚本接口 | ZenScript | 已确认 |
| Mod ID | `floatingpod` | 已确认 |
| JVM 基础包名 | `com.wi.floatingpod` | 已确认 |
| 显示名称 | `[FP]浮游辅助单元-FloatingPod` | 已确认 |
| 许可证 | GNU Lesser General Public License Version 3, 29 June 2007（LGPL-3.0） | 已确认 |

FloatingPod 是独立项目。它可以复用已经验证的 Cleanroom、Gradle、CraftTweaker 与渲染依赖配置，但不得依赖 PrototypeControl 或其他尚未稳定项目的运行时、源码或发布节奏。

Scalar 是 Cleanroom 生态的 Scala 语言提供器，为编译时和运行时分别提供 Scala 2.11 与 Scala 3 库。本项目仅以满足 Scalar `>=3.8.4` 最低前置的 Scala 3 路线实现业务逻辑，不将 Scala 2.11 作为业务实现目标。第一版实例前置选用官方分发 artifact `3.8.4-1`；该 artifact 的 Scalar `mcmod.info` / FML runtime mod version 是 `3.8.4`，不能把两者简写成同一版本。`3.8.4-1` 新增 `scala-parser-combinators_3`，但 FloatingPod 第一版不直接调用 parser API，也不声明它为直接依赖。

## 构建基线

构建脚手架以 `CleanroomModTemplate` 的 `main` 分支为基础：

| 组成 | 固定基线 | 使用方式 |
| --- | --- | --- |
| Cleanroom | `0.6.8-alpha` | 模组唯一目标运行平台 |
| Unimined | `1.4.27-kappa` | 模板所用构建插件基线 |
| Scalar | `>=3.8.4`（最低前置）；第一版分发 artifact `3.8.4-1` | 构建以 `scala3-library_3:3.8.4` 的 `compileOnly` 路径编译；FML 以 runtime mod version `3.8.4` 约束，测试实例安装完整 `3.8.4-1` artifact |

该组合是项目构建基线，不是已完成的兼容性保证。阶段 1-B writer 已完成 Scala Gradle 配置、`@Mod` 入口和 Java/Scala 互操作探针：`scala_version` 为 `3.8.4`，FML 约束为 `required-after:scalar@[3.8.4,)`，实例分发 artifact 为完整 `3.8.4-1`。writer 已报告干净构建、依赖解析、直接互操作行为和产物边界通过，但这些证据必须由独立 reviewer 复验；随后仍须在新建的 Cleanroom 客户端与专用服务端实例中验证加载。不得把构建成功或人工下载成功误称为运行时兼容性。

不得承诺兼容普通 Forge、Catroom、Bukkit 或其他非 Cleanroom 运行环境。当前 `1001 - Dim-Transit-Station` 测试服为 Catroom 与既有 Scalar/ZenUtils 组合，只能用于观察既有环境，不得作为 FloatingPod 的正式兼容性或发布验收环境。

## 依赖合同

以下为正式强制前置，实际 Maven 坐标、许可证信息与依赖解析结果必须在构建探针中固化：

| 依赖 | 版本约束 | 作用 |
| --- | --- | --- |
| Baubles | `1.5.2` | 辅助机终端饰品槽支持 |
| Scalar | `>=3.8.4` | Scala 3 编译与运行时提供器；第一版安装完整官方分发 artifact `3.8.4-1`，其 FML runtime mod version 为 `3.8.4` |
| CraftTweaker | 项目目标整合包提供的兼容版本 | ZenScript 宿主与脚本集成 |
| ZenUtils | `>=1.25.8` | ZenScript 配套能力 |
| GeckoLib | 官方 API `3.0.31`（版本识别以 MANIFEST `Implementation-Version` 或 SHA-256 为准，不以 FML runtime mod version 为准，见下文） | 实体模型与动画 |
| Draconic Evolution | `2.3.28` | 能量与护盾联动 API |
| Draconic Additions | `1.17.0.45` | 目标整合包依赖合同 |
| BrandonsCore | `>=2.4.19` | Draconic Evolution 的派生强制前置 |

**注（2026-08-10 起效，GeckoLib 行的强制前置语义部分失效）**：上表 GeckoLib 行原文记录的是「作为执行期外部前置安装」这一状态，保留不删以作历史记录。用户于 2026-08-10 裁定将 GeckoLib 及其 eliotlash 依赖以源码形式嵌入本仓库并改名到 `com.wi.floatingpod.vendored.**`（决策见 `doc/adr/0004-embed-geckolib.md`），嵌入后 GeckoLib 不再是执行期需要在测试实例中安装、解析并加载的正式强制前置，其 API 表面身份改由本节稍后的「嵌入源身份记录」段落承载。Baubles、Scalar、CraftTweaker、ZenUtils、Draconic Evolution、Draconic Additions 与 BrandonsCore 七项不受影响，仍是正式强制前置。

Lumenized `1.0.3` 是客户端侧可选前置。启用其辉光路径时，CTM 是客户端侧条件强制前置；未启用 Lumenized 时，基础安装不要求 CTM。二者均为渲染前置，安装在专用服务端不产生效果；服务端不要求也不检查这两项，服务端缺少二者不构成配置错误，启动或配置校验不得因此失败。

GeckoLib 使用官方 `3.0.31` 的 API 作为外部前置，不得 shade 或重打包进 FloatingPod。**该禁令已于 2026-08-10 被 `doc/adr/0004-embed-geckolib.md` supersede，失效理由见该 ADR：GeckoLib 及其 eliotlash 依赖改为以源码形式嵌入本仓库并改名到 `com.wi.floatingpod.vendored.**`，不再是外部前置。** 官方 1.12 分支在 `3.0.31` 发布时把 `gradle.properties` 的 `mod_version` 提升为 `3.0.31`，但未同步修改 `GeckoLib.java` 的 `VERSION` 常量，导致该常量与随之派生的 FML runtime mod version 仍是 `3.0.30`；这是上游发布疏漏，不是本项目环境问题，用户已裁定不构成阻塞。**该版本识别规则的适用范围已收窄**：它记录的是上游发布疏漏这一事实，嵌入后本项目不再依赖 geckolib3 的 FML 注册，故不再是本项目的版本校验依据，仅作为对上游的描述保留。不得以 FML join list 的版本号（如 `geckolib3@3.0.30`）作为依赖约束或版本校验依据，否则约束会落空；版本识别须以构建产物 MANIFEST 的 `Implementation-Version` 或 SHA-256 为准。若整合包采用同 API 兼容的定制 GeckoLib 构建，可在新 Cleanroom 验收实例中验证后使用；该兼容性不是对定制实现行为的无条件承诺，当前验收实例安装的即为此类定制 Foundation 构建而非官方分发 artifact，两者仅靠哈希区分。

**嵌入源身份记录（2026-08-10 新增，取代原版本识别规则在本项目内的约束作用）**：上游仓库 `bernie-g/geckolib`，提交 `9fd044b`（tag `v3.0.31`），权威检出为 `GeckoLib-Foundation-src`；其 95 个 geckolib3 源码文件与分发 jar 内源码逐字节全同（95/95，零差异零缺失）。**待取证（不得视为已完成）**：把 `Lib-Geckolib-forge-1.12.2-3.0.31.jar` 从测试实例移除后，客户端与专用服务端能否正常加载；该取证由 L0 在客户端阶段执行，本节仅记录为待办，不构成已完成事实，也不得据此改动 `FloatingPod-test/**`。

客户端 Gecko 模型中以 `bloom` 开头的 bone 在任何情况下都渲染为伪发光效果（满亮度、不受世界光照影响）；Lumenized 可用时在此基础上叠加辉光效果，Lumenized 缺失时保留伪发光效果但不叠加辉光，且均不影响模型、动画或实体功能。

**实际资产结构**（2026-08-23 补充）：阶段 1 实作的 `floating_pod.geo.json` 定义 5 个 bloom bone（`bloom_bone`、`bloom_bone2`、`bloom_bone3`、`bloom_bone4`、`bloom_bone5`），`floating_pod.animation.json` 定义 3 个动画（`idle`、`Weapon_on`、`Weapon_on_idle`）。代码中 `BLOOM_BONE_PREFIX = "bloom"` 正确识别所有以 `bloom` 开头的 bone，门禁 5 判据只要求「以 `bloom` 开头的 bone 呈现伪发光/辉光效果」，不限定 bone 数量或具体命名。动画名称 `Weapon_on` 与 `Weapon_on_idle` 是占位命名，阶段 2 实作时可重命名为符合产品语义的名称（如 `active`、`working` 等）。

阶段 2 的 SQLite 单人/独立单服 backend 需要的 SQLite 驱动由独立前置模组提供，FloatingPod 不打包、不 shade、不重定位该 JDBC 驱动。**该产物边界原文与「不得重复打包 Scalar 或 GeckoLib」并列为同一条产物边界原则；此并列关系已于 2026-08-10 失效**：GeckoLib 已改为源码嵌入并改名（见上文与 `doc/adr/0004-embed-geckolib.md`），不再适用「不得重复打包」这一表述；Scalar 与本条 SQLite 驱动的边界不变，二者仍各自独立成立，不因 GeckoLib 一项解除而受影响。该前置模组在需要时另建独立仓库构建，其 Maven 坐标、版本与许可证尚未确定，属阶段 2 决策；当前不得写成已确认前置，也不得加入本节依赖合同表格。

## 工程约束

- Gradle 缓存统一放置于 `C:/GradleCaches/FloatingPod`。
- 所有文本文件使用 UTF-8 无 BOM。
- JVM 代码禁止使用完全限定名代替 `import`。
- 业务实现和测试禁止使用反射，除非符合项目规范中明确允许的框架情形。
- 逻辑层优先依赖接口；接口命名为 `Xxx`，实现类命名为 `XxxImpl`。
- 错误处理遵循 Fail Fast；异常必须记录，不允许空 `catch`。
- 服务端核心循环必须隔离单个浮游单元或脚本行为的失败，避免未捕获异常终止整个进程。

## 基线验收门禁

1. 使用固定的 `C:/GradleCaches/FloatingPod` Gradle 缓存完成干净构建，并锁定全部正式前置的解析结果。
2. 独立 reviewer 复验第一版官方分发 artifact `3.8.4-1` 的来源、其 Scalar FML runtime mod version `3.8.4`、ContainedDeps、Scala 3 编译路径和最终产物内容；构建产物不得重复打包 Scalar、Scala 标准库或 `scala-parser-combinators_3`。
3. 新建 Cleanroom 测试实例中的客户端与专用服务端均能加载 FloatingPod。**「服务端路径不得加载 Gecko 客户端类」原文的字面判据已于 2026-08-10 失效并重新定义**：GeckoLib 嵌入改名后「Gecko 客户端类」字面上不再存在，该判据会空转。新判据：**专用服务端执行后，已载入类中属 `com.wi.floatingpod.vendored.**` 且其常量池符号引用触及 `net/minecraft/client/**` 的类数必须为 0。** 判据是常量池符号引用，不是包名分层；嵌入实作会按 side 分包，包名可作辅助说明，但不得写成门禁判据——约定可能被后续改动打破，常量池引用是产物内的客观事实。**旧方法（统计已载入的 `software.bernie.*` 全量）已作废**，原因是独立复验发现其子包过滤不完备：曾用 `model`/`renderers`/`resource` 三个子包枚举作负向证据，但 GeckoLib artifact 全部 1070 个 `software/bernie` class 中有 30 个引用 `net/minecraft/client`，三个子包只覆盖 13 个，漏掉 7 个 geckolib3 类与整个 `software/bernie/example/`；且实测服务端实际载入 4 个 `software.bernie.*` 类而非该方法报出的 2 个（详见 `Memory/PROJECT_STATE.md` 已完成事实段 C1-F1 记录）。这是新表述改用机械判据、不依赖人工枚举子包清单的直接理由。
4. **「官方 GeckoLib 3.0.31」的依赖链要求已于 2026-08-10 失效**：嵌入后实例内不再安装 GeckoLib，本条不再要求 GeckoLib 参与依赖链验证。CraftTweaker、ZenUtils、Baubles 与 Draconic 依赖链在该实例中能共同解析和加载，四项要求不变。
5. Lumenized 缺失时，`bloom` bone 必须呈现伪发光效果（满亮度、不受世界光照影响），且不破坏实体加载、模型或动画；Lumenized 确认已加载时，须在该伪发光效果之上额外呈现辉光效果，同样不破坏实体加载、模型或动画。两条路径须在客户端实例分别独立取证并记录，其中辉光路径的取证须以 Lumenized 确认已加载为前提，以区分「伪发光正常但辉光未叠加」与「Lumenized 缺失时的正确降级」这两种情况；专用服务端不参与本门禁判定。
6. ZenScript 注册、编译或加载错误能输出完整定位信息，并阻止无效定义进入运行期。

未通过上述门禁前，不得将构建基线或任一依赖组合标记为已兼容。
