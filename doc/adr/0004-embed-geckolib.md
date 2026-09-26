# ADR-0004：嵌入 GeckoLib 与 eliotlash 依赖

- 状态：已接受
- 日期：2026-08-10

## 背景

FloatingPod 依赖 GeckoLib `3.0.31` 实现实体模型与动画（见 `doc/adr/0001-platform-and-language.md` 决策第 6、8 条）。此前该依赖以外部官方 API 前置方式使用，不 shade、不重定位、不内置。阶段 1 门禁 5 的实作与取证过程中，用户于 2026-08-10 裁定改变这一方式：将 GeckoLib 及其 eliotlash（`mclib`/`molang`）依赖以源码形式嵌入本仓库，并改名到 `com.wi.floatingpod.vendored.**`。

动机是两项已在本仓库实测坐实、且均落在上游代码内部、无法从外部修复的缺陷：

1. **`IGeoRenderer.renderCube` 对缺面 cube 的 null quad 不判空。** `GeoCube.createFromPojoCube` 对 per-face UV 的每个面做 `west == null ? null : new GeoQuad(...)`，再无条件填入定长数组 `quads = new GeoQuad[6]`；而 `IGeoRenderer.renderCube` 以 `for (GeoQuad quad : cube.quads)` 遍历后直接读 `quad.normal`，不判空。任何走该方法的路径，在模型存在缺面 cube 时都会 NPE。本项目已实测：106 个 cube 全部使用 per-face UV，79 个面数少于 6，共产生 203 个 null quad，`/summon` 即崩溃（阻塞 C，已在 `Memory/PROJECT_STATE.md` 记录并修复，修法是本项目内自建 `submitCube`/`submitQuad` 逐面判空取代对 `renderCube` 的调用）。
2. **`AnimationProcessor.tickAnimation` 以实例字段 `lastTickValue` 与 `animatedEntities` 做同帧去重。** 该 processor 由 `AnimatedGeoModel` 构造函数唯一创建，随共享的模型提供器同生命周期，因此为同一实体类型的全部实体共用；叠加 `GeckoLibCache` 按资源缓存 `GeoModel` 使骨骼对象跨实体共享，导致同一渲染帧内 `seekTime` 恰好相等的多个实体会互相套用姿态（阻塞 E，已在 `Memory/PROJECT_STATE.md` 记录，尚未修复）。

两项缺陷都是 `software.bernie.geckolib3` 包内部的实现细节，本项目在保持「外部 API 前置、不修改其源码」的方式下无法修复；只有把源码纳入本仓库维护，才能直接修改这些实现。这是嵌入的核心理由，而非单纯的打包偏好变化。

## 决策

1. **GeckoLib 及其 eliotlash 依赖以源码形式嵌入本仓库，包名改为 `com.wi.floatingpod.vendored.**`。** 嵌入后 GeckoLib 不再是执行期需要在测试实例中安装、解析、加载的外部前置；本项目对其源码拥有直接修改权，用于修复阻塞 C、阻塞 E 等上游内部缺陷。
2. **本决策显式 supersede `doc/adr/0001-platform-and-language.md` 决策第 8 条**（「GeckoLib 以外部官方 API 前置方式使用，FloatingPod 不 shade、重定位或内置 GeckoLib」）。该条款不再适用；ADR-0001 已在决策第 6、8 条与影响段落逐处标注失效理由并保留原文，不删除历史结论。
3. **嵌入源身份**：上游仓库 `bernie-g/geckolib`，提交 `9fd044b`（tag `v3.0.31`），权威检出为只读参考仓库 `GeckoLib-Foundation-src`。该提交的 95 个 `software.bernie.geckolib3.*` 源码文件（`src/main/java` 与 `core/src/main/java` 合计）与分发 jar 内打包的源码逐字节全同（95/95，零差异零缺失），确认嵌入源与本项目此前编译期/执行期依赖的分发 artifact 是同一份代码。
4. **门禁 3「服务端路径不得加载 Gecko 客户端类」的判据同步重定义**：嵌入改名后「Gecko 客户端类」字面上不再存在，原判据会空转。新判据为机械可判定的常量池符号引用：专用服务端执行后，已载入类中属 `com.wi.floatingpod.vendored.**` 且其常量池符号引用触及 `net/minecraft/client/**` 的类数必须为 0。判据是常量池符号引用，不是包名分层——嵌入实作会按 side 分包，包名可作辅助说明，但不得写成门禁判据本身，因为分包约定可能被后续改动打破，而常量池引用是产物内的客观事实。该重定义已同步写入 `doc/01-technical-baseline.md` 门禁 3 与 `doc/adr/0001-platform-and-language.md` 影响段。旧方法（统计已载入的 `software.bernie.*` 全量后按子包枚举筛选客户端类）已作废：独立复验发现其子包过滤不完备，`model`/`renderers`/`resource` 三个子包枚举只覆盖 GeckoLib artifact 全部 1070 个 `software/bernie` class 中 30 个客户端引用类的 13 个，漏掉 7 个 geckolib3 类（`animation/AnimationTicker`、`file/AnimationFileLoader`、`file/GeoModelLoader`、`item/GeoArmorItem`、`util/AnimationUtils`、`util/GeoUtils`、`util/json/JsonAnimationUtils`）与整个 `software/bernie/example/`；且实测服务端实际载入 4 个 `software.bernie.*` 类而非该方法当时报出的 2 个（详见 `Memory/PROJECT_STATE.md` 已完成事实段 C1-F1 记录）。
5. **两份 `AnimationController.addModelFetcher` 注册不构成互相干扰。** 嵌入改名后，本项目的模型提供器注册与上游 `software.bernie` 版本（若仍以任何形式存在于其他环境）在型别上无法互相干扰，因为二者是不同的独立静态注册表；本项目内是独立类的独立静态注册，与上游命名空间不重叠。此前记录为待实测的疑虑已消解，不再是本决策的开放问题。
6. **2026-08-11，`L0-n1` 在用户委托下作出工程裁定**（用户指示「按你的建議來解決許可證問題，不要詢問我，我不在意許可證方面的問題，按合規的做法來即可」）：结合影响段的实测事实，eliotlash 授权不可得而非尚未找到（sources jar 返回 404、二进制 jar 为空、无 `LICENSE`/`NOTICE`、POM 无授权文本且无源码可查），因此不嵌入、shade、重定位或依赖任何 eliotlash artifact，改为从零重新实现且仅实现 GeckoLib 实际调用面所需的五个类型 `IValue`、`Variable`、`MolangParser`、`MolangException`、`Interpolations`，该集合不是通过阅读 eliotlash 代码得出；禁止反编译或改述 eliotlash 实现，今后仅可使用 GeckoLib 调用点可见的类名/成员名和公开 Molang 语言文档，行为依据语言规范与 GeckoLib 需求而非其字节码；重实现行为无需复制上游缺陷，`lerpYaw` 必须走短弧（`350f`→`10f` 在 `0.5f` 得 `0f` 而非 `180f`），且 `JsonKeyFrameUtils.getEasingArgs` 的 raw `List<IValue>`→`List<Double>` 类型转换缺陷与关键帧键值数字解析不对称缺陷亦不得复制。

## 影响

- **上游代码纳入本仓库维护，后续上游修复不再自动获得。** GeckoLib 官方仓库若发布 `3.0.31` 之后的修复（包括但不限于阻塞 C、阻塞 E 对应的上游 bug fix），本项目不会自动获得，须自行移植或自行维护对应逐字节比对与合并流程。
- **嵌入源码带上游授权标头，需在本仓库内保留。** 经核实，23 个 `software.bernie.geckolib3.*` 源码文件（`src` 与 `core/src` 内，不含 `example` 包）带 `Copyright (c) 2020. Author: Bernie G. (Gecko)` 版权声明；另有 1 个文件（`core/src/main/java/software/bernie/geckolib3/core/easing/EasingManager.java`）不带该版权声明，但嵌有两段第三方致谢：`Facebook, Inc. and its affiliates` 与 `MIT Copyright (c) 2015 Boris Chumichev`，共计 24 个带有某种授权/致谢标头的文件。权威检出 `GeckoLib-Foundation-src` 本身为双授权：根 `LICENSE` 为 MIT（`Copyright (c) 2021 GeckoThePecko`），`LICENSE.md` 为 GNU LGPL v3；`core/LICENSE` 另为 MIT。本项目自身许可证为 LGPL-3.0（见 `doc/01-technical-baseline.md`「已确认平台与身份」）。嵌入时须保留全部原始版权与致谢标头，不得移除或篡改。
- **eliotlash（`mclib`/`molang`）的源码与授权未取得，是未解决的阻塞。** **该结论已于 2026-08-11 被 `L0-n1` 在用户委托下 supersede 而失效**：见「决策」第 6 条。该缺失现因不使用其表达而不再构成阻塞。已实测的事实：
  - 实际版本是 `v19` 而非 `v18`。权威检出 `GeckoLib-Foundation-src` 的根 `build.gradle` 声明 `com.eliotlash.molang:molang:18` 与 `com.eliotlash.mclib:mclib:18`（且以 `shade` 配置重打包），但 `core/build.gradle` 声明的是 `com.eliotlash.molang:molang:19` 与 `com.eliotlash.mclib:mclib:19`（`implementation` 配置）；`core` 模块是 `IValue` 等关键动画数据类型的实际使用方，故实际生效版本是 `19` 而非根模块声明的 `18`。
  - `mclib-19-sources.jar` 与 `molang-19-sources.jar` 在 cloudsmith 均返回 HTTP 404；对应二进制 jar 可下载，但内含 0 个 `.java` 文件，`META-INF/` 只有 `MANIFEST.MF`，无 `LICENSE`/`NOTICE`；其 `.pom` 无 `licenses`/`scm`/`developers` 节点。工作区、Gradle 缓存与全局搜索均零命中源码或授权文件。`mchorse/mclib` 的 1.12 分支为 MIT 授权，但版本明确不同，不得当作替代源码使用。
  - `IValue` 是本项目实际依赖的关键帧数据模型的结构性类型，无法从依赖图中移除，故该阻塞直接卡住嵌入的完整性：嵌入后仍缺失 eliotlash 部分的源码与可验证授权。
  - 这是待使用者裁决的授权问题，不是工程任务；本决策不裁定任何第三方重打包授权可转授，也不宣称已有解决方案。**其中「待使用者裁决的授权问题，不是工程任务」及「不宣称已有解决方案」已于 2026-08-11 被 `L0-n1` 在用户委托下 supersede 而失效**：关于第三方重打包授权可转授的限定仍然有效；见「决策」第 6 条。
- **门禁 3 的取证方法须同步重定义**，见「决策」第 4 条；旧的子包枚举式判据作废，后续任何 side 边界取证须改用常量池符号引用这一机械判据。

## 未包含的决策

- eliotlash 源码与授权的最终处置方式（自行重新实现、寻求上游授权、接受二进制依赖等）——待用户裁决，本 ADR 不作裁定。**原结论已于 2026-08-11 被 `L0-n1` 在用户委托下 supersede 而失效**：见「决策」第 6 条。
- 是否建立 `THIRD-PARTY-NOTICES` 文件以承载上文列出的版权与致谢标头——用户尚未裁决，本 ADR 不建立该文件。
- 嵌入落地后是否从测试实例移除外部 `Lib-Geckolib-forge-1.12.2-3.0.31.jar`——该取证由 L0 在客户端阶段执行，本 ADR 不裁定，也不改动 `FloatingPod-test/**`。
- `com.wi.floatingpod.vendored.**` 内部的具体分包结构（例如是否按 side 进一步细分）——留给实际嵌入线路（`FP-vendor-geckolib`）在实作时决定，本 ADR 只固定顶层命名前缀与门禁 3 的判据形式。
