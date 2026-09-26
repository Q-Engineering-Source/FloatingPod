# FloatingPod 项目规划

FloatingPod 是面向 Minecraft 1.12.2、仅支持 Cleanroom 的服务端权威浮游辅助单元 Mod。项目使用满足 Scalar `>=3.8.4` 最低前置要求的 Scala 3 环境；第一版选用官方分发 artifact `3.8.4-1`，其 FML runtime mod version 为 `3.8.4`，通过真实物品模块和经审批的 ZenScript 行为扩展辅助机能力。

## 已确认基线

| 项目 | 决策 |
| --- | --- |
| 显示名称 | `[FP]浮游辅助单元-FloatingPod` |
| Mod ID | `floatingpod` |
| JVM 包名 | `com.wi.floatingpod` |
| 平台 | Minecraft 1.12.2 / Cleanroom Only / Java 25 |
| 构建基线 | Cleanroom `0.6.8-alpha`、Unimined `1.4.27-kappa`、Scalar `>=3.8.4`（最低前置）；第一版分发 artifact `3.8.4-1` / FML runtime mod version `3.8.4` |
| 脚本生态 | 服务端 ZenScript 审批与不可变行为注册表 |
| 权威模型 | 服务端内存实时权威、所选数据库崩溃恢复权威 |
| 存储模式 | SQLite 单服，或共享 MySQL 跨服；二者互斥 |
| 客户端职责 | 配置交互、GeckoLib 模型、动画、粒子和音效 |
| 许可证 | GNU LGPL v3 |
| 项目阶段 | 阶段 1-B 第一版构建 writer 已完成，等待独立 reviewer |

## 文档导航

- [产品范围](00-product-scope.md)
- [技术基线与依赖](01-technical-baseline.md)
- [系统架构](02-architecture.md)
- [ZenScript 脚本系统](03-scripting.md)
- [开发路线图](04-roadmap.md)
- [生命周期与交互](05-lifecycle-and-interactions.md)
- [ADR-0001：平台、语言与基础依赖](adr/0001-platform-and-language.md)
- [ADR-0002：权威、持久化与跨服接管](adr/0002-authority-and-storage.md)
- [ADR-0003：区块加载、活动容量与传送](adr/0003-chunk-loading.md)

## 当前实施入口

第一项实施工作是阶段 1 的工具链与兼容性探针。阶段 1-B 第一版 writer 已完成 Scala 3 / Java 25 互操作与产物边界探针，当前先由独立 reviewer 复验。reviewer 通过后，才可将 remapped JAR 与完整官方 Scalar artifact `3.8.4-1` 安装到新的 Cleanroom 客户端和专用服务端实例，验证 FloatingPod 的真实加载。

规范中的 `>=3.8.4` 只表示最低前置范围。第一版分发 artifact 的完整版本是 `3.8.4-1`，而 Scalar `mcmod.info` / FML runtime mod version 是 `3.8.4`；两者不得混写。`3.8.4-1` 新增 `scala-parser-combinators_3`，但 FloatingPod 第一版没有 parser API 需求，不声明 parser 直接依赖。

在探针通过前，规划中的依赖组合、跨服协议和 Draconic Evolution 护盾联动均属于已确认的设计目标，而不是已经验证的实现能力。
