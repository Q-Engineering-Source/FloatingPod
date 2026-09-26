# 开发路线图

## 阶段 0：规划基线

状态：已完成决策确认，待文档审查。

- 固定 Cleanroom Only、Java 25、Scalar `>=3.8.4` 最低前置范围、项目身份和依赖图；第一版分发 artifact 选用 `3.8.4-1`，其 FML runtime mod version 为 `3.8.4`。
- 固定辅助机生命周期、模块物品、脚本审批、权威存储和区块加载合同。
- 建立独立 Git 仓库、项目文档和项目级 Pensieve 数据。

完成标准：规划文档不存在阻塞脚手架创建的 `TBD`，跨文档术语和数值一致。

## 阶段 1：工具链与兼容性探针

- 以 `CleanroomModTemplate main` 建立 Gradle 工程，并将 Gradle 缓存置于 `C:/GradleCaches/FloatingPod`。
- 第一版已由 writer 合并 Scala 3 配置，使用 `scala3-library_3:3.8.4` 编译，并以完整官方 Scalar artifact `3.8.4-1` 作为实例分发前置（FML runtime mod version `3.8.4`）。独立 reviewer 必须复验 Java 25、Scala/Java 互操作和发布产物边界；不得把 `scala-parser-combinators_3` 声明为直接依赖或重复打包。
- 在新的 Cleanroom 测试实例中验证客户端、集成服务端和专用服务端启动。
- 验证官方 GeckoLib、CraftTweaker、ZenUtils、Baubles、Draconic 依赖链，以及 Lumenized 存在/缺失两条路径。**GeckoLib 一项已于 2026-08-10 失效**：嵌入后实例内不再安装 GeckoLib，本条不再要求 GeckoLib 参与依赖链验证（决策见 `doc/adr/0004-embed-geckolib.md`）；CraftTweaker、ZenUtils、Baubles、Draconic 四项仍须验证。

完成标准：干净构建可重复，客户端和专用服务端均能加载探针 Mod；产物不重复打包 Scalar。**「或 GeckoLib」一项已于 2026-08-10 失效**：GeckoLib 及其 eliotlash 依赖已裁定以源码形式嵌入本仓库并改名到 `com.wi.floatingpod.vendored.**`（决策见 `doc/adr/0004-embed-geckolib.md`），故「不重复打包」这一表述不再适用于 GeckoLib；Scalar 一项不受影响，产物仍不得重复打包 Scalar。

## 阶段 2：权威领域与本地持久化

- 定义辅助机 UUID、`UNBOUND`、绑定、所有权、权益和生命周期状态机。
- 建立普通 Scala/Java 存储接口、内存运行状态、30 秒独立刷盘和关键状态同步提交。
- 实现 SQLite 单人/独立单服 backend、schema 迁移和运行时导入/导出事务；SQLite 驱动由独立前置模组提供并另建仓库构建，FloatingPod 不打包该 JDBC 驱动。
- 建立服务端权威请求验证、版本检查和最小客户端状态同步。

完成标准：崩溃点测试不产生重复实体、所有权或物品；非关键状态最多回退一个持久化周期。

## 阶段 3：首个纵向原型

- 实现包装、拆包、激活密钥、终端、释放和召回。
- 实现通用模块槽位、真实 `ItemStack` 安装/拆卸和“跟随策略数据盘”。
- 实现配置 GUI、服务端参数校验、GeckoLib 模型与动画。
- 实现跟随、同维度末影传送效果、传送门路径和失败回退。
- 实现每辅助机 Forge ticket、5 x 5 默认强加载、全服 256 活动门禁和终端容量显示。

完成标准：两名玩家不能交叉控制；客户端篡改不能复制模块或绕过容量；ticket 失败会可靠收回。

## 阶段 4：MySQL 与跨服接管

- 实现与 SQLite 互斥的 MySQL 8.x/InnoDB backend。
- 实现固定逻辑节点 ID、服务器 UUID、epoch、state version 和 operation ID。
- 实现 `PREPARE -> CLAIM -> ACK`、60 秒租约、至少三次中继重试和管理员 reconciliation。
- 实现 BungeeCord 代理中继，但不在代理保存或裁决辅助机业务状态。

完成标准：断线、重复消息、旧 epoch、节点重启和各事务崩溃点均不能产生 active-active 实体。

## 阶段 5：脚本审批与行为注册表

- 实现玩家提交、管理员审批、精确内容哈希、不可变版本和撤销。
- 实现专用 loader、解析/类型/领域约束检查、暂存 builder 和不可变注册表原子替换。
- 实现 LuckPerms 权限源与内置后端，以及空数据盘生成已批准版本的流程。

完成标准：失败候选不改变活动快照；撤销立即阻止执行；无权限或被篡改的数据盘无法运行。

## 阶段 6：后续行为模块

- 在已验证的模块合同上分别交付战斗、采集和物品存储模块。
- 为行为调度建立 tick 预算、公平策略、错误隔离和诊断指标。
- 单独确定目标选择、领地权限、采集掉落、库存容量和资源平衡规则。

完成标准：各模块具备独立规则测试和多辅助机负载测试，不改变首个纵向原型的权威边界。

## 阶段 7：发布准备

- 完成兼容性矩阵、许可证、发行说明、配置参考、管理员恢复手册和升级说明。
- 执行客户端、专用服务端、多人、传送、死亡、离线、持久化、跨服和性能回归。
- 验证发布产物、依赖声明、数据库凭据脱敏和可重复构建。

完成标准：从干净环境可以复现构建和验收，所有已知限制均有记录。
