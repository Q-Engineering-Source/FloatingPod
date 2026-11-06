# ADR-0002：权威、持久化与跨服接管

- 状态：已接受
- 日期：2026-08-06
- 当前后端合同修订：2026-10-06（用户要求服务器兼容优先、正式 MySQL-only 与必需 SQL-JDBC 前置；本轮仅单 RBSS，具体方法见修复计划 Rev5）。
- 实现边界：当前生产 settings/factory 仍连接 SQLite，属于待修偏差；已开始MySQL源码并在SEC03独立测试schema建立候选objects，仍是TEST_WIP_BOOTSTRAPPING，无import/生产READY。方法/source APPROVE不代表正式runtime已转换或MySQL兼容验收；§8–11保留未来完整跨服设计，不纳入本轮已实现能力。
- 当前权限P2：`UNRESOLVED`。runtime/importer无TRIGGER时不能用不可见metadata认证guards；v3候选用固定schema无参数只读DEFINER procedure、runtime仅EXECUTE，每boot fresh实际audit。v3已有独立METHOD ONLY APPROVE，但actual9.7.2权限/缺guard正负证明仍未闭合，旧READY或source APPROVE不能放行，不默授runtime DDL。

## 背景

FloatingPod 需要同时覆盖单人、独立专用服务器和 BungeeCord 多专服网络。辅助机具有所有权、真实 `ItemStack` 模块、可执行脚本版本和活动实体，不能接受数据库失败或跨服竞态导致的复制、丢失或 active-active 写入。

高频移动与行为状态若逐 tick 写入数据库，会产生不可接受的存储压力；但把全部状态只留在内存中，又无法可靠处理崩溃恢复、转让和跨服接管。因此必须明确运行时权威、恢复权威和同步提交边界。

## 决策

### 2026-10-29 用户确认的恢复与收回修订

本节覆盖旧文档中的 removal-before-commit 和“先证明全部旧壳已物理清零才准入”的要求；确认来源为协调记录顶部的用户决定（用户原话“开始，该移除的旧实现就移除”）。这是当前产品合同，实现与验收状态另记工单，不能把本节视为已实现。

- 收回顺序固定为：服务端占用该 Pod 的操作资格、冻结工作并保护实体 → 采集完整稳定快照 → 同一数据库事务提交完整必要状态与已收回业务终态并确认 → 移除实体、释放 ticket。数据库结果 UNKNOWN 时保留原 operation 和冻结责任，不报告成功、不开放再次唤出、不盲重发。
- 数据库是活动 Pod 唯一恢复来源；服务端依据已确认业务状态重建实体和 ticket。普通 Entity 世界记录不得自行恢复活动资格。必要模块、物品、脚本及外部 Forge capability 状态的采集、持久化和恢复必须闭合后才能停用它们原有的保存路径。
- 异常旧壳可以作为原地悬停、不可工作、不可交互的诊断对象保留；不能取得业务资格或新增强加载 ticket。诊断展示与活动 Pod 身份分开，不能因旧壳存在阻断所有健康 Pod。用户后续提供 Gecko 故障模型和周边悬浮信息，本轮不增加粒子或闪烁要求。
- 可封闭的单 Pod 故障局部隔离；schema、authority/fencing、共享资源或已发生世界写入的影响无法封闭时，按实际影响范围阻断，不能跳过坏行拼接 Full26 或把 UNKNOWN 当不存在。
- 删除只为纯清壳而存在的全域扫描、absence、物理擦除证明和未来生产链。已有或可能执行过的 PREPARE、partial、CURRENT_HOLD 及真实世界效果仍须识别和处置；旧 wire/receipt 不改义、不伪造 ACK。仅保留这些事实所需的兼容路径，不以形式兼容保留全部废弃生产链。
- 优先复用现有成熟备份；备份不是第二自动恢复权威，成熟度与恢复入口须有实际证据。

1. 服务端是业务权威。节点健康运行期间，服务端内存是实时运行权威；所选数据库是崩溃恢复权威，并在 MySQL 跨服模式下承担节点协调事实来源。
2. 非关键高频状态以内存更新并标脏，每个辅助机使用独立计时器，默认每 30 秒持久化一次。进程崩溃时允许这些状态最多回退一个成功持久化周期。
3. 辅助机或模块配置、绑定、转让、权益、模块、物品、脚本版本、释放、召回和跨服接管属于关键状态，必须同步提交后才报告成功。正常停服执行最终全量刷盘。
4. 存储通过普通 Scala/Java 接口与 JDBC 实现，不引入 Spring Boot。正式运行强制依赖独立前置 mod `sqljdbcdriverlib` exact `0.2.0-rc.1`，使用其公开 API，不嵌入或 shade JDBC driver；FloatingPod 领域层仍不依赖具体 JDBC backend。
5. 正式运行后端固定 MySQL-only，config 不再提供 SQLite 选择、默认或 fallback；禁止双写、自动降级和运行时切换。SQLite 仅作为明确授权的离线迁移来源及历史测试参考，不构成运行时后端。
6. 本轮实现范围仅限单 RBSS 的 MySQL 持久化，使用独立 FP schema 与专用最小权限账号；不复用 SEC 正式账号 Authority schema/账户。未来完整跨服接管仍须共享指定 MySQL 的 authority/epoch/version/operation 协调与独立验收，不能因连接同一数据库就视为已启用跨服。
7. 本轮实际接线与验收目标为 SEC 只读回执的 MySQL `9.7.2` / InnoDB，替代旧 MySQL 8.x 目标，不由此宣称兼容验收已通过。数据库地址、端口、schema、连接参数与凭据/trust材料引用由服务端私有 config 管理；VERIFY_IDENTITY 固定，TLS身份、有效grants及持久性配置须实查。凭据不能发送至客户端或 BungeeCord，也不能写入 NBT 或日志；失败不能回退SQLite。
8. 跨服记录携带 authority 逻辑节点 ID、服务器 UUID、epoch、状态版本和幂等 operation ID。端口只作连接元数据，不作为持久节点身份。
9. 跨服 handoff 严格遵循 `PREPARE -> CLAIM -> ACK`。源服先停止行为、收回并提交；目标服通过数据库比较并交换取得 authority 后才能生成实体。禁止 active-active。
10. handoff 准备租约默认 60 秒，中继消息至少重试三次。超时、断线或 ACK 丢失后源服保持已收回，不自动重新激活；管理员通过 reconciliation 命令处理。
11. BungeeCord 插件只负责消息中继。数据库中的 authority、epoch、版本号和 operation ID 是接管事实来源，中继消息必须按这些字段幂等处理。
12. 正式取消“每辅助机一个 `.dat` 文件”的运行时方案。管理员可在运行时显式导入或导出带 schema、内容哈希和版本的迁移包，但迁移包不参与运行时写入。
13. 普通迁移包的逻辑导入先完整校验再原子提交；UUID 冲突默认拒绝，显式 replace 或迁移操作才允许覆盖。逻辑导入成功后辅助机保持收回状态，不自动生成实体。SQLite→MySQL全数据库的显式离线迁移另按已审v2：仅新空target BOOTSTRAPPING导入六表原终态与同tx receipt，完整图验证、importer收权/静止后装全部guards，再seal READY与授runtime；保留原UUID/pose/format1 binding及全部历史字段，不用recalled投影改写恢复权威，不自动生成实体或授world启动资格。
14. 后端固定MySQL；数据库连接、schema与节点身份变更要求停服重启。持久化间隔和活动上限等允许运行时修改的服务端 config，必须经过校验后生效；若变更同时修改了辅助机持久状态，则立即持久化相关辅助机。

## 配置身份的版本化合同（2026-10-25 窄同步）

本节承接已采纳的 [MySQL 使用计划 §3.1](../mysql-usage-plan-2026-10-11.md#31-新隔离环境的配置身份解环2026-10-23已接受-method实施中) 及其固定 E7C0911A 方法附录 §8.1–8.4，不新增部署策略、预算或旧库迁移授权。

- 配置语义摘要 C 与完整来源摘要 A 分开：C 从 P/V2 的部署语义生成，不包含 B/R/K 的回向文件 pins；完整 A 仍承诺全部实际文件 pins 与版本选择，原文件批准、来源和权限校验不得省略。P 仅为离线构造/结构 DATA，不能签发运行资格；runtime 从实际 metadata、已批准 K 与实际 policy 重建语义。
- metadata/K 显式选择 `FP_MYSQL_FORMAT3_MANIFEST_CONFIGURATION_BE_V2`，K format 为 `FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V2`，P format 为 `FP_MYSQL_FORMAT3_CONFIGURATION_RECIPE_V2`。闭键、canonical 编码、具体排除字段、hash framing 和原预算严格采用上述固定附录 §8.1–8.2；不自动升级 legacy、补缺省 selector 或把缺失 pins 填零。
- 新路径的 `ManifestSha256` 只通过校验完成的 typed semantic 输入及受限 factory 生成；结构解析结果、proposal 或任意外传摘要不能取得相同资格。数据库 manifest/audit、runtime input 和新 context/scope 使用 C；Ordinary/source/native/control 对完整 configuration 的原比较继续使用 A。context/witness/effect 的版本分派按 [MySQL 使用计划 §3.2 的正式 wire 纠正](../mysql-usage-plan-2026-10-11.md#32-e7-83-wire-身份正式窄纠正2026-10-25-main-核定) 及 E7 未受纠正部分；传统 witness 独立新 domain 后新增 BE32(3)，新 C Census V4 明确区分 flat(4,1)/Recipe(4,2)，mode projection 新 V3/encoding17 保留 V2 historical RAW 语义及旧 encoding3/16 各自原义。B1 外 V4、内 V2 规则仍按附录 §8.4，不能按同名字段全局替换或为旧 payload 授新资格。此为 Main 经独立只读设计审阅后核定的合同纠正，不自动释放实现或表示测试、运行通过；E7 原快照不改。
- 本合同采纳不表示实现或运行完成。Fresh/Ordinary、converter、B1、context/witness 的实际整合与必要验证仍须逐项闭合；已有配置候选编译/六项测试只构成该候选有限证据，不代表完整 parser/factory 接线、MySQL/服务器验收或 Ready。MySQL-only、现行 SourceLimitProfile 明确字节预算、原动作前收费/拒绝约束及失败责任保持不变。

## 影响

- 运行时不会因位置、导航或动画等高频变化持续刷新数据库，但崩溃后这些非关键状态可能有限回退。
- 所有权、模块与物品路径的延迟将包含数据库事务时间；这是避免复制和不一致所需的成本。
- 单服也使用MySQL，正式运行不依赖本地SQLite；显式离线迁移和历史测试不会获得runtime写入权，原SQLite/世界来源及历史hash保持可追溯。
- 单RBSS的MySQL也属于关键基础设施。关键提交不可用或结果未知时保留可诊断/pending责任，不早报成功、不盲重试或清理，不能回退本地SQLite；未来完整跨服还需单独实现和验收。
- handoff 需要持久化状态机、幂等收件记录和管理员 reconciliation 工具，并需要对每个崩溃点执行故障注入测试。
- 运行时导入和显式替换必须与活动实体、authority 和版本校验互斥，避免在导入期间产生第二个权威实例。

## 已否决方案

- **每辅助机一个 `.dat` 文件作为运行时存储**：已取消；只保留显式迁移包。
- **SQLite 正式 runtime/default/fallback、跨服镜像或每服数据库 Saga**：不提供；SQLite仅显式离线迁移/历史测试，未来跨服仍依赖共享MySQL协调合同。
- **SQLite 与 MySQL 双写或自动故障切换**：会产生冲突权威和不可验证的恢复顺序，禁止使用。
- **以端口号标识服务器**：端口会迁移、复用或被代理映射，不能提供稳定身份。
- **active-active 写入**：无法保证实体、所有权和真实物品唯一性，禁止使用。
- **Spring Boot DAO 层**：对 1.12.2 模组运行环境体积和复杂度过高，普通接口与 JDBC 足够表达所需合同。
