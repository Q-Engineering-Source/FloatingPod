# 崩溃点测试方法

## 目的

本目录的测试通过可编程的 `CrashPointHarness` 模拟进程在关键边界突然丢失。目标是验证 Stage 2 的 SQLite 恢复模型不会产生重复辅助机实体或同一辅助机的重复所有权；位置等非关键状态最多回退一个 30 秒持久化周期。模块和物品模型尚未进入 Stage 2，因此物品复制不在自动化范围内。

## 当前注入点

| ID | 产品边界 | 恢复含义 |
| --- | --- | --- |
| `after-db-commit-spawn` | spawn 的关键数据库提交完成、内存映射发布前 | 新进程只信任已提交活动状态。 |
| `after-db-commit-recall` | recall 的关键数据库提交完成、映射清理前 | 新进程只信任已提交非活动状态。 |
| `before-entity-spawn` | 已取得并初始化 ticket、创建实体前 | 重启回收未配对 ticket，不生成实体。 |
| `after-entity-spawn` | 实体已创建、关键数据库提交前 | 重启丢弃旧世界实体，只保留未提交的持久化状态。 |
| `before-ticket-request` | 调用 gateway 申请原始 Forge ticket 前 | 不会出现原始 ticket。 |
| `after-ticket-request` | 获得原始 ticket、写入 UUID ModData 前 | 有序恢复选择会丢弃没有 UUID 的 ticket，并由 Forge 回调处置。 |
| `before-periodic-flush` | 调用脏状态刷盘前 | 重启读到上一次成功刷盘的位置。 |
| `after-periodic-flush` | 脏状态刷盘成功后 | 重启读到已经成功刷盘的位置。 |

## 确定性恢复模型

测试使用固定 UUID、时间戳和文件型临时 SQLite 数据库。每个重启都会关闭旧 repository，创建新的 repository、`ActiveFloatingPodManager`、`ChunkLoadingManager`、typed test world、gateway 与必要的 `PeriodicFlushService`。typed test world 记录旧世界实体 ID，新的 world 从空实体集合开始；测试捕获两个 world 的实际 ID，避免把 `List.empty` 当作恢复证据。

ticket 重启模型按 Forge `OrderedLoadingCallback` 的顺序执行：先调用生产 `selectRecoverableTickets`，模型仅保留其返回的原始逻辑 ticket，再调用生产 `recoverAcceptedTickets`。因此没有 UUID ModData 的 ticket 不会进入第二回调或新的 manager 映射，也不会继续留在模型的 Forge 全局登记中。

## 运行方法

```powershell
$env:GRADLE_USER_HOME='C:/GradleCaches/FloatingPod'
.\gradlew.bat --no-daemon --console=plain test --tests com.wi.floatingpod.crashpoint.CrashPointHarnessTest --tests com.wi.floatingpod.crashpoint.StateSnapshotTest
```

```powershell
$env:GRADLE_USER_HOME='C:/GradleCaches/FloatingPod'
.\gradlew.bat --no-daemon --console=plain clean test
```

## 生产行为与扩展

生产默认禁用 harness。禁用时 `checkCrashPoint` 在读取 enabled 标志后立即返回，不查询 registry、不记录日志，也不改变状态。新增崩溃点时，必须：

1. 在产品代码的明确语义边界使用现有常量或新增稳定 ID。
2. 在本表记录边界和恢复含义。
3. 添加直接行为测试，使用关闭旧资源并新建运行时对象的确定性重启模型。
4. 在 `StateSnapshot.diff` 中断言没有重复实体、同一辅助机的多个所有者或重复持久化 UUID。

## Stage 2 与人工边界

Stage 2 自动化覆盖八个注入边界、SQLite 已提交状态、ticket 有序回调处置、typed world 的实体 ID 隔离，以及 30 秒非关键状态回退界限。真实 Cleanroom/Forge 的 ticket 回调时序、原始 Forge ticket 的进程级释放、Minecraft world 序列化重建、JDBC native 代码执行期间的操作系统级终止，仍需 L0-n1 的人工或集成验证。Stage 3 出现模块与物品模型后，再补充物品复制恢复测试。
