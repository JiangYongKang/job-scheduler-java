# 本地批处理任务编排与资源配额调度

进程内（in-process）批处理任务调度器，用单状态机 + 工作线程池模拟集群资源与任务依赖，
保证并发提交、依赖失败、配额受限、取消与重试等场景下调度结论一致、可区分、可追溯。

核心实现：`com.github.highcumontoa.jobschedulerjava.scheduler.LocalJobScheduler`
（Spring Boot 下由 `JobSchedulerConfig` 自动注册为 Bean，也可直接 `new` 使用）。

## 1. 调度模型

- **任务（TaskDefinition）**：全局唯一 `id`，归属于一个已注册的**队列**，携带
  依赖集合、资源需求（cpuUnits / memoryBytes）、`maxRetries`、`timeoutMillis`、
  `rejectWhenBusy` 策略。
- **队列（QueueQuota）**：独立维护四维约束
  - `maxConcurrency`：同时 RUNNING 任务数上限；
  - `cpuLimit` / `memoryLimit`：同时 RUNNING 任务的资源总和上限；
  - `maxQueued`：排队容量（`-1` 不限）。
- **依赖**：仅当所有前置任务均为 `SUCCEEDED` 时，任务才参与配额调度。
  依赖在**提交期**校验，批次整体原子（全成功或全部不注册）。
- **并发安全**：全部共享状态由一把 `ReentrantLock` 保护；任务体在锁外的线程池中执行，
  完成时带 `epoch` 轮次校验回锁提交结论，过期回调（取消、重试产生的新轮次）一律忽略，
  因此同一任务同一时刻至多一次执行，重复提交/重复触发不会产生重复的对外可见效果。
- **FIFO 就绪队列**：依赖满足后按提交顺序排队，队首放得下才分配；失败重试也回到队尾，
  不抢占已排队任务的配额。

### 提交期拒绝（任务不进入状态机）

提交失败抛出 `SubmitException`，通过 `SubmitErrorCode` 区分原因：

| 错误码 | 触发条件 |
| --- | --- |
| `MISSING_ID` | id 为空 |
| `DUPLICATE_ID` | 同批次重复 id，或 id 在已提交任务中已存在 |
| `UNKNOWN_DEPENDENCY` | 依赖了不存在的前置任务（批次内/跨批次） |
| `CYCLIC_DEPENDENCY` | 依赖图成环（含任务依赖自身），DFS 三色标记检测 |
| `UNKNOWN_QUEUE` | 目标队列未调用 `registerQueue` 注册 |

## 2. 任务状态流转

```
                         ┌──────────────────────────────────────────┐
                         │                                          │ 失败/超时（仍有额度，回队尾）
  submit ─► PENDING ──► QUEUED ──► RUNNING ─────────────────────► SUCCEEDED（终态）
              │           ▲          │  │  │  │
              │ 依赖满足   │ 配额等待  │  │  │  └─ 失败耗尽 ─► FAILED（终态）
              │           └──────────┘  │  └──── 超时耗尽 ─► TIMED_OUT（终态）
              │ 配额不足(REJECT策略)    ├────── 配额拒绝/排队满 ─► REJECTED（终态）
              ├──────────────────────► REJECTED                    │
              │                        ├────── 用户取消 ─► CANCELLED（终态）
              └── 用户取消 ────────────► CANCELLED（终态）
              │
              └── 任一上游进入非成功终态 ─► SKIPPED（终态）
```

非终态：`PENDING`（等依赖）、`QUEUED`（等配额）、`RUNNING`。
终态：`SUCCEEDED / FAILED / TIMED_OUT / REJECTED / CANCELLED / SKIPPED`。

### 终态原因（FailureReason，可区分、可追溯）

| FailureReason | 含义 |
| --- | --- |
| `NONE` | 非失败结论 |
| `DEPENDENCY_FAILED` | 上游为 FAILED/TIMED_OUT/REJECTED/CANCELLED/SKIPPED，本任务**级联 SKIPPED、从未执行**；`reasonDetail` 记录直接上游 id（传递级联可逐层追溯） |
| `QUOTA_REJECTED` | `rejectWhenBusy=true` 时配额不足，或 FIFO 排队容量已满；记录当时占用与需求 |
| `RETRIES_EXHAUSTED` | 业务失败且 `maxRetries` 已耗尽（末次异常信息保留在 `reasonDetail`） |
| `TIMEOUT` | 超过 `timeoutMillis` 且重试已耗尽，与普通失败耗尽严格区分 |
| `CANCELLED` | 用户取消（等待依赖/排队中立即取消；运行中则发出中断，任务体响应后落 CANCELLED） |

说明：
- 上游一旦进入任何非成功终态，下游立即 `SKIPPED` 且其任务体绝不执行；
  与上游无依赖关系的任务不受影响。
- 对 `PENDING/QUEUED` 任务的取消同步生效；对 `RUNNING` 任务的取消是中断请求，
  任务体需要响应中断（或检查 `TaskContext.remainingMillis()`）才能尽快收尾。
- 对终态任务再次取消/重试是幂等的：终态取消直接无效；
  `retry` 仅对 `FAILED/TIMED_OUT/REJECTED`（以及依赖已恢复成功的 `SKIPPED`）生效，
  每次手动重试增加一次尝试额度并重新参与 FIFO 调度，对 RUNNING 等状态返回 false、无副作用。

## 3. 配额语义

- 每次执行在启动时**原子**占用 `(1 并发, cpuUnits, memoryBytes)`，
  在该次执行结束的**所有路径**（成功、业务失败、超时、取消、线程池拒绝）上回收，
  因此调度失败不会泄漏资源。
- 需求在任一维度放不下即视为配额不足：
  - 任务 `rejectWhenBusy=false`（默认）：进入 `QUEUED` 排队，资源释放后按 FIFO 自动补位；
  - 任务 `rejectWhenBusy=true`：立即 `REJECTED`（`QUOTA_REJECTED`），终态、不排队。
  - 排队容量 `maxQueued` 已满时，即使是排队策略也立即 `REJECTED`。
- **排队与拒绝是两种可区分的结论**：`QUEUED` 为非终态且终将被调度（除非被取消），
  `REJECTED` 为终态且不会自动复活（需显式 `retry`）。

## 4. 重试与超时语义

- 总尝试次数 = `maxRetries + 1`（`maxRetries` 默认 0，即只执行一次）。
- 每次业务失败：仍有额度则任务回到就绪队列**尾部**等待下一轮（`attempt` 仅在真正启动执行时递增，
  排队/等依赖不计入重试）；额度耗尽进入 `FAILED`。
- `timeoutMillis > 0` 时，调度器在每次执行启动时注册一个截止定时器，
  到期后中断工作线程；任务体正常返回但中断/超时已生效时仍判超时（消除中断晚到竞态）。
  仍有额度则重试，耗尽进入 `TIMED_OUT`。`timeoutMillis <= 0` 表示不限制。
- 超时与业务失败是两条独立判定路径：最终原因分别为 `TIMEOUT` 与 `RETRIES_EXHAUSTED`，
  二者再与 `DEPENDENCY_FAILED`、`QUOTA_REJECTED` 互不混淆。
- `snapshot(taskId)` 返回的 `TaskSnapshot` 记录当前状态、失败原因、可读明细、
  当前/最大尝试次数与资源需求，可直接用于断言与审计日志。

## 5. API 概览（`JobScheduler`）

- `registerQueue(QueueQuota)`：注册队列配额；
- `submit(TaskDefinition)` / `submit(Collection<TaskDefinition>)`：单任务/原子批次提交；
- `snapshot(taskId)`：查询不可变快照（不存在返回 null）；
- `cancel(taskId)`：取消（排队中立即生效，运行中发中断）；
- `retry(taskId)`：对失败类终态追加一次尝试并重新调度，幂等；
- `awaitTerminal(taskId, timeoutMillis)`：等待单任务终态；
- `awaitQuiet(timeoutMillis)`：等待全部任务离开 PENDING/QUEUED/RUNNING；
- `shutdown()`：释放工作线程与定时器线程。

## 6. 本地验证方法

```bash
# 全量测试（28 个用例，含 Spring 上下文加载）
mvn test

# 按主题单测
mvn -Dtest=DependencyValidationTest test   # 重复id/未知依赖/成环/未知队列/DAG
mvn -Dtest=QuotaSchedulingTest test        # 并发上限、cpu 边界、排队 vs 拒绝、排队容量、并发提交不超发
mvn -Dtest=RetryTimeoutTest test           # 重试耗尽、超时终态、超时重试、手动重试幂等
mvn -Dtest=DependencyCascadeTest test      # 上游失败/取消/拒绝的级联 SKIPPED
mvn -Dtest=ConcurrencyControlTest test     # 并发重复提交唯一胜出、并发取消不泄漏配额

# 启动应用（调度器作为 Bean 可注入）
mvn spring-boot:run
```

每个单测都在日志中打印 **[输入]**（构造的场景）与 **[判定]**（观测到的状态/原因/资源占用），
surefire 报告位于 `target/surefire-reports/`。

### 最小使用示例

```java
JobScheduler scheduler = new LocalJobScheduler();
scheduler.registerQueue(QueueQuota.of("etl", 2, 4, 1024 * 1024 * 1024L, 100));

scheduler.submit(TaskDefinition.builder("extract").queue("etl")
        .timeoutMillis(5000).maxRetries(2)
        .body(ctx -> doExtract()).build());
scheduler.submit(TaskDefinition.builder("load").queue("etl")
        .dependsOn("extract").rejectWhenBusy(true)
        .body(ctx -> doLoad()).build());

TaskSnapshot result = scheduler.awaitTerminal("load", 30000);
// result.getStatus() / getFailureReason() / getReasonDetail() 给出可解释的调度结论
```
