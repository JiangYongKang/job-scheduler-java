# job-scheduler-java

本地批处理任务编排与资源配额调度器。以进程内状态模拟集群资源与任务依赖，
保证并发提交、依赖失败与配额受限场景下调度结论一致且可解释。

## 调度模型

```
提交(submit/submitBatch)
   │  依赖校验：重复定义 → 未知依赖 → 成环（任一非法整批拒绝）
   ▼
PENDING ── 上游全部 SUCCEEDED ──▶ QUEUED ── 配额获取成功 ──▶ RUNNING ──▶ SUCCEEDED
   │                                │  ▲                       │
   │ 上游 FAILED/BLOCKED            │  └──── 失败/超时且 ──────┘
   │ 或 CANCELLED                   │       还有剩余重试
   ▼                                │ （释放配额后重新排队）
BLOCKED                            ▼
                        拒绝型队列配额不足 → FAILED(QUOTA_REJECTED)
```

- **编排**：后台调度线程周期性扫描任务记录。`PENDING` 任务在上游全部成功后进入
  `QUEUED`；上游失败/取消则进入 `BLOCKED` 终态并沿依赖链向下游传播。
- **执行**：`QUEUED` 任务按队列分组、按有效优先级排序后获取配额并派发到工作线程池；
  执行结果统一在调度器锁上汇报，与超时/取消串行化，杜绝竞争。
- **超时**：`RUNNING` 超过 `timeoutMillis` 即中断执行线程、释放配额并转入重试或终态。
- **取消**：任意非终态任务可取消；取消幂等，重复取消不产生额外效果。

## 队列暂停/恢复

- `pauseQueue(queue)` 后不再向该队列**派发**新任务：排队任务停留 `QUEUED`，
  暂停前已进入 `QUEUED` 的任务顺序被保留；已运行任务不受影响，按原规则自然结束、
  失败重试或超时（重试后重新排队但仍被暂停压住）。
- 暂停期间提交、取消、依赖判定、超时、重试的语义与正常情况完全一致：
  提交正常受理（只排队不派发）、取消正常生效、拒绝型队列**不会**因暂停而拒绝任务。
- `resumeQueue(queue)` 后被压住的排队任务按原顺序继续参与调度。
- 暂停/恢复均幂等；整个过程中同一任务不会被重复派发，配额不会重复占用或漏回收。

## 优先级与等待老化

- 提交时可携带 `priority`（默认 0，数值越大越优先），只影响**同队列内的排队顺序**。
- 同队列内按**有效优先级降序 + 等待时间升序**派发；有效优先级 =
  基础优先级 + 老化加成。任务在 `QUEUED` 每等待满一个老化间隔
  （`setAgingIntervalMillis`，默认 1000ms，`<=0` 关闭）加成 +1，
  低优先级任务等待够久后必然被提升，不会被无限期压住。
- 优先级不改变已运行任务（不抢占），也不突破队列并发/资源上限；
  拒绝型队列配额满时高优先级任务同样进入 `FAILED/QUOTA_REJECTED`。
- 排队型队列采用**队头阻塞**：队头任务配额不足时本轮不再考察同队列后续任务，
  以严格保证顺序语义。

## 优雅停止/排空

- `shutdownGracefully(gracePeriodMillis)`：
  1. 立即停止接受新任务（之后 `submit/submitBatch` 返回拒绝，原因
     `SCHEDULER_CLOSED`，不会静默丢弃）；
  2. 停止派发新尝试（含重试），运行中任务在宽限期内按原规则自然收尾；
  3. 宽限期到点仍 `RUNNING` 的任务按超时逻辑收尾：中断执行线程、释放配额、
     进入 `FAILED/TIMEOUT`；仍处 `PENDING/QUEUED` 的任务进入
     `CANCELLED/SHUTDOWN_DRAINED`；
  4. 返回时保证所有任务处于终态、所有已占用配额恰好释放一次，
     每个任务的结局（成功 `SUCCEEDED` / 失败 `FAILED` / 取消 `CANCELLED` /
     阻塞 `BLOCKED`）均可通过快照区分。
- 排空期间取消、依赖传播等语义保持正常：用户取消仍为 `CANCELLED/CANCELLED_BY_USER`，
  下游仍可能因依赖取消而 `BLOCKED/DEPENDENCY_CANCELLED`。
- `shutdown()` 为立即关闭（中断调度循环与工作线程池），同样关闭提交入口。

## 兼容范围

- 新能力均为**增量语义**，不改变既有结论：依赖校验/传播、配额不超发与恰好释放一次、
  重试次数口径（`1 + maxRetries`）、超时判定、取消幂等、失败原因互斥可区分，
  在暂停/恢复、优先级排队与优雅停止下全部保持。
- `TaskDefinition` 旧构造/工厂方法（不含 `priority`）继续可用，默认优先级 0；
  不配置老化间隔时默认 1000ms，对同优先级任务等价于按排队时间先到先得。
- 暂停只作用于派发环节：依赖判定、超时检测、取消、结果汇报照常运转。
- 优雅停止只作用于“是否接受/派发新工作”：已进入运行态的尝试在宽限期内
  按原有超时/重试/取消规则收尾，终态原因体系不变。

## 任务状态与失败原因

| 状态 | 含义 | 是否终态 |
|---|---|---|
| PENDING | 等待上游依赖 | 否 |
| QUEUED | 依赖就绪，等待队列配额 | 否 |
| RUNNING | 已占用配额，执行中 | 否 |
| SUCCEEDED | 执行成功 | 是 |
| FAILED | 重试耗尽 / 超时 / 配额拒绝 | 是 |
| CANCELLED | 用户取消 | 是 |
| BLOCKED | 上游失败或取消，永不调度 | 是 |

`FailureReason` 取值互斥、可区分，覆盖提交拒绝与运行期终态两类：

- 提交拒绝：`DUPLICATE_TASK`、`UNKNOWN_DEPENDENCY`、`DEPENDENCY_CYCLE`、
  `SCHEDULER_CLOSED`（调度器已停止接收新任务）
- 终态原因：`QUOTA_REJECTED`（配额拒绝）、`DEPENDENCY_FAILED`、`DEPENDENCY_CANCELLED`
  （依赖失败/取消）、`RETRY_EXHAUSTED`（重试耗尽）、`TIMEOUT`（超时）、
  `CANCELLED_BY_USER`（用户取消）、`SHUTDOWN_DRAINED`（优雅停止排空取消）

## 配额语义

- 配额按**队列**维度配置：`maxConcurrent`（并发数）与 `maxResource`（资源单位数），
  任务以 `cost` 占用资源单位。
- 每个队列可选两种策略：
  - **排队**（`rejectWhenFull=false`）：配额不足时任务停留 `QUEUED`，配额释放后自动调度；
  - **拒绝**（`rejectWhenFull=true`）：配额不足时任务直接终态 `FAILED/QUOTA_REJECTED`。
- `tryAcquire` 的检查与占用在队列级锁内原子完成，并发提交不会超发；
  每次成功获取的配额在尝试结束（成功/失败/重试/超时/取消）时**恰好释放一次**，
  由任务记录上的 `quotaHeld` 标志保证，迟到的执行结果不会重复释放。

## 重试与超时语义

- `maxRetries` 为首次执行**之外**的重试次数，总尝试次数上限 = `1 + maxRetries`。
- 执行体抛异常视为失败：有剩余重试则释放配额、回到 `QUEUED` 重新排队；
  耗尽后进入 `FAILED/RETRY_EXHAUSTED`。
- `timeoutMillis` 为单次尝试的超时时间（`<=0` 表示不限制）：超时即中断执行线程，
  有剩余重试则重新排队，耗尽后进入 `FAILED/TIMEOUT`。
- 重复提交同一 `taskId` 被拒绝（`DUPLICATE_TASK`）；取消、重试、超时中断均幂等，
  不产生重复的对外可见效果（同一任务每次尝试只执行一次 handler）。

## 代码结构

```
scheduler/
├── model/      TaskDefinition(含 priority) / TaskStatus / FailureReason / TaskSnapshot / TaskHandler
├── dag/        DependencyValidator（重复、未知依赖、成环三色 DFS 检测）
├── quota/      QuotaManager（队列级原子配额）/ QueueUsage
├── core/       LocalJobScheduler（编排 + 配额 + 重试 + 超时 + 暂停/恢复
│                + 优先级/老化 + 优雅停止）/ TaskRecord / SubmitResult
└── SchedulerConfig（Spring 装配，默认队列 default: 并发4/资源8/排队）
```

## 本地验证

```bash
# 编译
mvn compile

# 全部测试（31 个用例）
mvn test

# 按场景运行
mvn test -Dtest=DependencyValidationTest   # 依赖校验：重复/未知/成环
mvn test -Dtest=QuotaBoundaryTest          # 配额边界：排队 vs 拒绝、资源回收
mvn test -Dtest=DependencyFailureTest      # 依赖失败/取消传播
mvn test -Dtest=RetryAndTimeoutTest        # 重试耗尽与超时
mvn test -Dtest=ConcurrentSubmissionTest   # 并发提交、取消幂等、不超发
mvn test -Dtest=QueuePauseResumeTest       # 暂停/恢复：保序、暂停期并发提交、语义一致
mvn test -Dtest=PrioritySchedulingTest     # 优先级排序、等待老化提升、不突破配额
mvn test -Dtest=GracefulShutdownTest       # 优雅停止：超时/取消/重试交织、拒绝新任务
```

每个测试用例的日志都会打印**输入**（任务拓扑、配额配置、并发参数）与
**判定依据**（期望的状态流转与失败原因），随后打印实际结果，便于核对调度结论。
调度器自身也输出状态迁移日志（受理/拒绝、配额获取/释放、重试、阻塞、终态原因）。
