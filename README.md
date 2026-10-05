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
- **执行**：`QUEUED` 任务按队列获取配额后派发到工作线程池执行；执行结果统一在
  调度器锁上汇报，与超时/取消串行化，杜绝竞争。
- **超时**：`RUNNING` 超过 `timeoutMillis` 即中断执行线程、释放配额并转入重试或终态。
- **取消**：任意非终态任务可取消；取消幂等，重复取消不产生额外效果。

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
  `SCHEDULER_STOPPED`（调度器排空/停止后不再受理）
- 终态原因：`QUOTA_REJECTED`（配额拒绝）、`DEPENDENCY_FAILED`、`DEPENDENCY_CANCELLED`
  （依赖失败/取消）、`RETRY_EXHAUSTED`（重试耗尽）、`TIMEOUT`（超时）、
  `CANCELLED_BY_USER`（用户取消）、`SCHEDULER_SHUTDOWN`（优雅停止收尾取消）

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

## 队列暂停/恢复

- `pauseQueue(queue)` 后，调度器不再向该队列**派发**新任务；`resumeQueue(queue)` 后
  被压住的排队任务继续参与调度。两者均幂等，暂停只影响指定队列。
- 暂停期间语义与正常情况一致：
  - 提交/批量提交照常受理（任务停留在 `PENDING`/`QUEUED`）；
  - 正在运行的任务按原规则自然结束或超时，超时与失败的重试照常发生，
    只是重试后回到 `QUEUED` 等待恢复；
  - 取消照常生效（`CANCELLED/CANCELLED_BY_USER`），依赖编排照常推进。
- 同一任务不会被重复派发（每次尝试恰好执行一次 handler），配额在暂停/恢复
  过程中不会重复计扣或漏回收；恢复后同优先级任务保持暂停前的排队顺序（FIFO）。

## 优先级与等待老化

- 提交时可携带 `priority`（`TaskDefinition.of(..., priority, handler)`，旧签名默认 0），
  数值越大越优先；**只影响同队列内的排队顺序**，不影响已运行任务，也不突破
  队列并发与资源限制，拒绝型队列满员时高优先级任务照样被 `QUOTA_REJECTED` 拒绝。
- 调度顺序：等效优先级高者优先；并列时先进入 `QUEUED` 者优先（完全并列保持提交顺序）。
- 老化（anti-starvation）：等效优先级 = `priority + 等待时长 / agingIntervalMillis`
  （构造器可配，默认 1000ms，`<=0` 关闭）。等待越久提升越多，低优先级任务
  不会被持续到来的高优先级任务无限期压住。
- 重试重新排队视为新的排队（等待时长重新计时）。

## 优雅停止/排空

- `shutdownGracefully(graceMillis)`：
  1. 立即进入排空阶段，不再受理新任务——`submit`/`submitBatch` 返回
     `SCHEDULER_STOPPED` 拒绝（带可读说明，不静默丢弃）；
  2. 存量任务在宽限期内按原规则继续调度：排队任务仍会派发、失败/超时仍重试、
     取消仍生效；
  3. 到达截止时间仍未结束的 `RUNNING` 任务按超时逻辑收尾为 `FAILED/TIMEOUT`
     （中断执行线程、恰好释放一次配额、不再重试）；仍处于 `PENDING`/`QUEUED`
     的任务收尾为 `CANCELLED/SCHEDULER_SHUTDOWN`，与用户取消可区分。
- 返回时保证：所有任务均为终态（完成/失败/取消可区分）、配额全部释放、
  调度线程与工作线程池已停止。方法幂等，重复调用直接返回。
- `shutdown()` 为立即硬停止（同样使后续提交被拒绝）；Spring 装配仍使用
  `shutdown` 作为 destroyMethod。

## 与既有能力的兼容范围

- 暂停/恢复、优先级、优雅停止均建立在原有编排、配额、重试、超时规则之上，
  不改变其判定逻辑；`TaskDefinition.of` 旧签名（无 priority）行为与之前完全一致。
- 暂停与排空可叠加：排空期内暂停的队列不会派发，截止时其排队任务按
  `SCHEDULER_SHUTDOWN` 收尾。

## 代码结构

```
scheduler/
├── model/      TaskDefinition / TaskStatus / FailureReason / TaskSnapshot / TaskHandler
├── dag/        DependencyValidator（重复、未知依赖、成环三色 DFS 检测）
├── quota/      QuotaManager（队列级原子配额）/ QueueUsage
├── core/       LocalJobScheduler（编排 + 配额 + 重试 + 超时）/ TaskRecord / SubmitResult
└── SchedulerConfig（Spring 装配，默认队列 default: 并发4/资源8/排队）
```

## 本地验证

```bash
# 编译
mvn compile

# 全部测试（37 个用例）
mvn test

# 按场景运行
mvn test -Dtest=DependencyValidationTest   # 依赖校验：重复/未知/成环
mvn test -Dtest=QuotaBoundaryTest          # 配额边界：排队 vs 拒绝、资源回收
mvn test -Dtest=DependencyFailureTest      # 依赖失败/取消传播
mvn test -Dtest=RetryAndTimeoutTest        # 重试耗尽与超时
mvn test -Dtest=ConcurrentSubmissionTest   # 并发提交、取消幂等、不超发
mvn test -Dtest=QueuePauseResumeTest       # 暂停/恢复：并发提交、超时重试、顺序保持
mvn test -Dtest=PriorityQueueingTest       # 优先级排序、等待老化、满员拒绝、并发上限
mvn test -Dtest=GracefulShutdownTest       # 优雅停止：排空、超时收尾、停止后拒绝
```

每个测试用例的日志都会打印**输入**（任务拓扑、配额配置、并发参数）与
**判定依据**（期望的状态流转与失败原因），随后打印实际结果，便于核对调度结论。
调度器自身也输出状态迁移日志（受理/拒绝、配额获取/释放、重试、阻塞、终态原因）。
