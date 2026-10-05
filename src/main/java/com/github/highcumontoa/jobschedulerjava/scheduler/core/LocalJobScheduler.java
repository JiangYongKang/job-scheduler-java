package com.github.highcumontoa.jobschedulerjava.scheduler.core;

import com.github.highcumontoa.jobschedulerjava.scheduler.dag.DependencyValidator;
import com.github.highcumontoa.jobschedulerjava.scheduler.dag.ValidationResult;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;
import com.github.highcumontoa.jobschedulerjava.scheduler.quota.QuotaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 进程内本地批处理任务调度器。
 *
 * 语义：
 * - 提交：整批做重复定义 / 未知依赖 / 成环校验，任一非法整批拒绝，原因互斥可区分。
 * - 编排：上游失败或取消会令下游进入 BLOCKED 终态，原因分别为
 *   DEPENDENCY_FAILED / DEPENDENCY_CANCELLED 且可沿依赖链追溯。
 * - 配额：按队列约束并发数与资源量；队列策略为“拒绝”时配额不足任务进入
 *   FAILED/QUOTA_REJECTED 终态，否则停留在 QUEUED 排队，二者可区分。
 * - 重试/超时：maxRetries 为首次执行之外的重试次数；超时按 timeoutMillis 判定；
 *   重试耗尽后按 RETRY_EXHAUSTED 或 TIMEOUT 进入终态。
 * - 暂停/恢复：pauseQueue 后不再向该队列派发新任务（排队任务保序等待），
 *   运行中任务按原规则收尾；resumeQueue 后按原顺序继续调度。
 * - 优先级/老化：同队列内按有效优先级（基础 priority + 等待老化加成）降序、
 *   等待时间升序派发；不抢占运行中任务，不突破配额上限。
 * - 优雅停止：shutdownGracefully 停止接受新任务与派发新尝试，运行中任务在
 *   宽限期内自然收尾，超期按超时终结（FAILED/TIMEOUT），排队任务以
 *   CANCELLED/SHUTDOWN_DRAINED 终结；返回时无中间态任务、无配额泄漏。
 * - 任何路径下成功占用的配额在尝试结束（成功/失败/重试/超时/取消）时恰好释放一次。
 */
public class LocalJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(LocalJobScheduler.class);
    private static final long TICK_MILLIS = 5L;

    private final QuotaManager quotaManager;
    private final DependencyValidator validator;
    private final ExecutorService workers;
    private final Map<String, TaskRecord> records = new LinkedHashMap<>();
    /** 已暂停的队列：暂停期间不向这些队列派发新任务，运行中任务不受影响 */
    private final Set<String> pausedQueues = new HashSet<>();

    private volatile boolean running = false;
    private Thread schedulerThread;
    /** 是否还接受新任务；优雅停止或关闭后变为 false */
    private volatile boolean accepting = true;
    /** 是否处于优雅停止排空阶段：不再派发新任务，等待运行中任务收尾 */
    private volatile boolean draining = false;
    /** 排队老化间隔：每等待满一个间隔，有效优先级 +1；<=0 关闭老化 */
    private volatile long agingIntervalMillis = 1_000L;

    public LocalJobScheduler(QuotaManager quotaManager, DependencyValidator validator) {
        this(quotaManager, validator,
                Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "scheduler-worker");
                    t.setDaemon(true);
                    return t;
                }));
    }

    LocalJobScheduler(QuotaManager quotaManager, DependencyValidator validator,
                      ExecutorService workers) {
        this.quotaManager = quotaManager;
        this.validator = validator;
        this.workers = workers;
    }

    // ------------------------------------------------------------- 提交

    public synchronized SubmitResult submit(TaskDefinition definition) {
        if (!accepting) {
            log.info("submit rejected: task={} reason={} (scheduler not accepting)",
                    definition.taskId(), FailureReason.SCHEDULER_CLOSED);
            return SubmitResult.rejected(definition.taskId(), FailureReason.SCHEDULER_CLOSED,
                    "scheduler is shutting down or already stopped");
        }
        ValidationResult result = validator.validate(List.of(definition), records.keySet());
        if (!result.ok()) {
            log.info("submit rejected: task={} reason={} detail={}",
                    definition.taskId(), result.reason(), result.detail());
            return SubmitResult.rejected(definition.taskId(), result.reason(), result.detail());
        }
        register(definition);
        log.info("submit accepted: task={} queue={} deps={} cost={} maxRetries={} timeoutMs={}",
                definition.taskId(), definition.queue(), definition.dependencies(),
                definition.cost(), definition.maxRetries(), definition.timeoutMillis());
        notifyAll();
        return SubmitResult.accepted(definition.taskId());
    }

    public synchronized List<SubmitResult> submitBatch(List<TaskDefinition> batch) {
        if (!accepting) {
            log.info("batch submit rejected (scheduler not accepting): size={}", batch.size());
            return batch.stream()
                    .map(d -> SubmitResult.rejected(d.taskId(), FailureReason.SCHEDULER_CLOSED,
                            "scheduler is shutting down or already stopped"))
                    .toList();
        }
        ValidationResult result = validator.validate(batch, records.keySet());
        if (!result.ok()) {
            log.info("batch submit rejected (whole batch): size={} reason={} detail={}",
                    batch.size(), result.reason(), result.detail());
            return batch.stream()
                    .map(d -> SubmitResult.rejected(d.taskId(), result.reason(), result.detail()))
                    .toList();
        }
        List<SubmitResult> results = new ArrayList<>();
        for (TaskDefinition def : batch) {
            register(def);
            results.add(SubmitResult.accepted(def.taskId()));
        }
        log.info("batch submit accepted: size={} tasks={}", batch.size(),
                batch.stream().map(TaskDefinition::taskId).toList());
        notifyAll();
        return results;
    }

    private void register(TaskDefinition definition) {
        records.put(definition.taskId(), new TaskRecord(definition));
    }

    // ------------------------------------------------------------- 队列暂停/恢复

    /**
     * 暂停队列：不再向该队列派发新任务；已运行任务按原规则自然结束或超时，
     * 排队任务停留在 QUEUED 并保持排队顺序，恢复后继续调度。幂等。
     */
    public synchronized void pauseQueue(String queue) {
        if (pausedQueues.add(queue)) {
            log.info("queue paused: queue={}", queue);
        }
    }

    /** 恢复队列：被压住的排队任务按原排队顺序继续参与调度。幂等。 */
    public synchronized void resumeQueue(String queue) {
        if (pausedQueues.remove(queue)) {
            log.info("queue resumed: queue={}", queue);
            notifyAll();
        }
    }

    public synchronized boolean isQueuePaused(String queue) {
        return pausedQueues.contains(queue);
    }

    // ------------------------------------------------------------- 优先级/老化

    /**
     * 设置排队老化间隔（毫秒）。任务在 QUEUED 每等待满一个间隔，有效优先级 +1，
     * 防止低优先级任务被无限期压住；<=0 表示关闭老化。默认 1000ms。
     */
    public void setAgingIntervalMillis(long agingIntervalMillis) {
        this.agingIntervalMillis = agingIntervalMillis;
    }

    /** 有效优先级 = 基础优先级 + 等待老化加成。调用方必须持有调度器锁。 */
    private long effectivePriority(TaskRecord record, long nowMillis) {
        long interval = agingIntervalMillis;
        long boost = 0L;
        if (interval > 0 && record.queuedSinceMillis > 0) {
            boost = Math.max(0L, nowMillis - record.queuedSinceMillis) / interval;
        }
        return record.definition.priority() + boost;
    }

    // ------------------------------------------------------------- 取消

    /**
     * 取消任务。幂等：任务处于可取消状态时取消并返回 true；
     * 已进入终态则返回 false，重复调用不产生额外对外效果。
     */
    public synchronized boolean cancel(String taskId) {
        TaskRecord record = records.get(taskId);
        if (record == null || record.isTerminal()) {
            log.info("cancel ignored: task={} status={}", taskId,
                    record == null ? "NOT_FOUND" : record.status);
            return false;
        }
        TaskStatus previous = record.status;
        record.status = TaskStatus.CANCELLED;
        record.reason = FailureReason.CANCELLED_BY_USER;
        // 先取执行句柄再释放配额（释放会清空句柄），保证中断信号送达执行线程
        Future<?> future = record.runningFuture;
        releaseQuotaHeld(record);
        if (future != null) {
            future.cancel(true);
        }
        log.info("task cancelled: task={} previousStatus={} reason={}",
                taskId, previous, FailureReason.CANCELLED_BY_USER);
        notifyAll();
        return true;
    }

    // ------------------------------------------------------------- 查询

    public synchronized TaskSnapshot snapshot(String taskId) {
        TaskRecord record = records.get(taskId);
        return record == null ? null : record.toSnapshot();
    }

    public synchronized List<TaskSnapshot> snapshots() {
        return records.values().stream().map(TaskRecord::toSnapshot).toList();
    }

    /** 阻塞直到所有任务进入终态或超时，返回是否全部终态。 */
    public boolean awaitTerminal(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (this) {
            while (!allTerminal()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return allTerminal();
                }
                wait(Math.min(remaining, 50L));
            }
            return true;
        }
    }

    private boolean allTerminal() {
        for (TaskRecord r : records.values()) {
            if (!r.isTerminal()) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------- 调度循环

    public void start() {
        if (running) {
            return;
        }
        running = true;
        schedulerThread = new Thread(this::runLoop, "scheduler-loop");
        schedulerThread.setDaemon(true);
        schedulerThread.start();
        log.info("scheduler started");
    }

    public void shutdown() {
        accepting = false;
        draining = false;
        running = false;
        if (schedulerThread != null) {
            schedulerThread.interrupt();
        }
        workers.shutdownNow();
        try {
            workers.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("scheduler shutdown");
    }

    /**
     * 优雅停止/排空：立即停止接受新任务与派发新尝试，给运行中任务
     * {@code gracePeriodMillis} 的等待时间自然收尾；到点仍未结束的运行中任务
     * 按超时逻辑收尾（中断执行线程、释放配额、FAILED/TIMEOUT），
     * 仍在 PENDING/QUEUED 的任务以 CANCELLED/SHUTDOWN_DRAINED 终结。
     * 返回时保证：所有任务处于终态、所有已占用配额恰好释放一次。
     */
    public void shutdownGracefully(long gracePeriodMillis) {
        synchronized (this) {
            if (!accepting && !draining) {
                log.info("graceful shutdown ignored: scheduler already stopped");
                return;
            }
            accepting = false;
            draining = true;
            log.info("graceful shutdown initiated: gracePeriodMs={}", gracePeriodMillis);
            notifyAll();
        }
        long deadline = System.currentTimeMillis() + gracePeriodMillis;
        synchronized (this) {
            while (hasRunningTasks()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }
                try {
                    wait(Math.min(remaining, 50L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            finalizeDrain();
        }
        // 停止调度循环与工作线程池（此时所有任务已终态，不再有需要派发的尝试）
        running = false;
        draining = false;
        if (schedulerThread != null) {
            schedulerThread.interrupt();
        }
        workers.shutdownNow();
        try {
            workers.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("graceful shutdown completed: {}", summarizeOutcomes());
    }

    private boolean hasRunningTasks() {
        for (TaskRecord r : records.values()) {
            if (r.status == TaskStatus.RUNNING) {
                return true;
            }
        }
        return false;
    }

    /** 排空收尾：运行中任务按超时终结，排队/等待任务按排空取消。调用方必须持有调度器锁。 */
    private void finalizeDrain() {
        for (TaskRecord record : records.values()) {
            switch (record.status) {
                case RUNNING -> {
                    // 先取执行句柄再释放配额，保证中断信号送达执行线程
                    Future<?> future = record.runningFuture;
                    releaseQuotaHeld(record);
                    if (future != null) {
                        future.cancel(true);
                    }
                    record.status = TaskStatus.FAILED;
                    record.reason = FailureReason.TIMEOUT;
                    log.info("drain timeout finish: task={} attempts={} reason=TIMEOUT",
                            record.definition.taskId(), record.attempts);
                }
                case PENDING, QUEUED -> {
                    TaskStatus previous = record.status;
                    record.status = TaskStatus.CANCELLED;
                    record.reason = FailureReason.SHUTDOWN_DRAINED;
                    log.info("drain cancel: task={} previousStatus={} reason=SHUTDOWN_DRAINED",
                            record.definition.taskId(), previous);
                }
                default -> { }
            }
        }
        notifyAll();
    }

    private String summarizeOutcomes() {
        long succeeded = 0, failed = 0, cancelled = 0, blocked = 0;
        synchronized (this) {
            for (TaskRecord r : records.values()) {
                switch (r.status) {
                    case SUCCEEDED -> succeeded++;
                    case FAILED -> failed++;
                    case CANCELLED -> cancelled++;
                    case BLOCKED -> blocked++;
                    default -> { }
                }
            }
        }
        return "succeeded=" + succeeded + " failed=" + failed
                + " cancelled=" + cancelled + " blocked=" + blocked;
    }

    private void runLoop() {
        while (running) {
            try {
                tick();
                Thread.sleep(TICK_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.error("scheduler tick error", e);
            }
        }
    }

    private void tick() {
        synchronized (this) {
            for (TaskRecord record : records.values()) {
                if (record.isTerminal()) {
                    continue;
                }
                switch (record.status) {
                    case PENDING -> evaluateDependencies(record);
                    case RUNNING -> checkTimeout(record);
                    default -> { }
                }
            }
            dispatchQueued();
        }
    }

    /** 依据上游状态决定 PENDING 任务：继续等待 / 被阻塞 / 进入排队。 */
    private void evaluateDependencies(TaskRecord record) {
        boolean upstreamFailed = false;
        boolean upstreamCancelled = false;
        boolean allSucceeded = true;
        for (String depId : record.definition.dependencies()) {
            TaskRecord dep = records.get(depId);
            if (dep == null) {
                // 提交校验保证不会发生，防御性判定
                allSucceeded = false;
                continue;
            }
            switch (dep.status) {
                case SUCCEEDED -> { }
                case FAILED, BLOCKED -> {
                    upstreamFailed = true;
                    allSucceeded = false;
                }
                case CANCELLED -> {
                    upstreamCancelled = true;
                    allSucceeded = false;
                }
                default -> allSucceeded = false;
            }
        }
        if (upstreamFailed || upstreamCancelled) {
            FailureReason reason = upstreamFailed
                    ? FailureReason.DEPENDENCY_FAILED
                    : FailureReason.DEPENDENCY_CANCELLED;
            block(record, reason);
            return;
        }
        if (allSucceeded) {
            record.status = TaskStatus.QUEUED;
            record.reason = FailureReason.NONE;
            if (record.queuedSinceMillis == 0L) {
                record.queuedSinceMillis = System.currentTimeMillis();
            }
            log.info("task ready: task={} -> QUEUED", record.definition.taskId());
        }
    }

    private void block(TaskRecord record, FailureReason reason) {
        record.status = TaskStatus.BLOCKED;
        record.reason = reason;
        log.info("task blocked: task={} reason={}", record.definition.taskId(), reason);
        notifyAll();
    }

    /**
     * 按队列分组派发 QUEUED 任务：同队列内按有效优先级（基础优先级 + 老化加成）
     * 降序、等待时间升序排列；排队型队列采用队头阻塞以严格保序，
     * 拒绝型队列配额不足时该任务进入终态后继续考察后续任务。
     */
    private void dispatchQueued() {
        // 优雅停止排空阶段：不再派发任何新任务
        if (draining) {
            return;
        }
        Map<String, List<TaskRecord>> byQueue = new LinkedHashMap<>();
        for (TaskRecord record : records.values()) {
            String queue = record.definition.queue();
            if (record.status == TaskStatus.QUEUED && !pausedQueues.contains(queue)) {
                byQueue.computeIfAbsent(queue, k -> new ArrayList<>()).add(record);
            }
        }
        long now = System.currentTimeMillis();
        for (Map.Entry<String, List<TaskRecord>> entry : byQueue.entrySet()) {
            String queue = entry.getKey();
            List<TaskRecord> candidates = entry.getValue();
            candidates.sort(Comparator
                    .comparingLong((TaskRecord r) -> effectivePriority(r, now)).reversed()
                    .thenComparingLong(r -> r.queuedSinceMillis));
            for (TaskRecord record : candidates) {
                if (record.status != TaskStatus.QUEUED) {
                    continue;
                }
                if (!tryDispatch(record)) {
                    // 排队型队列：队头阻塞，不再考察本队列后续任务，保证顺序语义
                    break;
                }
            }
        }
    }

    /**
     * 单个 QUEUED 任务尝试获取配额并派发。
     *
     * @return true 表示已派发或已按拒绝策略终结（可继续考察同队列后续任务）；
     *         false 表示排队型队列配额不足（调用方应停止本队列本轮派发）
     */
    private boolean tryDispatch(TaskRecord record) {
        String queue = record.definition.queue();
        if (quotaManager.tryAcquire(queue, record.definition.cost())) {
            record.status = TaskStatus.RUNNING;
            record.attempts++;
            record.attemptStartMillis = System.currentTimeMillis();
            record.dispatched = true;
            record.quotaHeld = true;
            // 持锁提交并记录句柄，保证超时/取消总能拿到本次尝试的 Future；
            // 尝试序号随任务记录单调递增，用于丢弃迟到/越界的结果汇报
            final int attempt = record.attempts;
            record.runningFuture = workers.submit(
                    () -> executeAttempt(record.definition.taskId(), attempt));
            log.info("quota acquired: task={} queue={} attempt={} cost={} priority={} effectivePriority={} waitedMs={}",
                    record.definition.taskId(), queue, record.attempts,
                    record.definition.cost(), record.definition.priority(),
                    effectivePriority(record, record.attemptStartMillis),
                    record.queuedSinceMillis > 0
                            ? record.attemptStartMillis - record.queuedSinceMillis : 0L);
            return true;
        }
        if (quotaManager.isRejectWhenFull(queue)) {
            record.status = TaskStatus.FAILED;
            record.reason = FailureReason.QUOTA_REJECTED;
            log.info("task rejected by full queue: task={} queue={} reason={}",
                    record.definition.taskId(), queue, FailureReason.QUOTA_REJECTED);
            notifyAll();
            return true;
        }
        // 排队型队列：继续停留在 QUEUED，等待下一轮调度
        return false;
    }

    private void checkTimeout(TaskRecord record) {
        long timeout = record.definition.timeoutMillis();
        if (timeout <= 0) {
            return;
        }
        long elapsed = System.currentTimeMillis() - record.attemptStartMillis;
        if (elapsed < timeout) {
            return;
        }
        log.info("attempt timeout: task={} attempt={} elapsedMs={} limitMs={}",
                record.definition.taskId(), record.attempts, elapsed, timeout);
        // 先取执行句柄再释放配额（释放会清空句柄），保证中断信号送达执行线程
        Future<?> future = record.runningFuture;
        releaseQuotaHeld(record);
        if (future != null) {
            future.cancel(true);
        }
        if (record.attempts <= record.definition.maxRetries()) {
            record.status = TaskStatus.QUEUED;
            record.reason = FailureReason.NONE;
            log.info("schedule retry after timeout: task={} nextAttempt={} maxRetries={}",
                    record.definition.taskId(), record.attempts + 1,
                    record.definition.maxRetries());
        } else {
            record.status = TaskStatus.FAILED;
            record.reason = FailureReason.TIMEOUT;
            log.info("task failed: task={} reason=TIMEOUT attempts={}",
                    record.definition.taskId(), record.attempts);
        }
        notifyAll();
    }

    // ------------------------------------------------------------- 执行

    private void executeAttempt(String taskId, int attempt) {
        TaskDefinition definition;
        synchronized (this) {
            TaskRecord record = records.get(taskId);
            if (record == null || record.status != TaskStatus.RUNNING
                    || record.attempts != attempt) {
                return;
            }
            definition = record.definition;
        }
        Throwable error = null;
        try {
            definition.handler().run();
        } catch (Throwable t) {
            error = t;
        }
        final Throwable outcome = error;
        // 完成回调统一串行化到调度器锁上，避免与超时/取消竞争
        try {
            synchronized (this) {
                reportOutcome(taskId, attempt, outcome);
            }
        } catch (Throwable t) {
            log.error("report outcome failed: task={}", taskId, t);
        }
    }

    private void reportOutcome(String taskId, int attempt, Throwable error) {
        TaskRecord record = records.get(taskId);
        // 仅当该尝试仍是当前运行中的尝试时采纳结果；超时/取消已先行转移状态，
        // 或更新的尝试已派发（迟到结果），一律丢弃，
        // 因此不会重复释放配额或重复改变终态。
        if (record == null || record.status != TaskStatus.RUNNING
                || record.attempts != attempt) {
            return;
        }
        if (error == null) {
            record.status = TaskStatus.SUCCEEDED;
            record.reason = FailureReason.NONE;
            releaseQuotaHeld(record);
            log.info("task succeeded: task={} attempts={}", taskId, record.attempts);
            notifyAll();
            return;
        }
        releaseQuotaHeld(record);
        if (record.attempts <= record.definition.maxRetries()) {
            record.status = TaskStatus.QUEUED;
            record.reason = FailureReason.NONE;
            log.info("attempt failed, schedule retry: task={} attempt={} maxRetries={} error={}",
                    taskId, record.attempts, record.definition.maxRetries(),
                    error.toString());
        } else {
            record.status = TaskStatus.FAILED;
            record.reason = FailureReason.RETRY_EXHAUSTED;
            log.info("task failed: task={} reason=RETRY_EXHAUSTED attempts={} lastError={}",
                    taskId, record.attempts, error.toString());
        }
        notifyAll();
    }

    /** 若当前尝试持有配额则恰好释放一次。调用方必须持有调度器锁。 */
    private void releaseQuotaHeld(TaskRecord record) {
        if (record.quotaHeld) {
            record.quotaHeld = false;
            quotaManager.release(record.definition.queue(), record.definition.cost());
            log.info("quota released: task={} queue={} cost={}",
                    record.definition.taskId(), record.definition.queue(),
                    record.definition.cost());
        }
        record.runningFuture = null;
    }
}
