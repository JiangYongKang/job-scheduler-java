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
 * - 暂停/恢复：pauseQueue 后不再向该队列派发新任务，运行中任务按原规则收尾，
 *   resumeQueue 后排队任务按原顺序继续调度；暂停期间提交/取消/重试/超时语义不变。
 * - 优先级/老化：同队列内等效优先级（priority + 等待时长/agingInterval）高者先派发，
 *   并列按进入 QUEUED 先后；不影响运行中任务，不突破并发与资源限制。
 * - 优雅停止：shutdownGracefully 停止受理新任务（提交返回 SCHEDULER_STOPPED），
 *   宽限期内存量任务按原规则排空；截止后 RUNNING 按 FAILED/TIMEOUT 收尾、
 *   PENDING/QUEUED 按 CANCELLED/SCHEDULER_SHUTDOWN 收尾，返回时无中间状态、无配额占用。
 * - 任何路径下成功占用的配额在尝试结束（成功/失败/重试/超时/取消）时恰好释放一次。
 */
public class LocalJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(LocalJobScheduler.class);
    private static final long TICK_MILLIS = 5L;
    /** 默认排队老化间隔：每等待 1 秒等效优先级 +1 */
    public static final long DEFAULT_AGING_INTERVAL_MILLIS = 1_000L;

    private final QuotaManager quotaManager;
    private final DependencyValidator validator;
    private final ExecutorService workers;
    private final Map<String, TaskRecord> records = new LinkedHashMap<>();
    /** 已暂停的队列：暂停期间不向这些队列派发新任务（持有调度器锁时访问） */
    private final Set<String> pausedQueues = new HashSet<>();
    /** 排队老化间隔（毫秒），<=0 表示不启用老化 */
    private final long agingIntervalMillis;

    private volatile boolean running = false;
    private Thread schedulerThread;

    /** 调度器生命周期阶段（持有调度器锁时读写）。 */
    private enum Phase {
        /** 正常受理与调度 */
        ACCEPTING,
        /** 排空中：不再受理新任务，存量任务继续调度直至终态或到达排空截止 */
        DRAINING,
        /** 已停止：不再受理也不再调度 */
        STOPPED
    }

    private Phase phase = Phase.ACCEPTING;

    public LocalJobScheduler(QuotaManager quotaManager, DependencyValidator validator) {
        this(quotaManager, validator, DEFAULT_AGING_INTERVAL_MILLIS);
    }

    public LocalJobScheduler(QuotaManager quotaManager, DependencyValidator validator,
                             long agingIntervalMillis) {
        this(quotaManager, validator, agingIntervalMillis,
                Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "scheduler-worker");
                    t.setDaemon(true);
                    return t;
                }));
    }

    LocalJobScheduler(QuotaManager quotaManager, DependencyValidator validator,
                      long agingIntervalMillis, ExecutorService workers) {
        this.quotaManager = quotaManager;
        this.validator = validator;
        this.agingIntervalMillis = agingIntervalMillis;
        this.workers = workers;
    }

    // ------------------------------------------------------------- 提交

    public synchronized SubmitResult submit(TaskDefinition definition) {
        if (phase != Phase.ACCEPTING) {
            log.info("submit rejected: task={} reason={} phase={}",
                    definition.taskId(), FailureReason.SCHEDULER_STOPPED, phase);
            return SubmitResult.rejected(definition.taskId(), FailureReason.SCHEDULER_STOPPED,
                    "scheduler is " + phase + ", not accepting new tasks");
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
        if (phase != Phase.ACCEPTING) {
            log.info("batch submit rejected (scheduler stopped): size={} phase={}",
                    batch.size(), phase);
            return batch.stream()
                    .map(d -> SubmitResult.rejected(d.taskId(), FailureReason.SCHEDULER_STOPPED,
                            "scheduler is " + phase + ", not accepting new tasks"))
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
        // 先取执行句柄再释放配额（releaseQuotaHeld 会清空 runningFuture）
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

    // ------------------------------------------------------------- 队列暂停/恢复

    /**
     * 暂停队列：不再向该队列派发新任务；正在运行的任务按原规则自然结束或超时，
     * 排队任务保持 QUEUED 与原有排队顺序，恢复后继续调度。幂等。
     */
    public synchronized void pauseQueue(String queue) {
        if (pausedQueues.add(queue)) {
            log.info("queue paused: queue={}", queue);
        }
    }

    /** 恢复队列：被压住的排队任务按优先级与原有顺序继续参与调度。幂等。 */
    public synchronized void resumeQueue(String queue) {
        if (pausedQueues.remove(queue)) {
            log.info("queue resumed: queue={}", queue);
            notifyAll();
        }
    }

    public synchronized boolean isQueuePaused(String queue) {
        return pausedQueues.contains(queue);
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
        synchronized (this) {
            phase = Phase.STOPPED;
            notifyAll();
        }
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

    // ------------------------------------------------------------- 优雅停止/排空

    /**
     * 优雅停止：立即停止受理新任务（提交返回 SCHEDULER_STOPPED 拒绝），
     * 存量任务在 graceMillis 内继续按原规则调度（排队任务仍会派发、失败仍重试）；
     * 到达截止时间仍未结束的 RUNNING 任务按超时逻辑收尾为 FAILED/TIMEOUT（不再重试），
     * 仍处于 PENDING/QUEUED 的任务收尾为 CANCELLED/SCHEDULER_SHUTDOWN。
     * 返回时保证所有任务均为终态、配额全部释放、调度线程与工作线程池已停止。
     * 幂等：已进入排空或停止阶段时直接返回。
     */
    public void shutdownGracefully(long graceMillis) {
        synchronized (this) {
            if (phase != Phase.ACCEPTING) {
                log.info("graceful shutdown ignored: phase={}", phase);
                return;
            }
            phase = Phase.DRAINING;
            log.info("scheduler draining: graceMillis={} pending={} queued={} running={}",
                    graceMillis, countByStatus(TaskStatus.PENDING),
                    countByStatus(TaskStatus.QUEUED), countByStatus(TaskStatus.RUNNING));
            notifyAll();
        }
        long deadline = System.currentTimeMillis() + Math.max(0L, graceMillis);
        synchronized (this) {
            // 等待所有存量任务到达终态（排队任务在排空期内仍会被派发执行）
            while (!allTerminal()) {
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
            phase = Phase.STOPPED;
            notifyAll();
        }
        shutdown();
    }

    public synchronized boolean isAccepting() {
        return phase == Phase.ACCEPTING;
    }

    private long countByStatus(TaskStatus status) {
        return records.values().stream().filter(r -> r.status == status).count();
    }

    /**
     * 排空收尾：把仍停留在中间状态的任务一次性转移到终态。
     * RUNNING 按超时终态（FAILED/TIMEOUT，中断执行线程、恰好释放一次配额、不再重试）；
     * PENDING/QUEUED 按停止取消（CANCELLED/SCHEDULER_SHUTDOWN）。调用方必须持有调度器锁。
     */
    private void finalizeDrain() {
        for (TaskRecord record : records.values()) {
            if (record.isTerminal()) {
                continue;
            }
            switch (record.status) {
                case RUNNING -> {
                    Future<?> future = record.runningFuture;
                    releaseQuotaHeld(record);
                    if (future != null) {
                        future.cancel(true);
                    }
                    record.status = TaskStatus.FAILED;
                    record.reason = FailureReason.TIMEOUT;
                    log.info("drain finalize: task={} RUNNING -> FAILED/TIMEOUT attempts={}",
                            record.definition.taskId(), record.attempts);
                }
                case PENDING, QUEUED -> {
                    record.status = TaskStatus.CANCELLED;
                    record.reason = FailureReason.SCHEDULER_SHUTDOWN;
                    log.info("drain finalize: task={} -> CANCELLED/SCHEDULER_SHUTDOWN",
                            record.definition.taskId());
                }
                default -> { }
            }
        }
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
        List<TaskRecord> readyToDispatch = new ArrayList<>();
        synchronized (this) {
            for (TaskRecord record : records.values()) {
                if (record.isTerminal()) {
                    continue;
                }
                switch (record.status) {
                    case PENDING -> evaluateDependencies(record);
                    case QUEUED -> readyToDispatch.add(record);
                    case RUNNING -> checkTimeout(record);
                    default -> { }
                }
            }
            dispatchQueued(readyToDispatch);
        }
    }

    /**
     * 按队列暂停状态与（老化后的）优先级顺序派发 QUEUED 任务。
     * 排序规则：等效优先级高者优先；并列时进入 QUEUED 早者优先（List.sort 稳定，
     * 完全并列保持提交顺序）。暂停队列整体跳过，不消耗配额也不改变排队位置。
     */
    private void dispatchQueued(List<TaskRecord> queued) {
        if (queued.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        queued.sort((a, b) -> {
            int byPriority = Long.compare(effectivePriority(b, now), effectivePriority(a, now));
            if (byPriority != 0) {
                return byPriority;
            }
            return Long.compare(a.queuedAtMillis, b.queuedAtMillis);
        });
        for (TaskRecord record : queued) {
            if (record.status != TaskStatus.QUEUED) {
                // 同一 tick 内可能已被前序逻辑改变（防御性检查）
                continue;
            }
            String queue = record.definition.queue();
            if (pausedQueues.contains(queue)) {
                continue;
            }
            tryDispatch(record);
        }
    }

    /** 等效优先级 = 提交优先级 + 等待时长 / 老化间隔；未启用老化时等于提交优先级。 */
    private long effectivePriority(TaskRecord record, long now) {
        long boost = agingIntervalMillis > 0
                ? Math.max(0L, now - record.queuedAtMillis) / agingIntervalMillis
                : 0L;
        return record.definition.priority() + boost;
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
            record.queuedAtMillis = System.currentTimeMillis();
            log.info("task ready: task={} -> QUEUED", record.definition.taskId());
        }
    }

    private void block(TaskRecord record, FailureReason reason) {
        record.status = TaskStatus.BLOCKED;
        record.reason = reason;
        log.info("task blocked: task={} reason={}", record.definition.taskId(), reason);
        notifyAll();
    }

    /** QUEUED 任务尝试获取配额并派发；拒绝型队列配额不足时进入终态。 */
    private void tryDispatch(TaskRecord record) {
        String queue = record.definition.queue();
        if (quotaManager.tryAcquire(queue, record.definition.cost())) {
            record.status = TaskStatus.RUNNING;
            record.attempts++;
            record.attemptStartMillis = System.currentTimeMillis();
            record.dispatched = true;
            record.quotaHeld = true;
            // 持锁提交并记录句柄，保证超时/取消总能拿到本次尝试的 Future
            record.runningFuture = workers.submit(() -> executeAttempt(record.definition.taskId()));
            log.info("quota acquired: task={} queue={} attempt={} cost={}",
                    record.definition.taskId(), queue, record.attempts,
                    record.definition.cost());
        } else if (quotaManager.isRejectWhenFull(queue)) {
            record.status = TaskStatus.FAILED;
            record.reason = FailureReason.QUOTA_REJECTED;
            log.info("task rejected by full queue: task={} queue={} reason={}",
                    record.definition.taskId(), queue, FailureReason.QUOTA_REJECTED);
            notifyAll();
        }
        // 排队型队列：继续停留在 QUEUED，等待下一轮调度
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
        // 先取执行句柄再释放配额（releaseQuotaHeld 会清空 runningFuture）
        Future<?> future = record.runningFuture;
        releaseQuotaHeld(record);
        if (future != null) {
            future.cancel(true);
        }
        if (record.attempts <= record.definition.maxRetries()) {
            record.status = TaskStatus.QUEUED;
            record.reason = FailureReason.NONE;
            record.queuedAtMillis = System.currentTimeMillis();
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

    private void executeAttempt(String taskId) {
        TaskDefinition definition;
        synchronized (this) {
            TaskRecord record = records.get(taskId);
            if (record == null || record.status != TaskStatus.RUNNING) {
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
                reportOutcome(taskId, outcome);
            }
        } catch (Throwable t) {
            log.error("report outcome failed: task={}", taskId, t);
        }
    }

    private void reportOutcome(String taskId, Throwable error) {
        TaskRecord record = records.get(taskId);
        // 仅当该尝试仍在运行态时采纳结果；超时/取消已先行转移状态，迟到结果被丢弃，
        // 因此不会重复释放配额或重复改变终态。
        if (record == null || record.status != TaskStatus.RUNNING) {
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
            record.queuedAtMillis = System.currentTimeMillis();
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
