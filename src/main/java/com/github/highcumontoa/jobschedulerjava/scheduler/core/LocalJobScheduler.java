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
 * - 单次执行结果归属：每次派发带单调递增的执行序号（attemptSeq），执行线程回报结果时
 *   必须带回派发序号，仅当它仍是当前在途执行且任务处于 RUNNING 时结果才会被采纳。
 *   执行体若在被判超时、被取消或优雅停止收尾后仍不理会打断继续运行，其之后到达的
 *   “迟到结果”一律忽略：不改变任务状态、也不替别的执行归还配额。
 * - 在途槽位：同一任务同一时刻只允许一次执行在途。超时/取消/停止收尾会立即释放队列配额，
 *   但在途槽位保留到旧执行线程真正退出，下一次重试过了该时刻才会派发，
 *   因此不会出现同一任务前后两次执行同时在跑；配额在超时/重试/取消/暂停恢复/优雅停止
 *   各链路交织下都恰好回收一次。
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
        // 先取执行句柄再释放配额（releaseQuotaHeld 会清空 runningFuture）。
        // 保留 inFlightSeq：执行体可能不理会中断继续运行，其线程真正退出前
        // 迟到结果仍归属于本次执行并被忽略（任务已是终态，不会再派发新执行）。
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
                    // 释放配额并收尾为终态；保留 inFlightSeq 直到执行线程真正退出，
                    // 其迟到结果归属于本次执行并被忽略（状态已是终态，不会再派发新执行）
                    releaseQuotaHeld(record);
                    if (future != null) {
                        future.cancel(true);
                    }
                    record.status = TaskStatus.FAILED;
                    record.reason = FailureReason.TIMEOUT;
                    log.info("drain finalize: task={} RUNNING -> FAILED/TIMEOUT attempts={} seq={}",
                            record.definition.taskId(), record.attempts, record.inFlightSeq);
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
        if (record.inFlightSeq != 0L) {
            // 上一次执行线程尚未退出（可能是被判超时/取消后仍不理会中断的僵尸执行）。
            // 必须等它真正退出、归还在途槽位后才能派发下一次执行，
            // 保证同一任务同一时刻只有一次执行在途、只有一次执行占用额度。
            return;
        }
        String queue = record.definition.queue();
        if (quotaManager.tryAcquire(queue, record.definition.cost())) {
            record.status = TaskStatus.RUNNING;
            record.attempts++;
            // 派发序号即本次执行的归属凭证：只有序号匹配的回报才会被采纳
            record.attemptSeq++;
            record.inFlightSeq = record.attemptSeq;
            record.attemptStartMillis = System.currentTimeMillis();
            record.dispatched = true;
            record.quotaHeld = true;
            final long dispatchedSeq = record.attemptSeq;
            // 持锁提交并记录句柄，保证超时/取消总能拿到本次尝试的 Future；
            // 执行线程带回派发时的序号，迟到结果按序号识别并忽略
            record.runningFuture = workers.submit(
                    () -> executeAttempt(record.definition.taskId(), dispatchedSeq));
            log.info("quota acquired: task={} queue={} attempt={} seq={} cost={}",
                    record.definition.taskId(), queue, record.attempts, dispatchedSeq,
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
        log.info("attempt timeout: task={} attempt={} seq={} elapsedMs={} limitMs={}",
                record.definition.taskId(), record.attempts, record.inFlightSeq, elapsed, timeout);
        // 先取执行句柄再释放配额（releaseQuotaHeld 会清空 runningFuture）。
        // 注意：此处只释放配额与终态/重试状态，不清 inFlightSeq——
        // 执行体可能不理会中断继续运行，在途槽位必须保留到该线程真正退出，
        // 期间不得派发下一次执行（避免同一任务两次执行同时在途）。
        Future<?> future = record.runningFuture;
        releaseQuotaHeld(record);
        if (future != null) {
            future.cancel(true);
        }
        if (record.attempts <= record.definition.maxRetries()) {
            record.status = TaskStatus.QUEUED;
            record.reason = FailureReason.NONE;
            record.queuedAtMillis = System.currentTimeMillis();
            log.info("schedule retry after timeout: task={} nextAttempt={} maxRetries={} "
                            + "(retry waits for overdue attempt thread to exit, seq={})",
                    record.definition.taskId(), record.attempts + 1,
                    record.definition.maxRetries(), record.inFlightSeq);
        } else {
            record.status = TaskStatus.FAILED;
            record.reason = FailureReason.TIMEOUT;
            log.info("task failed: task={} reason=TIMEOUT attempts={} "
                            + "(overdue attempt thread still in flight, seq={}; its late result will be ignored)",
                    record.definition.taskId(), record.attempts, record.inFlightSeq);
        }
        notifyAll();
    }

    // ------------------------------------------------------------- 执行

    private void executeAttempt(String taskId, long seq) {
        TaskDefinition definition;
        synchronized (this) {
            TaskRecord record = records.get(taskId);
            if (record == null || record.inFlightSeq != seq) {
                // 派发后被覆盖（防御性，正常路径不会发生）：不属于任何在途执行，直接退出
                return;
            }
            if (record.status != TaskStatus.RUNNING) {
                // 派发后、执行体开始前已被超时/取消/收尾：不执行 handler，
                // 只归还在途槽位（配额已由判定方释放），让下一次执行可以被派发
                record.inFlightSeq = 0L;
                notifyAll();
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
                reportOutcome(taskId, seq, outcome);
            }
        } catch (Throwable t) {
            log.error("report outcome failed: task={} seq={}", taskId, seq, t);
        }
    }

    private void reportOutcome(String taskId, long seq, Throwable error) {
        TaskRecord record = records.get(taskId);
        if (record == null) {
            return;
        }
        if (record.inFlightSeq != seq) {
            // 该执行线程已不属于当前在途执行（极端竞争下的防御性丢弃）
            return;
        }
        if (record.status != TaskStatus.RUNNING) {
            // 本次执行在返回前已被判超时 / 被取消 / 在优雅停止中收尾：
            // 结果一律忽略——不改变任务状态、不替别的执行归还配额（配额已由判定方恰好释放一次）。
            // 唯一要做的是归还在途槽位：僵尸执行线程真正退出后，下一次重试才可以被派发。
            record.inFlightSeq = 0L;
            log.info("stale attempt result ignored: task={} seq={} statusAtArrival={} error={}",
                    taskId, seq, record.status, error == null ? "<success>" : error.toString());
            notifyAll();
            return;
        }
        // 结果归属本次执行：执行线程即将退出，归还在途槽位与配额（均恰好一次）
        record.inFlightSeq = 0L;
        if (error == null) {
            record.status = TaskStatus.SUCCEEDED;
            record.reason = FailureReason.NONE;
            releaseQuotaHeld(record);
            log.info("task succeeded: task={} seq={} attempts={}", taskId, seq, record.attempts);
            notifyAll();
            return;
        }
        releaseQuotaHeld(record);
        if (record.attempts <= record.definition.maxRetries()) {
            record.status = TaskStatus.QUEUED;
            record.reason = FailureReason.NONE;
            record.queuedAtMillis = System.currentTimeMillis();
            log.info("attempt failed, schedule retry: task={} seq={} attempt={} maxRetries={} error={}",
                    taskId, seq, record.attempts, record.definition.maxRetries(),
                    error.toString());
        } else {
            record.status = TaskStatus.FAILED;
            record.reason = FailureReason.RETRY_EXHAUSTED;
            log.info("task failed: task={} seq={} reason=RETRY_EXHAUSTED attempts={} lastError={}",
                    taskId, seq, record.attempts, error.toString());
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
