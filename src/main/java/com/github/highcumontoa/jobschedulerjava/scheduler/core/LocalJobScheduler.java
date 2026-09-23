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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * - 任何路径下成功占用的配额在尝试结束（成功/失败/重试/超时/取消）时恰好释放一次。
 */
public class LocalJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(LocalJobScheduler.class);
    private static final long TICK_MILLIS = 5L;

    private final QuotaManager quotaManager;
    private final DependencyValidator validator;
    private final ExecutorService workers;
    private final Map<String, TaskRecord> records = new LinkedHashMap<>();

    private volatile boolean running = false;
    private Thread schedulerThread;

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
        releaseQuotaHeld(record);
        Future<?> future = record.runningFuture;
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
                    case QUEUED -> tryDispatch(record);
                    case RUNNING -> checkTimeout(record);
                    default -> { }
                }
            }
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
        releaseQuotaHeld(record);
        Future<?> future = record.runningFuture;
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
