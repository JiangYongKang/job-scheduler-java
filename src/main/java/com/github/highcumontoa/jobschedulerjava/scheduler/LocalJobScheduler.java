package com.github.highcumontoa.jobschedulerjava.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.QueueQuota;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.SubmitErrorCode;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.SubmitException;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskContext;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于单把状态锁 + 工作线程池 + 超时调度线程的进程内调度器。
 *
 * <p>设计要点：</p>
 * <ul>
 *   <li>所有共享状态（任务状态、队列用量、就绪队列）仅在 {@link #lock} 下变更，
 *       任务体在锁外执行，完成后带 epoch 校验回锁提交结论，杜绝重复执行/过期回调覆盖。</li>
 *   <li>提交期：空 id、重复 id、未知队列、未知依赖、成环分别以不同 {@link SubmitErrorCode} 拒绝，整批原子。</li>
 *   <li>调度期：依赖全成功才参与配额；配额不足按任务策略进入 QUEUED（FIFO）或 REJECTED；
 *       上游进入任何非成功终态，下游级联 SKIPPED，原因 DEPENDENCY_FAILED。</li>
 *   <li>配额在每次执行开始时占用、在每次执行结束的所有路径（成功/失败/超时/取消）下回收。</li>
 * </ul>
 */
public class LocalJobScheduler implements JobScheduler {

    private static final Logger log = LoggerFactory.getLogger(LocalJobScheduler.class);
    private static final String DEFAULT_QUEUE = "default";

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    private final ExecutorService workers;
    private final ScheduledExecutorService timeouts;
    private final boolean ownsPools;
    private volatile boolean shutdown = false;

    /** id -> 运行态 */
    private final Map<String, RuntimeTask> tasks = new LinkedHashMap<>();
    /** 队列名 -> 配额 */
    private final Map<String, QueueQuota> quotas = new HashMap<>();
    /** 队列名 -> 当前用量 */
    private final Map<String, QueueUsage> usage = new HashMap<>();
    /** 队列名 -> 已满足依赖、等待配额的 FIFO 就绪队列 */
    private final Map<String, Deque<RuntimeTask>> readyQueues = new HashMap<>();
    /** 任务 id -> 直接下游 id 集合 */
    private final Map<String, Set<String>> dependents = new HashMap<>();

    public LocalJobScheduler() {
        this(defaultWorkers(), defaultTimeoutScheduler(), true);
    }

    public LocalJobScheduler(ExecutorService executor) {
        this(executor, defaultTimeoutScheduler(), false);
    }

    LocalJobScheduler(ExecutorService workers, ScheduledExecutorService timeouts, boolean ownsPools) {
        this.workers = workers;
        this.timeouts = timeouts;
        this.ownsPools = ownsPools;
        registerQueue(new QueueQuota(DEFAULT_QUEUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, -1));
    }

    private static ExecutorService defaultWorkers() {
        return Executors.newCachedThreadPool(daemonFactory("scheduler-worker-"));
    }

    private static ScheduledExecutorService defaultTimeoutScheduler() {
        return Executors.newScheduledThreadPool(1, daemonFactory("scheduler-timeout-"));
    }

    private static ThreadFactory daemonFactory(String prefix) {
        return r -> {
            Thread t = new Thread(r, prefix + System.nanoTime());
            t.setDaemon(true);
            return t;
        };
    }

    // ------------------------------------------------------------------
    // 队列注册
    // ------------------------------------------------------------------

    @Override
    public void registerQueue(QueueQuota quota) {
        lock.lock();
        try {
            quotas.put(quota.getQueue(), quota);
            usage.computeIfAbsent(quota.getQueue(), q -> new QueueUsage());
            readyQueues.computeIfAbsent(quota.getQueue(), q -> new ArrayDeque<>());
            log.info("registerQueue queue={} maxConcurrency={} cpuLimit={} memoryLimit={} maxQueued={}",
                    quota.getQueue(), quota.getMaxConcurrency(), quota.getCpuLimit(),
                    quota.getMemoryLimit(), quota.getMaxQueued());
            pumpAllQueues();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 提交与校验
    // ------------------------------------------------------------------

    @Override
    public void submit(Collection<TaskDefinition> definitions) {
        if (definitions == null || definitions.isEmpty()) {
            return;
        }
        lock.lock();
        try {
            List<TaskDefinition> batch = new ArrayList<>(definitions);
            validate(batch);

            for (TaskDefinition def : batch) {
                RuntimeTask rt = new RuntimeTask(def);
                tasks.put(def.getId(), rt);
                usage.computeIfAbsent(def.getQueue(), q -> new QueueUsage());
                readyQueues.computeIfAbsent(def.getQueue(), q -> new ArrayDeque<>());
                for (String dep : def.getDependencies()) {
                    dependents.computeIfAbsent(dep, d -> new HashSet<>()).add(def.getId());
                }
                log.info("submit accepted taskId={} queue={} deps={} cpu={} memory={} maxRetries={} timeoutMs={} rejectWhenBusy={}",
                        def.getId(), def.getQueue(), def.getDependencies(), def.getCpuUnits(),
                        def.getMemoryBytes(), def.getMaxRetries(), def.getTimeoutMillis(), def.isRejectWhenBusy());
            }
            // 按提交顺序尝试推进；无依赖者会立刻运行或排队，有依赖者保持 PENDING。
            for (TaskDefinition def : batch) {
                tryDispatch(tasks.get(def.getId()));
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void validate(List<TaskDefinition> batch) {
        Set<String> batchIds = new HashSet<>();
        // 第一遍：单任务静态校验，并收集本批全部 id。
        for (TaskDefinition def : batch) {
            if (def == null) {
                throw new SubmitException(SubmitErrorCode.MISSING_ID, "任务定义不能为空");
            }
            if (def.getId() == null || def.getId().isBlank()) {
                throw new SubmitException(SubmitErrorCode.MISSING_ID, "任务 id 不能为空");
            }
            if (!batchIds.add(def.getId())) {
                throw new SubmitException(SubmitErrorCode.DUPLICATE_ID,
                        "同一提交批次中存在重复任务 id: " + def.getId());
            }
            if (tasks.containsKey(def.getId())) {
                throw new SubmitException(SubmitErrorCode.DUPLICATE_ID,
                        "任务 id 已存在，重复提交被拒绝: " + def.getId());
            }
            if (!quotas.containsKey(def.getQueue())) {
                throw new SubmitException(SubmitErrorCode.UNKNOWN_QUEUE,
                        "队列未注册，无法提交: queue=" + def.getQueue() + ", taskId=" + def.getId());
            }
        }
        // 第二遍：依赖引用校验（此时本批 id 集合已完整，顺序无关）。
        for (TaskDefinition def : batch) {
            for (String dep : def.getDependencies()) {
                if (dep.equals(def.getId())) {
                    throw new SubmitException(SubmitErrorCode.CYCLIC_DEPENDENCY,
                            "任务不能依赖自身: " + def.getId());
                }
                if (!tasks.containsKey(dep) && !batchIds.contains(dep)) {
                    throw new SubmitException(SubmitErrorCode.UNKNOWN_DEPENDENCY,
                            "任务 " + def.getId() + " 引用了不存在的前置任务: " + dep);
                }
            }
        }
        detectCycle(batch);
    }

    /** 在「已存在图 ∪ 本批」上做三色 DFS 环检测。 */
    private void detectCycle(List<TaskDefinition> batch) {
        Map<String, Integer> color = new HashMap<>(); // 0=白 1=灰 2=黑
        Map<String, Set<String>> adjacency = new HashMap<>();
        for (RuntimeTask rt : tasks.values()) {
            adjacency.put(rt.def.getId(), rt.def.getDependencies());
            color.put(rt.def.getId(), 0);
        }
        for (TaskDefinition def : batch) {
            adjacency.put(def.getId(), def.getDependencies());
            color.putIfAbsent(def.getId(), 0);
        }
        for (String id : adjacency.keySet()) {
            if (color.get(id) == 0) {
                dfsCycle(id, color, adjacency);
            }
        }
    }

    private void dfsCycle(String id, Map<String, Integer> color, Map<String, Set<String>> adjacency) {
        color.put(id, 1);
        for (String dep : adjacency.getOrDefault(id, Set.of())) {
            Integer c = color.get(dep);
            if (c == null) {
                continue; // 未知依赖已在前面单独校验
            }
            if (c == 1) {
                throw new SubmitException(SubmitErrorCode.CYCLIC_DEPENDENCY,
                        "依赖关系成环，环上任务包含: " + dep);
            }
            if (c == 0) {
                dfsCycle(dep, color, adjacency);
            }
        }
        color.put(id, 2);
    }

    // ------------------------------------------------------------------
    // 调度核心（仅在锁内调用）
    // ------------------------------------------------------------------

    /**
     * 尝试推进一个 PENDING 任务：核对依赖结论，再尝试获取配额。
     * 任何终态结论都会继续级联推进下游，并在资源释放后泵送就绪队列。
     */
    private void tryDispatch(RuntimeTask rt) {
        if (rt.status != TaskStatus.PENDING) {
            return;
        }
        // 1. 依赖核对
        for (String depId : rt.def.getDependencies()) {
            RuntimeTask dep = tasks.get(depId);
            if (dep == null || dep.status != TaskStatus.SUCCEEDED) {
                if (dep != null && dep.status.isTerminal()) {
                    cascadeSkip(rt, depId);
                    return;
                }
                // 上游仍在途，继续等待
                return;
            }
        }
        // 2. 配额核对
        QueueQuota q = quotas.get(rt.def.getQueue());
        QueueUsage u = usage.get(rt.def.getQueue());
        if (fits(q, u, rt)) {
            launch(rt, q, u);
            return;
        }
        if (rt.def.isRejectWhenBusy()) {
            toRejected(rt, "队列配额不足且任务策略为 REJECT: queue=" + rt.def.getQueue()
                    + ", 需要 cpu=" + rt.def.getCpuUnits() + " memory=" + rt.def.getMemoryBytes()
                    + ", 当前占用 " + u);
            return;
        }
        Deque<RuntimeTask> ready = readyQueues.get(rt.def.getQueue());
        if (q.getMaxQueued() >= 0 && ready.size() >= q.getMaxQueued()) {
            toRejected(rt, "队列排队容量已满: queue=" + rt.def.getQueue()
                    + ", maxQueued=" + q.getMaxQueued());
            return;
        }
        rt.status = TaskStatus.QUEUED;
        rt.failureReason = FailureReason.NONE;
        ready.addLast(rt);
        log.info("taskQueued taskId={} queue={} 当前占用 {} 排队长度={}",
                rt.def.getId(), rt.def.getQueue(), u, ready.size());
    }

    private boolean fits(QueueQuota q, QueueUsage u, RuntimeTask rt) {
        return u.running < q.getMaxConcurrency()
                && (long) u.cpu + rt.def.getCpuUnits() <= q.getCpuLimit()
                && u.memory + rt.def.getMemoryBytes() <= q.getMemoryLimit();
    }

    /** 分配配额并把任务体投递到工作线程池。 */
    private void launch(RuntimeTask rt, QueueQuota q, QueueUsage u) {
        if (shutdown) {
            toRejected(rt, "调度器已关闭，不再接受调度: taskId=" + rt.def.getId());
            return;
        }
        rt.attempt += 1;
        rt.epoch += 1;
        rt.status = TaskStatus.RUNNING;
        rt.failureReason = FailureReason.NONE;
        rt.reasonDetail = null;
        rt.cancelRequested = false;
        rt.timedOut = false;
        u.running += 1;
        u.cpu += rt.def.getCpuUnits();
        u.memory += rt.def.getMemoryBytes();

        final int epoch = rt.epoch;
        final int attempt = rt.attempt;
        final long deadline = rt.def.getTimeoutMillis() > 0
                ? System.currentTimeMillis() + rt.def.getTimeoutMillis() : Long.MAX_VALUE;
        log.info("taskLaunch taskId={} queue={} attempt={}/{} 占用后 {} (cpu+{}, memory+{})",
                rt.def.getId(), rt.def.getQueue(), attempt, rt.allowedAttempts, u,
                rt.def.getCpuUnits(), rt.def.getMemoryBytes());

        ScheduledFuture<?> timeoutFuture = null;
        if (rt.def.getTimeoutMillis() > 0) {
            timeoutFuture = timeouts.schedule(() -> onTimeout(rt, epoch),
                    rt.def.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        final ScheduledFuture<?> tf = timeoutFuture;
        Future<?> workerFuture;
        try {
            workerFuture = workers.submit(() -> runBody(rt, epoch, attempt, deadline, tf));
        } catch (RuntimeException ex) {
            // 线程池拒绝（例如已关闭）：立即回收本次占用。
            u.running -= 1;
            u.cpu -= rt.def.getCpuUnits();
            u.memory -= rt.def.getMemoryBytes();
            if (tf != null) {
                tf.cancel(false);
            }
            rt.status = TaskStatus.FAILED;
            rt.failureReason = FailureReason.RETRIES_EXHAUSTED;
            rt.reasonDetail = "调度器拒绝执行: " + ex.getMessage();
            propagateAfterFinish(rt);
            changed.signalAll();
            return;
        }
        rt.workerFuture = workerFuture;
    }

    private void runBody(RuntimeTask rt, int epoch, int attempt, long deadlineEpoch, ScheduledFuture<?> timeoutFuture) {
        Throwable error = null;
        boolean interrupted = Thread.currentThread().isInterrupted();
        try {
            TaskContext ctx = new TaskContext(rt.def.getId(), attempt, deadlineEpoch);
            rt.def.getBody().execute(ctx);
        } catch (Throwable t) {
            error = t;
            interrupted = Thread.currentThread().isInterrupted() || t instanceof InterruptedException;
        } finally {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
            // 消除「中断信号到达晚于任务体返回」的竞态：
            // 重新读取中断标志，并以调度器超时/取消标记为准，任务体正常返回也不能判成功。
            if (error == null) {
                interrupted = Thread.currentThread().isInterrupted() || rt.timedOut || rt.cancelRequested;
            }
            complete(rt, epoch, error, interrupted);
        }
    }

    private void onTimeout(RuntimeTask rt, int epoch) {
        lock.lock();
        try {
            if (rt.epoch != epoch || rt.status != TaskStatus.RUNNING) {
                return;
            }
            rt.timedOut = true;
            log.warn("taskTimeoutInterrupt taskId={} attempt={} 超时，中断工作线程", rt.def.getId(), rt.attempt);
            if (rt.workerFuture != null) {
                rt.workerFuture.cancel(true);
            }
        } finally {
            lock.unlock();
        }
    }

    /** 工作线程回锁提交执行结论（带 epoch 校验，过期回调直接忽略）。 */
    private void complete(RuntimeTask rt, int epoch, Throwable error, boolean interrupted) {
        lock.lock();
        try {
            if (rt.epoch != epoch || rt.status != TaskStatus.RUNNING) {
                // 过期回调（取消/新一轮执行已开始），资源由对应轮次负责，什么都不做。
                return;
            }
            releaseQuota(rt);

            if (error == null && !interrupted) {
                rt.status = TaskStatus.SUCCEEDED;
                rt.failureReason = FailureReason.NONE;
                rt.reasonDetail = null;
                rt.lastError = null;
                log.info("taskSucceeded taskId={} attempt={}", rt.def.getId(), rt.attempt);
            } else if (rt.cancelRequested) {
                rt.status = TaskStatus.CANCELLED;
                rt.failureReason = FailureReason.CANCELLED;
                rt.reasonDetail = "用户取消，工作线程响应中断退出";
                log.info("taskCancelled taskId={} attempt={}", rt.def.getId(), rt.attempt);
            } else if (rt.timedOut) {
                rt.lastError = error != null ? error.getMessage() : "execution timed out";
                if (rt.attempt < rt.allowedAttempts) {
                    log.warn("taskTimeoutRetry taskId={} attempt={}/{} 将重试",
                            rt.def.getId(), rt.attempt, rt.allowedAttempts);
                    requeueForRetry(rt);
                } else {
                    rt.status = TaskStatus.TIMED_OUT;
                    rt.failureReason = FailureReason.TIMEOUT;
                    rt.reasonDetail = "执行超时且重试已耗尽，末次错误: " + rt.lastError;
                    log.warn("taskTimedOut taskId={} attempt={} {}", rt.def.getId(), rt.attempt, rt.reasonDetail);
                }
            } else {
                rt.lastError = error != null ? String.valueOf(error) : "interrupted";
                if (rt.attempt < rt.allowedAttempts) {
                    log.warn("taskFailedRetry taskId={} attempt={}/{} error={} 将重试",
                            rt.def.getId(), rt.attempt, rt.allowedAttempts, rt.lastError);
                    requeueForRetry(rt);
                } else {
                    rt.status = TaskStatus.FAILED;
                    rt.failureReason = FailureReason.RETRIES_EXHAUSTED;
                    rt.reasonDetail = "执行失败且重试已耗尽，末次错误: " + rt.lastError;
                    log.warn("taskFailed taskId={} attempt={} {}", rt.def.getId(), rt.attempt, rt.reasonDetail);
                }
            }

            rt.workerFuture = null;
            propagateAfterFinish(rt);
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** 把任务重置为 PENDING（保留尝试计数），随后由 {@link #tryDispatch} 决定立即执行还是进入就绪队列。 */
    private void enqueueForSchedule(RuntimeTask rt, String trigger) {
        rt.status = TaskStatus.PENDING;
        rt.failureReason = FailureReason.NONE;
        rt.reasonDetail = null;
        rt.lastError = null;
        log.info("{} taskId={} allowedAttempts={} 重新参与调度", trigger, rt.def.getId(), rt.allowedAttempts);
    }

    /** 失败后重新进入就绪队列尾部排队，遵循 FIFO，不抢占已排队任务的配额。 */
    private void requeueForRetry(RuntimeTask rt) {
        rt.status = TaskStatus.QUEUED;
        rt.failureReason = FailureReason.NONE;
        rt.reasonDetail = null;
        readyQueues.get(rt.def.getQueue()).addLast(rt);
        log.info("taskRetryQueued taskId={} queue={} 下一次为 attempt {}/{}",
                rt.def.getId(), rt.def.getQueue(), rt.attempt + 1, rt.allowedAttempts);
    }

    private void releaseQuota(RuntimeTask rt) {
        QueueUsage u = usage.get(rt.def.getQueue());
        if (u == null) {
            return;
        }
        u.running -= 1;
        u.cpu -= rt.def.getCpuUnits();
        u.memory -= rt.def.getMemoryBytes();
    }

    /** 任务结论落定后：泵送就绪队列 + 推进/级联下游。 */
    private void propagateAfterFinish(RuntimeTask finished) {
        pumpAllQueues();
        Set<String> down = dependents.getOrDefault(finished.def.getId(), Set.of());
        for (String downId : down) {
            RuntimeTask d = tasks.get(downId);
            if (d != null) {
                tryDispatch(d);
            }
        }
    }

    /** FIFO 泵送所有队列：队首放得下就执行，放不下则阻塞后续（可解释的公平语义）。 */
    private void pumpAllQueues() {
        for (Map.Entry<String, Deque<RuntimeTask>> e : readyQueues.entrySet()) {
            Deque<RuntimeTask> ready = e.getValue();
            QueueQuota q = quotas.get(e.getKey());
            QueueUsage u = usage.get(e.getKey());
            while (!ready.isEmpty()) {
                RuntimeTask rt = ready.peek();
                if (rt.status != TaskStatus.QUEUED) {
                    ready.poll();
                    continue;
                }
                if (q == null || !fits(q, u, rt)) {
                    break;
                }
                ready.poll();
                rt.status = TaskStatus.PENDING; // 复用同一条分配路径
                tryDispatch(rt);
            }
        }
    }

    private void toRejected(RuntimeTask rt, String detail) {
        rt.status = TaskStatus.REJECTED;
        rt.failureReason = FailureReason.QUOTA_REJECTED;
        rt.reasonDetail = detail;
        log.warn("taskRejected taskId={} reason={}", rt.def.getId(), detail);
        cascadeDependents(rt);
        changed.signalAll();
    }

    /** 上游未成功：下游（含传递下游）一律 SKIPPED，绝不被调度。 */
    private void cascadeSkip(RuntimeTask rt, String failedUpstreamId) {
        rt.status = TaskStatus.SKIPPED;
        rt.failureReason = FailureReason.DEPENDENCY_FAILED;
        rt.reasonDetail = "上游任务未成功，级联跳过: upstreamTaskId=" + failedUpstreamId;
        log.info("taskSkipped taskId={} upstream={}", rt.def.getId(), failedUpstreamId);
        cascadeDependents(rt);
        changed.signalAll();
    }

    private void cascadeDependents(RuntimeTask upstream) {
        Set<String> down = dependents.getOrDefault(upstream.def.getId(), Set.of());
        for (String downId : down) {
            RuntimeTask d = tasks.get(downId);
            if (d != null && d.status == TaskStatus.PENDING) {
                cascadeSkip(d, upstream.def.getId());
            } else if (d != null && d.status == TaskStatus.QUEUED) {
                // 理论上 QUEUED 时依赖必然已成功，防御性处理。
                readyQueues.get(d.def.getQueue()).remove(d);
                cascadeSkip(d, upstream.def.getId());
            }
        }
    }

    // ------------------------------------------------------------------
    // 查询 / 取消 / 重试
    // ------------------------------------------------------------------

    @Override
    public TaskSnapshot snapshot(String taskId) {
        lock.lock();
        try {
            RuntimeTask rt = tasks.get(taskId);
            return rt == null ? null : rt.toSnapshot();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean cancel(String taskId) {
        lock.lock();
        try {
            RuntimeTask rt = tasks.get(taskId);
            if (rt == null || rt.status.isTerminal()) {
                log.info("cancelIgnored taskId={} 不存在或已终态", taskId);
                return false;
            }
            if (rt.status == TaskStatus.RUNNING) {
                rt.cancelRequested = true;
                if (rt.workerFuture != null) {
                    rt.workerFuture.cancel(true);
                }
                log.info("cancelRequested taskId={} 运行中，已发出中断", taskId);
                changed.signalAll();
                return true;
            }
            if (rt.status == TaskStatus.QUEUED) {
                readyQueues.get(rt.def.getQueue()).remove(rt);
            }
            TaskStatus previous = rt.status;
            rt.status = TaskStatus.CANCELLED;
            rt.failureReason = FailureReason.CANCELLED;
            rt.reasonDetail = "任务在" + (previous == TaskStatus.QUEUED ? "排队中" : "等待依赖时") + "被用户取消";
            log.info("taskCancelled taskId={} 未执行即取消 previous={}", taskId, previous);
            cascadeDependents(rt);
            pumpAllQueues();
            changed.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean retry(String taskId) {
        lock.lock();
        try {
            RuntimeTask rt = tasks.get(taskId);
            if (rt == null) {
                return false;
            }
            switch (rt.status) {
                case FAILED, TIMED_OUT, REJECTED -> {
                    rt.allowedAttempts += 1;
                    enqueueForSchedule(rt, "manualRetry");
                    tryDispatch(rt);
                    changed.signalAll();
                    return true;
                }
                case SKIPPED -> {
                    for (String depId : rt.def.getDependencies()) {
                        RuntimeTask dep = tasks.get(depId);
                        if (dep == null || dep.status != TaskStatus.SUCCEEDED) {
                            log.info("manualRetryDenied taskId={} 上游 {} 尚未成功", taskId, depId);
                            return false;
                        }
                    }
                    enqueueForSchedule(rt, "manualRetrySkipped");
                    tryDispatch(rt);
                    changed.signalAll();
                    return true;
                }
                default -> {
                    log.info("manualRetryIgnored taskId={} 当前状态 {} 不可重试", taskId, rt.status);
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public TaskSnapshot awaitTerminal(String taskId, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        lock.lock();
        try {
            RuntimeTask rt;
            while ((rt = tasks.get(taskId)) != null && !rt.status.isTerminal()) {
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0 || !changed.await(remain, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
            return rt == null ? null : rt.toSnapshot();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean awaitQuiet(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        lock.lock();
        try {
            while (hasInFlight()) {
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0 || !changed.await(remain, TimeUnit.MILLISECONDS)) {
                    return !hasInFlight();
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private boolean hasInFlight() {
        for (RuntimeTask rt : tasks.values()) {
            if (!rt.status.isTerminal()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
        } finally {
            lock.unlock();
        }
        if (ownsPools) {
            workers.shutdownNow();
            timeouts.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // 内部类型
    // ------------------------------------------------------------------

    private static final class QueueUsage {
        int running;
        int cpu;
        long memory;

        @Override
        public String toString() {
            return "QueueUsage{running=" + running + ", cpu=" + cpu + ", memory=" + memory + "}";
        }
    }

    private final class RuntimeTask {
        final TaskDefinition def;
        TaskStatus status = TaskStatus.PENDING;
        FailureReason failureReason = FailureReason.NONE;
        String reasonDetail;
        int attempt;
        int allowedAttempts;
        int epoch;
        volatile boolean cancelRequested;
        volatile boolean timedOut;
        volatile Future<?> workerFuture;
        String lastError;

        RuntimeTask(TaskDefinition def) {
            this.def = def;
            this.allowedAttempts = Math.max(1, def.getMaxRetries() + 1);
        }

        TaskSnapshot toSnapshot() {
            return new TaskSnapshot(def.getId(), def.getQueue(), status, failureReason, reasonDetail,
                    attempt, def.getMaxRetries(), def.getCpuUnits(), def.getMemoryBytes());
        }
    }
}
