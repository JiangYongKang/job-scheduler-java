package com.github.highcumontoa.jobschedulerjava.tests.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.core.LocalJobScheduler;
import com.github.highcumontoa.jobschedulerjava.scheduler.core.SubmitResult;
import com.github.highcumontoa.jobschedulerjava.scheduler.dag.DependencyValidator;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;
import com.github.highcumontoa.jobschedulerjava.scheduler.quota.QueueUsage;
import com.github.highcumontoa.jobschedulerjava.scheduler.quota.QuotaManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 队列暂停/恢复：暂停期间不派发新任务、运行中任务自然结束或超时、
 * 提交/取消/重试语义不变、恢复后按原顺序继续调度、任务不重复执行、配额不泄漏。
 */
class QueuePauseResumeTest {

    private static final Logger log = LoggerFactory.getLogger(QueuePauseResumeTest.class);

    private LocalJobScheduler scheduler;
    private QuotaManager quota;

    @BeforeEach
    void setUp() {
        quota = new QuotaManager();
        // 并发 1：保证执行顺序可观测
        quota.configureQueue("q", 1, 16, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    private void awaitStatus(String taskId, TaskStatus expected, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            TaskSnapshot snap = scheduler.snapshot(taskId);
            if (snap != null && snap.status() == expected) {
                return;
            }
            Thread.sleep(5);
        }
        TaskSnapshot snap = scheduler.snapshot(taskId);
        throw new AssertionError("task " + taskId + " not " + expected + " in time, actual="
                + (snap == null ? "null" : snap.status()));
    }

    @Test
    void pausedQueueHoldsQueuedTasksAndResumePreservesOrder() throws Exception {
        CountDownLatch blockerRelease = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> executionOrder = new ConcurrentLinkedQueue<>();
        // 输入：并发=1；blocker 占位运行；暂停后提交 t1/t2/t3；放行 blocker 后确认三者仍排队；恢复
        scheduler.submit(TaskDefinition.of("blocker", "q", Set.of(), 1, 0, 0, () -> {
            try {
                blockerRelease.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        awaitStatus("blocker", TaskStatus.RUNNING, 5_000);
        scheduler.pauseQueue("q");
        for (String id : List.of("t1", "t2", "t3")) {
            SubmitResult r = scheduler.submit(TaskDefinition.of(id, "q", Set.of(), 1, 0, 0,
                    () -> executionOrder.add(id)));
            assertTrue(r.accepted(), "暂停期间提交应正常受理: " + id);
        }
        log.info("输入: 并发=1, blocker 运行中暂停队列并提交 t1/t2/t3; 判定依据: blocker 完成后 t1/t2/t3 仍 QUEUED, 恢复后按 t1,t2,t3 顺序执行且各一次");
        blockerRelease.countDown();
        awaitStatus("blocker", TaskStatus.SUCCEEDED, 5_000);
        Thread.sleep(150); // 给调度器足够 tick，确认暂停确实压住派发
        for (String id : List.of("t1", "t2", "t3")) {
            TaskSnapshot snap = scheduler.snapshot(id);
            log.info("暂停中状态: {} -> {}", id, snap.status());
            assertEquals(TaskStatus.QUEUED, snap.status(), "暂停期间不得派发: " + id);
        }
        assertTrue(executionOrder.isEmpty(), "暂停期间不得有任务执行");

        scheduler.resumeQueue("q");
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: executionOrder={}", executionOrder);
        assertEquals(List.of("t1", "t2", "t3"), List.copyOf(executionOrder),
                "恢复后应保持暂停前的排队顺序");
        for (String id : List.of("t1", "t2", "t3")) {
            assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot(id).status());
        }
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running(), "结束后配额必须全部释放");
        assertEquals(0, usage.usedResource());
    }

    @Test
    void runningTaskFinishesNaturallyWhilePaused() throws Exception {
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger queuedRuns = new AtomicInteger();
        // 输入：runner 运行中暂停队列；queued 排队；runner 自然结束
        scheduler.submit(TaskDefinition.of("runner", "q", Set.of(), 1, 0, 0, () -> {
            try {
                finish.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        awaitStatus("runner", TaskStatus.RUNNING, 5_000);
        scheduler.pauseQueue("q");
        scheduler.submit(TaskDefinition.of("queued", "q", Set.of(), 1, 0, 0, queuedRuns::incrementAndGet));
        log.info("输入: runner 运行中暂停, queued 排队; 判定依据: runner 自然结束为 SUCCEEDED, queued 保持 QUEUED 直到恢复");
        finish.countDown();
        awaitStatus("runner", TaskStatus.SUCCEEDED, 5_000);
        Thread.sleep(100);
        TaskSnapshot queuedSnap = scheduler.snapshot("queued");
        log.info("结果: runner={} queued={} queuedRuns={}",
                scheduler.snapshot("runner").status(), queuedSnap.status(), queuedRuns.get());
        assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("runner").status(),
                "暂停不影响运行中任务自然结束");
        assertEquals(TaskStatus.QUEUED, queuedSnap.status());
        assertEquals(0, queuedRuns.get());
        scheduler.resumeQueue("q");
        assertTrue(scheduler.awaitTerminal(10_000));
        assertEquals(1, queuedRuns.get(), "恢复后恰好执行一次");
    }

    @Test
    void cancelAndTimeoutRetryDuringPause() throws Exception {
        AtomicInteger timeoutCalls = new AtomicInteger();
        AtomicInteger victimRuns = new AtomicInteger();
        // 输入：to 运行中(timeout=100ms, maxRetries=1)暂停队列；victim 排队中被取消
        scheduler.submit(TaskDefinition.of("to", "q", Set.of(), 1, 1, 100, () -> {
            timeoutCalls.incrementAndGet();
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted");
            }
        }));
        awaitStatus("to", TaskStatus.RUNNING, 5_000);
        scheduler.pauseQueue("q");
        scheduler.submit(TaskDefinition.of("victim", "q", Set.of(), 1, 0, 0, victimRuns::incrementAndGet));
        log.info("输入: to 运行中暂停(timeout=100ms,重试1次), victim 排队中取消; 判定依据: to 超时后回到 QUEUED 不派发, 恢复后第2次尝试超时 -> FAILED/TIMEOUT attempts=2; victim=CANCELLED 且从未执行");
        assertTrue(scheduler.cancel("victim"), "暂停期间取消排队任务应生效");
        // 等待第一次超时发生：to 应回到 QUEUED（有剩余重试）但不被派发
        awaitStatus("to", TaskStatus.QUEUED, 5_000);
        Thread.sleep(150);
        TaskSnapshot toSnap = scheduler.snapshot("to");
        log.info("暂停中超时后状态: to={} attempts={} calls={}", toSnap.status(), toSnap.attempts(), timeoutCalls.get());
        assertEquals(TaskStatus.QUEUED, toSnap.status(), "暂停期间超时重试应停留在 QUEUED");
        assertEquals(1, timeoutCalls.get(), "暂停期间不得派发重试");
        assertEquals(0, quota.snapshot().get(0).running(), "超时后配额必须已释放");

        scheduler.resumeQueue("q");
        assertTrue(scheduler.awaitTerminal(10_000));
        TaskSnapshot toFinal = scheduler.snapshot("to");
        TaskSnapshot victimFinal = scheduler.snapshot("victim");
        log.info("结果: to={}/{} attempts={} calls={} victim={}/{} victimRuns={}",
                toFinal.status(), toFinal.reason(), toFinal.attempts(), timeoutCalls.get(),
                victimFinal.status(), victimFinal.reason(), victimRuns.get());
        assertEquals(TaskStatus.FAILED, toFinal.status());
        assertEquals(FailureReason.TIMEOUT, toFinal.reason());
        assertEquals(2, toFinal.attempts());
        assertEquals(2, timeoutCalls.get(), "每次尝试恰好执行一次，不能重复执行");
        assertEquals(TaskStatus.CANCELLED, victimFinal.status());
        assertEquals(FailureReason.CANCELLED_BY_USER, victimFinal.reason());
        assertEquals(0, victimRuns.get(), "取消的任务不得执行");
        assertEquals(0, quota.snapshot().get(0).running(), "全部结束后配额必须释放干净");
    }

    @Test
    void concurrentSubmissionDuringPauseExecutesExactlyOnceAfterResume() throws Exception {
        int taskCount = 16;
        AtomicInteger totalRuns = new AtomicInteger();
        scheduler.pauseQueue("q");
        log.info("输入: 暂停队列后 {} 个线程各提交 1 个任务; 判定依据: 全部受理、暂停期间零执行, 恢复后每个任务恰好执行一次且配额归零", taskCount);
        ExecutorService pool = Executors.newFixedThreadPool(taskCount);
        try {
            CountDownLatch ready = new CountDownLatch(taskCount);
            CountDownLatch go = new CountDownLatch(1);
            Future<?>[] futures = new Future<?>[taskCount];
            for (int i = 0; i < taskCount; i++) {
                final String id = "c" + i;
                futures[i] = pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return scheduler.submit(TaskDefinition.of(id, "q", Set.of(), 1, 0, 0,
                            totalRuns::incrementAndGet));
                });
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            go.countDown();
            for (Future<?> f : futures) {
                SubmitResult r = (SubmitResult) f.get(5, TimeUnit.SECONDS);
                assertTrue(r.accepted(), "暂停期间并发提交应正常受理");
            }
        } finally {
            pool.shutdownNow();
        }
        Thread.sleep(150);
        assertEquals(0, totalRuns.get(), "暂停期间不得有任何执行");
        scheduler.resumeQueue("q");
        assertTrue(scheduler.awaitTerminal(15_000));
        log.info("结果: totalRuns={} taskCount={}", totalRuns.get(), taskCount);
        assertEquals(taskCount, totalRuns.get(), "每个任务必须恰好执行一次");
        for (int i = 0; i < taskCount; i++) {
            TaskSnapshot snap = scheduler.snapshot("c" + i);
            assertEquals(TaskStatus.SUCCEEDED, snap.status());
            assertEquals(1, snap.attempts(), "不得重复派发: c" + i);
        }
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running());
        assertEquals(0, usage.usedResource());
    }

    @Test
    void pauseIsIdempotentAndDoesNotAffectOtherQueues() throws Exception {
        quota.configureQueue("other", 1, 4, false);
        AtomicInteger otherRuns = new AtomicInteger();
        scheduler.pauseQueue("q");
        scheduler.pauseQueue("q"); // 幂等
        assertTrue(scheduler.isQueuePaused("q"));
        scheduler.submit(TaskDefinition.of("other-task", "other", Set.of(), 1, 0, 0,
                otherRuns::incrementAndGet));
        log.info("输入: 暂停 q(重复暂停幂等), other 队列提交任务; 判定依据: other 正常执行, q 保持暂停");
        assertTrue(scheduler.awaitTerminal(10_000));
        assertEquals(1, otherRuns.get(), "暂停 q 不应影响 other 队列");
        assertTrue(scheduler.isQueuePaused("q"));
        scheduler.resumeQueue("q");
        scheduler.resumeQueue("q"); // 幂等
        assertFalse(scheduler.isQueuePaused("q"));
    }
}
