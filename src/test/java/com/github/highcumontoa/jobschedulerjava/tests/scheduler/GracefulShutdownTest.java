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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 优雅停止/排空：停止后拒绝新任务；排空期内存量任务按原规则收尾；
 * 超时未结束的按超时终态、未运行的按停止取消；无中间状态、无配额泄漏；
 * 停止后提交明确拒绝而非静默丢弃。
 */
class GracefulShutdownTest {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownTest.class);

    private LocalJobScheduler scheduler;
    private QuotaManager quota;

    @BeforeEach
    void setUp() {
        quota = new QuotaManager();
        quota.configureQueue("q", 2, 16, false);
        // 并发 1：保证“排队中”状态可稳定观测
        quota.configureQueue("q1", 1, 16, false);
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

    private void assertAllTerminalAndQuotaClean() {
        for (TaskSnapshot snap : scheduler.snapshots()) {
            assertTrue(snap.status() == TaskStatus.SUCCEEDED || snap.status() == TaskStatus.FAILED
                            || snap.status() == TaskStatus.CANCELLED || snap.status() == TaskStatus.BLOCKED,
                    "任务卡在中间状态: " + snap.taskId() + "=" + snap.status());
        }
        for (QueueUsage usage : quota.snapshot()) {
            assertEquals(0, usage.running(), "配额未释放: " + usage.queue());
            assertEquals(0, usage.usedResource(), "资源未释放: " + usage.queue());
        }
    }

    @Test
    void drainCompletesInflightWorkWithinGrace() throws Exception {
        CountDownLatch slow = new CountDownLatch(1);
        AtomicInteger queuedRuns = new AtomicInteger();
        scheduler.submit(TaskDefinition.of("running", "q", Set.of(), 1, 0, 0, () -> {
            try {
                slow.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        scheduler.submit(TaskDefinition.of("queued", "q", Set.of(), 1, 0, 0,
                queuedRuns::incrementAndGet));
        awaitStatus("running", TaskStatus.RUNNING, 5_000);
        log.info("输入: 1 个运行中任务 + 1 个排队任务, grace=2000ms; 判定依据: 两者在排空期内完成 SUCCEEDED, 之后提交被 SCHEDULER_STOPPED 拒绝");
        Thread drainer = new Thread(() -> {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            slow.countDown();
        });
        drainer.setDaemon(true);
        drainer.start();
        long start = System.currentTimeMillis();
        scheduler.shutdownGracefully(2_000);
        long elapsed = System.currentTimeMillis() - start;
        TaskSnapshot running = scheduler.snapshot("running");
        TaskSnapshot queued = scheduler.snapshot("queued");
        log.info("结果: running={} queued={} queuedRuns={} drainElapsedMs={}",
                running.status(), queued.status(), queuedRuns.get(), elapsed);
        assertEquals(TaskStatus.SUCCEEDED, running.status());
        assertEquals(TaskStatus.SUCCEEDED, queued.status(), "排队任务应在排空期内被派发完成");
        assertEquals(1, queuedRuns.get());
        assertAllTerminalAndQuotaClean();
    }

    @Test
    void overdueRunningFinalizedAsTimeoutAndQueuedAsCancelled() throws Exception {
        AtomicInteger runningCalls = new AtomicInteger();
        AtomicInteger queuedRuns = new AtomicInteger();
        scheduler.submit(TaskDefinition.of("overdue", "q1", Set.of(), 1, 0, 0, () -> {
            runningCalls.incrementAndGet();
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        scheduler.submit(TaskDefinition.of("waiting", "q1", Set.of(), 1, 0, 0,
                queuedRuns::incrementAndGet));
        awaitStatus("overdue", TaskStatus.RUNNING, 5_000);
        awaitStatus("waiting", TaskStatus.QUEUED, 5_000);
        log.info("输入: overdue 运行需30s, waiting 排队, grace=200ms; 判定依据: overdue -> FAILED/TIMEOUT, waiting -> CANCELLED/SCHEDULER_SHUTDOWN, 排空耗时应远小于30s");
        long start = System.currentTimeMillis();
        scheduler.shutdownGracefully(200);
        long elapsed = System.currentTimeMillis() - start;
        TaskSnapshot overdue = scheduler.snapshot("overdue");
        TaskSnapshot waiting = scheduler.snapshot("waiting");
        log.info("结果: overdue={}/{} waiting={}/{} queuedRuns={} drainElapsedMs={}",
                overdue.status(), overdue.reason(), waiting.status(), waiting.reason(),
                queuedRuns.get(), elapsed);
        assertEquals(TaskStatus.FAILED, overdue.status());
        assertEquals(FailureReason.TIMEOUT, overdue.reason(), "到点未结束的运行任务按超时收尾");
        assertEquals(TaskStatus.CANCELLED, waiting.status());
        assertEquals(FailureReason.SCHEDULER_SHUTDOWN, waiting.reason(), "未运行的排队任务按停止取消");
        assertEquals(0, queuedRuns.get());
        assertTrue(elapsed < 10_000, "排空必须在截止后及时返回, elapsed=" + elapsed);
        assertAllTerminalAndQuotaClean();
    }

    @Test
    void submitAfterShutdownIsRejectedExplicitly() throws Exception {
        scheduler.shutdownGracefully(100);
        log.info("输入: 调度器已停止后提交单个与批量任务; 判定依据: 全部拒绝且 reason=SCHEDULER_STOPPED, 不静默丢弃");
        SubmitResult single = scheduler.submit(TaskDefinition.of("late", "q", Set.of(), 1, 0, 0,
                () -> { }));
        List<SubmitResult> batch = scheduler.submitBatch(List.of(
                TaskDefinition.of("late-b1", "q", Set.of(), 1, 0, 0, () -> { }),
                TaskDefinition.of("late-b2", "q", Set.of(), 1, 0, 0, () -> { })));
        log.info("结果: single=({},{}) batch={}", single.accepted(), single.reason(),
                batch.stream().map(r -> r.accepted() + "/" + r.reason()).toList());
        assertFalse(single.accepted());
        assertEquals(FailureReason.SCHEDULER_STOPPED, single.reason());
        assertFalse(single.detail().isEmpty(), "拒绝必须携带可读的说明");
        for (SubmitResult r : batch) {
            assertFalse(r.accepted());
            assertEquals(FailureReason.SCHEDULER_STOPPED, r.reason());
        }
        assertFalse(scheduler.isAccepting());
    }

    @Test
    void timeoutCancelRetryInterleavedDuringDrain() throws Exception {
        AtomicInteger timeoutCalls = new AtomicInteger();
        AtomicInteger flakyCalls = new AtomicInteger();
        AtomicInteger victimRuns = new AtomicInteger();
        // A: 每次睡眠5s, timeout=300ms, maxRetries=1 -> 两次超时后 FAILED/TIMEOUT（并发=1，占住唯一槽位）
        scheduler.submit(TaskDefinition.of("a-timeout", "q1", Set.of(), 1, 1, 300, () -> {
            timeoutCalls.incrementAndGet();
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted");
            }
        }));
        // B: 排队中（槽位被 A 占用），排空开始后被用户取消
        scheduler.submit(TaskDefinition.of("b-victim", "q1", Set.of(), 1, 0, 0,
                victimRuns::incrementAndGet));
        // C: 第1次失败, 重试成功 -> SUCCEEDED attempts=2
        scheduler.submit(TaskDefinition.of("c-flaky", "q1", Set.of(), 1, 1, 0, () -> {
            if (flakyCalls.incrementAndGet() == 1) {
                throw new IllegalStateException("first attempt fails");
            }
        }));
        awaitStatus("a-timeout", TaskStatus.RUNNING, 5_000);
        awaitStatus("b-victim", TaskStatus.QUEUED, 5_000);
        log.info("输入: grace=2000ms 排空; A=超时重试1次, B=排空中被用户取消, C=首次失败重试成功; 判定依据: A=FAILED/TIMEOUT attempts=2, B=CANCELLED/CANCELLED_BY_USER 且未执行, C=SUCCEEDED attempts=2");
        Thread drainer = new Thread(() -> {
            scheduler.shutdownGracefully(2_000);
        });
        drainer.start();
        // A 的首次尝试会占住槽位约 300ms，此时取消 B 一定仍在排队
        assertTrue(scheduler.cancel("b-victim"), "排空期间取消排队任务应生效");
        drainer.join(10_000);
        assertFalse(drainer.isAlive(), "排空必须在宽限期内返回");

        TaskSnapshot a = scheduler.snapshot("a-timeout");
        TaskSnapshot b = scheduler.snapshot("b-victim");
        TaskSnapshot c = scheduler.snapshot("c-flaky");
        log.info("结果: A={}/{} attempts={} calls={} | B={}/{} runs={} | C={}/{} attempts={} calls={}",
                a.status(), a.reason(), a.attempts(), timeoutCalls.get(),
                b.status(), b.reason(), victimRuns.get(),
                c.status(), c.reason(), c.attempts(), flakyCalls.get());
        assertEquals(TaskStatus.FAILED, a.status());
        assertEquals(FailureReason.TIMEOUT, a.reason());
        assertEquals(2, a.attempts(), "排空期内超时仍按原规则重试");
        assertEquals(2, timeoutCalls.get(), "每次尝试恰好执行一次");
        assertEquals(TaskStatus.CANCELLED, b.status());
        assertEquals(FailureReason.CANCELLED_BY_USER, b.reason(), "用户取消与停止收尾必须可区分");
        assertEquals(0, victimRuns.get());
        assertEquals(TaskStatus.SUCCEEDED, c.status());
        assertEquals(2, c.attempts(), "排空期内失败重试仍按原规则进行");
        assertAllTerminalAndQuotaClean();
    }

    @Test
    void gracefulShutdownIsIdempotent() throws Exception {
        scheduler.submit(TaskDefinition.of("quick", "q", Set.of(), 1, 0, 0, () -> { }));
        assertTrue(scheduler.awaitTerminal(5_000));
        log.info("输入: 重复调用 shutdownGracefully; 判定依据: 两次都正常返回, 状态不变");
        scheduler.shutdownGracefully(500);
        TaskSnapshot before = scheduler.snapshot("quick");
        scheduler.shutdownGracefully(500);
        TaskSnapshot after = scheduler.snapshot("quick");
        log.info("结果: before={} after={}", before.status(), after.status());
        assertEquals(TaskStatus.SUCCEEDED, before.status());
        assertEquals(before.status(), after.status());
        assertAllTerminalAndQuotaClean();
    }
}
