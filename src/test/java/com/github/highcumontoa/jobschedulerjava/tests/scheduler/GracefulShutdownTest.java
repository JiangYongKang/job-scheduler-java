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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 优雅停止/排空：停止后拒绝新任务；运行中任务在宽限期内自然收尾；
 * 超期任务按超时逻辑终结；排队任务以 SHUTDOWN_DRAINED 取消；
 * 返回时无中间态任务、无配额泄漏，各任务终态可区分。
 */
class GracefulShutdownTest {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownTest.class);

    private LocalJobScheduler scheduler;
    private QuotaManager quota;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    private void newScheduler() {
        quota = new QuotaManager();
        quota.configureQueue("q", 2, 10, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
    }

    @Test
    void runningTasksFinishWithinGraceAndSubmitIsRejectedAfterwards() throws Exception {
        newScheduler();
        CountDownLatch quickStarted = new CountDownLatch(1);
        // 运行中任务：80ms 内自然完成
        scheduler.submit(TaskDefinition.of("quick", "q", Set.of(), 1, 0, 0, () -> {
            quickStarted.countDown();
            Thread.sleep(80);
        }));
        // 排队任务：并发被 quick 与 slow 占满时保持 QUEUED
        scheduler.submit(TaskDefinition.of("slow", "q", Set.of(), 1, 0, 0, () -> {
            Thread.sleep(150);
        }));
        scheduler.submit(TaskDefinition.of("queued", "q", Set.of(), 1, 0, 0, () -> { }));
        assertTrue(quickStarted.await(5, TimeUnit.SECONDS));
        Thread.sleep(50); // 让 slow 也进入 RUNNING，queued 保持 QUEUED
        log.info("输入: 并发=2, quick/slow 运行中, queued 排队; 调用 shutdownGracefully(2000); "
                + "判定依据: quick/slow 自然完成 SUCCEEDED, queued=CANCELLED/SHUTDOWN_DRAINED");

        scheduler.shutdownGracefully(2_000);

        TaskSnapshot quick = scheduler.snapshot("quick");
        TaskSnapshot slow = scheduler.snapshot("slow");
        TaskSnapshot queued = scheduler.snapshot("queued");
        log.info("结果: quick={} slow={} queued=({},{})", quick.status(), slow.status(),
                queued.status(), queued.reason());
        assertEquals(TaskStatus.SUCCEEDED, quick.status());
        assertEquals(TaskStatus.SUCCEEDED, slow.status());
        assertEquals(TaskStatus.CANCELLED, queued.status());
        assertEquals(FailureReason.SHUTDOWN_DRAINED, queued.reason());

        // 停止完成后提交：明确拒绝而非静默丢弃
        SubmitResult late = scheduler.submit(
                TaskDefinition.of("late", "q", Set.of(), 1, 0, 0, () -> { }));
        log.info("判定依据: 停止后提交应被拒绝且原因=SCHEDULER_CLOSED; 结果: accepted={} reason={}",
                late.accepted(), late.reason());
        assertFalse(late.accepted());
        assertEquals(FailureReason.SCHEDULER_CLOSED, late.reason());

        assertTrue(scheduler.snapshots().stream().allMatch(s ->
                s.status() == TaskStatus.SUCCEEDED || s.status() == TaskStatus.FAILED
                        || s.status() == TaskStatus.CANCELLED || s.status() == TaskStatus.BLOCKED),
                "停止完成后不得有任务卡在中间状态");
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running(), "停止后不得有配额被占用");
        assertEquals(0, usage.usedResource());
    }

    @Test
    void overrunningTaskIsInterruptedAndFinishedAsTimeout() throws Exception {
        newScheduler();
        CountDownLatch started = new CountDownLatch(2);
        AtomicBoolean interrupted = new AtomicBoolean();
        // 两个长任务占满并发=2 的全部槽位
        for (int i = 1; i <= 2; i++) {
            scheduler.submit(TaskDefinition.of("overrunner" + i, "q", Set.of(), 1, 0, 0, () -> {
                started.countDown();
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                }
            }));
        }
        scheduler.submit(TaskDefinition.of("waiting", "q", Set.of(), 1, 0, 0, () -> { }));
        scheduler.submit(TaskDefinition.of("waiting2", "q", Set.of(), 1, 0, 0, () -> { }));
        assertTrue(started.await(5, TimeUnit.SECONDS));
        log.info("输入: 并发=2, overrunner1/2 长时间运行(60s) 占满槽位, waiting/waiting2 排队; "
                + "shutdownGracefully(300); 判定依据: 宽限到点后运行中任务被中断并 FAILED/TIMEOUT, "
                + "排队任务 CANCELLED/SHUTDOWN_DRAINED");

        long start = System.currentTimeMillis();
        scheduler.shutdownGracefully(300);
        long elapsed = System.currentTimeMillis() - start;

        TaskSnapshot over1 = scheduler.snapshot("overrunner1");
        TaskSnapshot over2 = scheduler.snapshot("overrunner2");
        log.info("结果: overrunner1=({},{}) overrunner2=({},{}) interrupted={} elapsedMs={}",
                over1.status(), over1.reason(), over2.status(), over2.reason(),
                interrupted.get(), elapsed);
        assertEquals(TaskStatus.FAILED, over1.status());
        assertEquals(FailureReason.TIMEOUT, over1.reason());
        assertEquals(TaskStatus.FAILED, over2.status());
        assertEquals(FailureReason.TIMEOUT, over2.reason());
        assertTrue(interrupted.get(), "超期任务的执行线程必须收到中断");
        assertTrue(elapsed >= 250 && elapsed < 5_000, "应在宽限期到点后收尾而非等待任务自然结束");
        assertEquals(TaskStatus.CANCELLED, scheduler.snapshot("waiting").status());
        assertEquals(FailureReason.SHUTDOWN_DRAINED, scheduler.snapshot("waiting").reason());
        assertEquals(TaskStatus.CANCELLED, scheduler.snapshot("waiting2").status());
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running(), "超期任务的配额必须释放");
        assertEquals(0, usage.usedResource());
    }

    @Test
    void drainWithTimeoutCancelAndRetryInterleaved() throws Exception {
        newScheduler();
        CountDownLatch retryStarted = new CountDownLatch(1);
        CountDownLatch timeoutStarted = new CountDownLatch(1);
        AtomicInteger retryCalls = new AtomicInteger();

        // 占满并发=2 的两个运行中任务
        scheduler.submit(TaskDefinition.of("retry-task", "q", Set.of(), 1, 3, 0, () -> {
            retryStarted.countDown();
            retryCalls.incrementAndGet();
            Thread.sleep(30);
            throw new IllegalStateException("keep failing");
        }));
        scheduler.submit(TaskDefinition.of("timeout-task", "q", Set.of(), 1, 1, 120, () -> {
            timeoutStarted.countDown();
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        scheduler.submit(TaskDefinition.of("cancel-me", "q", Set.of(), 1, 0, 0, () -> { }));
        scheduler.submit(TaskDefinition.of("pending-child", "q", Set.of("cancel-me"), 1, 0, 0, () -> { }));
        assertTrue(retryStarted.await(5, TimeUnit.SECONDS));
        assertTrue(timeoutStarted.await(5, TimeUnit.SECONDS));
        log.info("输入: retry-task(maxRetries=3,持续失败) 与 timeout-task(timeout=120ms) 运行中, "
                + "cancel-me 排队, pending-child 等待依赖; 排空宽限=600ms, 排空中用户取消 cancel-me");

        // 后台线程执行排空，主线程在排空期间做取消/提交操作
        Thread drainer = new Thread(() -> scheduler.shutdownGracefully(600), "drainer");
        drainer.start();
        Thread.sleep(100); // 确保已进入排空阶段

        // 排空期间提交：明确拒绝
        SubmitResult duringDrain = scheduler.submit(
                TaskDefinition.of("during-drain", "q", Set.of(), 1, 0, 0, () -> { }));
        log.info("结果(排空中提交): accepted={} reason={}", duringDrain.accepted(), duringDrain.reason());
        assertFalse(duringDrain.accepted());
        assertEquals(FailureReason.SCHEDULER_CLOSED, duringDrain.reason());

        // 排空期间取消排队任务：与正常语义一致
        assertTrue(scheduler.cancel("cancel-me"));
        drainer.join(10_000);
        assertFalse(drainer.isAlive(), "排空必须在宽限期后完成");

        TaskSnapshot retry = scheduler.snapshot("retry-task");
        TaskSnapshot timeout = scheduler.snapshot("timeout-task");
        TaskSnapshot cancelled = scheduler.snapshot("cancel-me");
        TaskSnapshot child = scheduler.snapshot("pending-child");
        log.info("结果: retry-task=({},{},attempts={}) timeout-task=({},{}) cancel-me=({},{}) "
                        + "pending-child=({},{}) retryCalls={}",
                retry.status(), retry.reason(), retry.attempts(),
                timeout.status(), timeout.reason(),
                cancelled.status(), cancelled.reason(),
                child.status(), child.reason(), retryCalls.get());

        // 重试任务：排空阶段不再派发新尝试，失败重排队后以排空取消收尾
        assertEquals(TaskStatus.CANCELLED, retry.status());
        assertEquals(FailureReason.SHUTDOWN_DRAINED, retry.reason());
        assertTrue(retry.attempts() >= 1 && retryCalls.get() == retry.attempts(),
                "每次尝试只执行一次，且排空后不再派发新尝试");
        // 超时任务：宽限期内按原超时规则运转，最终到点按超时收尾或重排队后排空取消
        assertTrue(timeout.status() == TaskStatus.FAILED || timeout.status() == TaskStatus.CANCELLED);
        assertTrue(timeout.reason() == FailureReason.TIMEOUT
                || timeout.reason() == FailureReason.SHUTDOWN_DRAINED);
        // 用户取消：保持用户取消语义，可区分
        assertEquals(TaskStatus.CANCELLED, cancelled.status());
        assertEquals(FailureReason.CANCELLED_BY_USER, cancelled.reason());
        // 依赖被取消的下游：要么先被依赖传播终结(BLOCKED/DEPENDENCY_CANCELLED)，
        // 要么被排空取消(CANCELLED/SHUTDOWN_DRAINED)，均为可区分的终态
        boolean blockedByDependency = child.status() == TaskStatus.BLOCKED
                && child.reason() == FailureReason.DEPENDENCY_CANCELLED;
        boolean drainedCancel = child.status() == TaskStatus.CANCELLED
                && child.reason() == FailureReason.SHUTDOWN_DRAINED;
        assertTrue(blockedByDependency || drainedCancel,
                "下游任务终态必须可区分: " + child.status() + "/" + child.reason());

        // 无中间态、无配额泄漏
        assertTrue(scheduler.snapshots().stream().allMatch(s ->
                s.status() == TaskStatus.SUCCEEDED || s.status() == TaskStatus.FAILED
                        || s.status() == TaskStatus.CANCELLED || s.status() == TaskStatus.BLOCKED));
        QueueUsage usage = quota.snapshot().get(0);
        log.info("判定依据: 排空后配额必须全部回收; 结果: usage={}", usage);
        assertEquals(0, usage.running());
        assertEquals(0, usage.usedResource());
    }
}
