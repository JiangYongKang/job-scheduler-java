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

import java.util.ArrayList;
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
 * 队列暂停/恢复：暂停期间不派发新任务，运行中任务自然结束；
 * 恢复后排队任务按原顺序继续调度；暂停期间提交/取消/重试语义不变。
 */
class QueuePauseResumeTest {

    private static final Logger log = LoggerFactory.getLogger(QueuePauseResumeTest.class);

    private LocalJobScheduler scheduler;
    private QuotaManager quota;

    @BeforeEach
    void setUp() {
        quota = new QuotaManager();
        quota.configureQueue("q", 1, 10, false); // 并发 1，便于观察顺序
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    void pausedQueueHoldsQueuedTasksAndResumePreservesOrder() throws Exception {
        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> executionOrder = new ConcurrentLinkedQueue<>();

        scheduler.submit(TaskDefinition.of("blocker", "q", Set.of(), 1, 0, 0, () -> {
            blockerEntered.countDown();
            releaseBlocker.await(5, TimeUnit.SECONDS);
        }));
        assertTrue(blockerEntered.await(5, TimeUnit.SECONDS));

        // 排队 4 个任务后暂停队列
        for (int i = 0; i < 4; i++) {
            final String id = "t" + i;
            scheduler.submit(TaskDefinition.of(id, "q", Set.of(), 1, 0, 0,
                    () -> executionOrder.add(id)));
        }
        scheduler.pauseQueue("q");
        log.info("输入: 并发=1 队列, blocker 占用中, t0..t3 排队后暂停队列; "
                + "判定依据: 释放 blocker 后暂停期间 t0..t3 均保持 QUEUED 且不执行");

        releaseBlocker.countDown();
        // 等 blocker 自然结束，再给调度器若干轮机会，确认暂停期间不派发
        long blockerDeadline = System.currentTimeMillis() + 5_000;
        while (scheduler.snapshot("blocker").status() != TaskStatus.SUCCEEDED
                && System.currentTimeMillis() < blockerDeadline) {
            Thread.sleep(10);
        }
        Thread.sleep(150);
        log.info("结果(暂停中): blocker={} t0={} t3={} executed={}",
                scheduler.snapshot("blocker").status(), scheduler.snapshot("t0").status(),
                scheduler.snapshot("t3").status(), executionOrder);
        assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("blocker").status(),
                "运行中任务应在暂停期间自然结束");
        for (int i = 0; i < 4; i++) {
            assertEquals(TaskStatus.QUEUED, scheduler.snapshot("t" + i).status(),
                    "暂停期间排队任务不得被派发");
        }
        assertTrue(executionOrder.isEmpty(), "暂停期间不得执行任何排队任务");

        scheduler.resumeQueue("q");
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("判定依据: 恢复后按暂停前排队顺序执行 t0,t1,t2,t3; 结果: order={}", executionOrder);
        assertEquals(List.of("t0", "t1", "t2", "t3"), new ArrayList<>(executionOrder),
                "恢复后应保持暂停前的排队顺序");
        assertTrue(scheduler.snapshots().stream().allMatch(s -> s.status() == TaskStatus.SUCCEEDED));
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running());
        assertEquals(0, usage.usedResource());
    }

    @Test
    void submitCancelRetryAndTimeoutStayConsistentDuringPause() throws Exception {
        // 暂停期间提交：正常受理但不派发
        scheduler.pauseQueue("q");
        SubmitResult submitted = scheduler.submit(
                TaskDefinition.of("submitted-during-pause", "q", Set.of(), 1, 0, 0, () -> { }));
        log.info("输入: 暂停期间提交任务; 判定依据: 受理成功且停留 QUEUED; 结果: accepted={} status={}",
                submitted.accepted(), scheduler.snapshot("submitted-during-pause").status());
        assertTrue(submitted.accepted());
        Thread.sleep(100);
        assertEquals(TaskStatus.QUEUED, scheduler.snapshot("submitted-during-pause").status());

        // 暂停期间取消排队任务：与正常情况一致
        assertTrue(scheduler.cancel("submitted-during-pause"));
        TaskSnapshot cancelled = scheduler.snapshot("submitted-during-pause");
        log.info("输入: 暂停期间取消排队任务; 判定依据: CANCELLED/CANCELLED_BY_USER; 结果: {} {}",
                cancelled.status(), cancelled.reason());
        assertEquals(TaskStatus.CANCELLED, cancelled.status());
        assertEquals(FailureReason.CANCELLED_BY_USER, cancelled.reason());
        assertFalse(scheduler.cancel("submitted-during-pause"), "取消应幂等");

        // 暂停期间运行中任务超时：按原规则释放配额并重新排队，但因暂停不被派发
        scheduler.resumeQueue("q");
        AtomicInteger attempts = new AtomicInteger();
        scheduler.submit(TaskDefinition.of("timeout-task", "q", Set.of(), 1, 1, 80, () -> {
            attempts.incrementAndGet();
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        // 等第一次尝试进入 RUNNING 后暂停队列
        long deadline = System.currentTimeMillis() + 5_000;
        while (scheduler.snapshot("timeout-task").status() != TaskStatus.RUNNING
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        scheduler.pauseQueue("q");
        log.info("输入: 运行中任务 timeout=80ms maxRetries=1, 运行中暂停队列; "
                + "判定依据: 超时后重新排队但暂停期间不派发, 恢复后重试并最终 TIMEOUT 失败");
        Thread.sleep(300); // 超时发生并重新排队
        TaskSnapshot duringPause = scheduler.snapshot("timeout-task");
        log.info("结果(暂停中): status={} attempts={}", duringPause.status(), duringPause.attempts());
        assertEquals(TaskStatus.QUEUED, duringPause.status(),
                "超时重试应重新排队且暂停期间不被派发");
        assertEquals(1, attempts.get(), "暂停期间不得发生第二次执行");

        scheduler.resumeQueue("q");
        assertTrue(scheduler.awaitTerminal(10_000));
        TaskSnapshot done = scheduler.snapshot("timeout-task");
        log.info("结果(恢复后): status={} reason={} attempts={}", done.status(), done.reason(), done.attempts());
        assertEquals(TaskStatus.FAILED, done.status());
        assertEquals(FailureReason.TIMEOUT, done.reason());
        assertEquals(2, done.attempts(), "恢复后应恰好重试一次");
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running(), "超时释放的配额不得漏回收或重复回收");
        assertEquals(0, usage.usedResource());
    }

    @Test
    void concurrentSubmissionDuringPauseNeverDuplicatesNorOversells() throws Exception {
        scheduler.pauseQueue("q");
        int taskCount = 30;
        int threads = 6;
        AtomicInteger totalExecutions = new AtomicInteger();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        List<Future<SubmitResult>> futures = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            final String id = "p" + i;
            futures.add(pool.submit(() -> {
                startGun.await();
                return scheduler.submit(TaskDefinition.of(id, "q", Set.of(), 1, 0, 0, () -> {
                    totalExecutions.incrementAndGet();
                    int now = inFlight.incrementAndGet();
                    maxInFlight.accumulateAndGet(now, Math::max);
                    Thread.sleep(5);
                    inFlight.decrementAndGet();
                }));
            }));
        }
        log.info("输入: 暂停期间 {} 个线程并发提交 {} 个任务; 判定依据: 全部受理且暂停期间无一执行",
                threads, taskCount);
        startGun.countDown();
        for (Future<SubmitResult> f : futures) {
            assertTrue(f.get(10, TimeUnit.SECONDS).accepted(), "暂停期间提交应正常受理");
        }
        pool.shutdown();
        Thread.sleep(150);
        log.info("结果(暂停中): totalExecutions={} (期望 0)", totalExecutions.get());
        assertEquals(0, totalExecutions.get(), "暂停期间不得派发任何任务");

        scheduler.resumeQueue("q");
        assertTrue(scheduler.awaitTerminal(15_000));
        log.info("判定依据: 恢复后每个任务恰好执行一次且并发<=1; 结果: totalExecutions={} maxInFlight={}",
                totalExecutions.get(), maxInFlight.get());
        assertEquals(taskCount, totalExecutions.get(), "每个任务必须恰好执行一次");
        assertTrue(maxInFlight.get() <= 1, "并发不得超过队列上限");
        assertTrue(scheduler.snapshots().stream().allMatch(s -> s.status() == TaskStatus.SUCCEEDED));
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running());
        assertEquals(0, usage.usedResource());
    }
}
