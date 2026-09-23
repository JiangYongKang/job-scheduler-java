package com.github.highcumontoa.jobschedulerjava.tests.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.core.LocalJobScheduler;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 配额边界：并发上限不被突破；排队与拒绝两种结果可区分；任务结束后配额被回收。
 */
class QuotaBoundaryTest {

    private static final Logger log = LoggerFactory.getLogger(QuotaBoundaryTest.class);

    private LocalJobScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void concurrencyLimitIsNeverExceededAndTasksQueue() throws Exception {
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("q", 2, 100, false); // 并发上限 2，排队策略
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);

        // 输入：4 个任务，前 2 个阻塞在栅栏上，后 2 个应处于 QUEUED
        for (int i = 0; i < 4; i++) {
            final boolean blocker = i < 2;
            scheduler.submit(TaskDefinition.of("t" + i, "q", Set.of(), 1, 0, 0, () -> {
                int now = inFlight.incrementAndGet();
                maxObserved.accumulateAndGet(now, Math::max);
                if (blocker) {
                    entered.countDown();
                    release.await(5, TimeUnit.SECONDS);
                }
            }));
        }
        log.info("输入: 队列并发上限=2, 提交4个任务, 前2个阻塞; 判定依据: 在栅栏释放前 inFlight<=2 且后2个任务为 QUEUED");

        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100); // 留给调度器若干轮调度机会
        TaskSnapshot t2 = scheduler.snapshot("t2");
        TaskSnapshot t3 = scheduler.snapshot("t3");
        log.info("判定依据: 配额满时排队策略任务应停留 QUEUED; 结果: t2={} t3={} maxInFlight={}",
                t2.status(), t3.status(), maxObserved.get());
        assertEquals(TaskStatus.QUEUED, t2.status());
        assertEquals(TaskStatus.QUEUED, t3.status());
        assertTrue(maxObserved.get() <= 2, "并发不得超过上限");

        release.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("判定依据: 栅栏释放后全部成功且配额归零; 结果: snapshots={} usage={}",
                scheduler.snapshots(), quota.snapshot());
        assertTrue(scheduler.snapshots().stream().allMatch(s -> s.status() == TaskStatus.SUCCEEDED));
        assertEquals(4, scheduler.snapshots().stream().mapToInt(TaskSnapshot::attempts).sum());
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running());
        assertEquals(0, usage.usedResource());
    }

    @Test
    void rejectPolicyQueueRejectsWhenFullWithDistinctReason() throws Exception {
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("qr", 1, 100, true); // 并发上限 1，拒绝策略
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.of("holder", "qr", Set.of(), 1, 0, 0, () -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
        }));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        scheduler.submit(TaskDefinition.of("rejected", "qr", Set.of(), 1, 0, 0, () -> { }));
        log.info("输入: 拒绝型队列并发=1, holder 占用中再提交 rejected; 判定依据: rejected 应进入 FAILED/QUOTA_REJECTED 而非排队");

        // 等待 rejected 进入终态
        long deadline = System.currentTimeMillis() + 5_000;
        TaskSnapshot snap;
        do {
            snap = scheduler.snapshot("rejected");
            Thread.sleep(10);
        } while (snap.status() != TaskStatus.FAILED && System.currentTimeMillis() < deadline);
        log.info("结果: status={} reason={}", snap.status(), snap.reason());
        assertEquals(TaskStatus.FAILED, snap.status());
        assertEquals(FailureReason.QUOTA_REJECTED, snap.reason());

        release.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        QueueUsage usage = quota.snapshot().get(0);
        log.info("判定依据: holder 结束后配额必须回收; 结果: usage={}", usage);
        assertEquals(0, usage.running());
    }

    @Test
    void resourceQuotaBoundaryIsRespected() throws Exception {
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("qb", 10, 3, false); // 资源上限 3，并发上限 10
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        // 输入：3 个 cost=2 的任务，资源上限 3 —— 任意时刻最多 1 个在运行
        for (int i = 0; i < 3; i++) {
            scheduler.submit(TaskDefinition.of("r" + i, "qb", Set.of(), 2, 0, 0, () -> {
                int now = inFlight.incrementAndGet();
                maxObserved.accumulateAndGet(now, Math::max);
                Thread.sleep(30);
                inFlight.decrementAndGet();
            }));
        }
        log.info("输入: 资源上限=3, 3个cost=2的任务; 判定依据: 任意时刻最多1个运行(2*2>3), 最终全部成功");
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: maxInFlight={} snapshots={}", maxObserved.get(), scheduler.snapshots());
        assertEquals(1, maxObserved.get());
        assertTrue(scheduler.snapshots().stream().allMatch(s -> s.status() == TaskStatus.SUCCEEDED));
    }

    @Test
    void quotaIsReleasedWhenAttemptFails() throws Exception {
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("qf", 1, 10, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();

        scheduler.submit(TaskDefinition.of("fail", "qf", Set.of(), 1, 0, 0, () -> {
            throw new IllegalStateException("boom");
        }));
        log.info("输入: 并发=1 队列, 一个必然失败且不可重试的任务; 判定依据: 失败后配额必须回收, 后续任务可调度");
        assertTrue(scheduler.awaitTerminal(10_000));
        TaskSnapshot failed = scheduler.snapshot("fail");
        log.info("结果: status={} reason={} usage={}", failed.status(), failed.reason(), quota.snapshot());
        assertEquals(TaskStatus.FAILED, failed.status());
        assertEquals(FailureReason.RETRY_EXHAUSTED, failed.reason());
        assertEquals(0, quota.snapshot().get(0).running());

        // 后续任务应能正常调度，证明配额已回收
        scheduler.submit(TaskDefinition.of("after", "qf", Set.of(), 1, 0, 0, () -> { }));
        assertTrue(scheduler.awaitTerminal(10_000));
        assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("after").status());
        log.info("判定依据: 后续任务成功证明配额已回收; 结果: {}", scheduler.snapshot("after"));
    }
}
