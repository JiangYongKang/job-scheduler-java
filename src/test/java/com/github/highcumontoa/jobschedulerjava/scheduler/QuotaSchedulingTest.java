package com.github.highcumontoa.jobschedulerjava.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.QueueQuota;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 队列并发/资源配额边界：超配排队 vs 直接拒绝可区分；任何时刻不得超发；失败后资源回收。
 */
class QuotaSchedulingTest {

    private static final Logger log = LoggerFactory.getLogger(QuotaSchedulingTest.class);

    private LocalJobScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void concurrencyCapTasksThirdQueuedThenRunsAfterRelease() throws Exception {
        scheduler = new LocalJobScheduler();
        scheduler.registerQueue(new QueueQuota("q", 2, 100, Long.MAX_VALUE, -1));

        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();

        for (int i = 1; i <= 3; i++) {
            final int id = i;
            scheduler.submit(TaskDefinition.builder("t" + id).queue("q").body(ctx -> {
                int now = inFlight.incrementAndGet();
                maxInFlight.accumulateAndGet(now, Math::max);
                entered.countDown();
                release.await(5, TimeUnit.SECONDS);
                inFlight.decrementAndGet();
            }).build());
        }

        log.info("[输入] 并发上限=2，提交 t1,t2,t3（任务体阻塞至放行）");
        assertTrue(entered.await(2, TimeUnit.SECONDS), "两个任务应获得配额开始执行");
        TaskSnapshot third = scheduler.snapshot("t3");
        log.info("[判定] t3={}（期望 QUEUED），观测峰值并发={}（期望<=2）", third, maxInFlight.get());
        assertEquals(TaskStatus.QUEUED, third.getStatus());
        assertEquals(2, maxInFlight.get(), "任何时刻都不得超发并发配额");

        release.countDown();
        assertTrue(scheduler.awaitQuiet(5000));
        log.info("[判定] 放行后全部终态: t1={} t2={} t3={} 峰值并发={}",
                scheduler.snapshot("t1").getStatus(), scheduler.snapshot("t2").getStatus(),
                scheduler.snapshot("t3").getStatus(), maxInFlight.get());
        assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("t3").getStatus());
        assertEquals(2, maxInFlight.get());
    }

    @Test
    void cpuQuotaBoundaryIsRespectedAndRecoveredAfterFailure() throws Exception {
        scheduler = new LocalJobScheduler();
        // cpuLimit=3：t1 占 2，t2 需 2 放不下 -> 排队；t1 失败释放后 t2 才能上。
        scheduler.registerQueue(new QueueQuota("q", 10, 3, Long.MAX_VALUE, -1));
        CountDownLatch t1Entered = new CountDownLatch(1);
        CountDownLatch failNow = new CountDownLatch(1);

        scheduler.submit(TaskDefinition.builder("t1").queue("q").cpuUnits(2).body(ctx -> {
            t1Entered.countDown();
            failNow.await(5, TimeUnit.SECONDS);
            throw new IllegalStateException("boom");
        }).build());
        scheduler.submit(TaskDefinition.builder("t2").queue("q").cpuUnits(2).body(ctx -> {
        }).build());

        log.info("[输入] cpuLimit=3，t1(2cpu) 将失败，t2(2cpu) 应等待资源回收");
        assertTrue(t1Entered.await(2, TimeUnit.SECONDS));
        TaskSnapshot waiting = scheduler.snapshot("t2");
        log.info("[判定] t1 运行中 t2={}（期望 QUEUED，2+2>3 不得超发）", waiting);
        assertEquals(TaskStatus.QUEUED, waiting.getStatus());

        failNow.countDown();
        TaskSnapshot t1 = scheduler.awaitTerminal("t1", 5000);
        TaskSnapshot t2 = scheduler.awaitTerminal("t2", 5000);
        log.info("[判定] t1={}/{} t2={}/{}（失败后资源回收，t2 获得配额）",
                t1.getStatus(), t1.getFailureReason(), t2.getStatus(), t2.getFailureReason());
        assertEquals(TaskStatus.FAILED, t1.getStatus());
        assertEquals(FailureReason.RETRIES_EXHAUSTED, t1.getFailureReason());
        assertEquals(TaskStatus.SUCCEEDED, t2.getStatus());
    }

    @Test
    void rejectWhenBusyIsDistinguishableFromQueued() throws Exception {
        scheduler = new LocalJobScheduler();
        scheduler.registerQueue(new QueueQuota("q", 1, 100, Long.MAX_VALUE, -1));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        scheduler.submit(TaskDefinition.builder("blocker").queue("q").body(ctx -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
        }).build());
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        scheduler.submit(TaskDefinition.builder("waiter").queue("q").build());
        scheduler.submit(TaskDefinition.builder("rejecter").queue("q").rejectWhenBusy(true).build());
        log.info("[输入] 配额占满时分别提交排队型 waiter 与拒绝型 rejecter");

        TaskSnapshot waiter = scheduler.snapshot("waiter");
        TaskSnapshot rejecter = scheduler.snapshot("rejecter");
        log.info("[判定] waiter={}（期望 QUEUED） rejecter={}/{}（期望 REJECTED/QUOTA_REJECTED）",
                waiter.getStatus(), rejecter.getStatus(), rejecter.getFailureReason());
        assertEquals(TaskStatus.QUEUED, waiter.getStatus());
        assertEquals(TaskStatus.REJECTED, rejecter.getStatus());
        assertEquals(FailureReason.QUOTA_REJECTED, rejecter.getFailureReason());
        assertTrue(rejecter.getReasonDetail().contains("REJECT"), "拒绝原因需可追溯: " + rejecter.getReasonDetail());

        release.countDown();
        scheduler.awaitTerminal("waiter", 5000);
        log.info("[判定] 释放后 waiter={}，rejecter 保持终态={}",
                scheduler.snapshot("waiter").getStatus(), scheduler.snapshot("rejecter").getStatus());
        assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("waiter").getStatus());
        assertEquals(TaskStatus.REJECTED, scheduler.snapshot("rejecter").getStatus());
    }

    @Test
    void fullWaitingQueueIsRejected() throws Exception {
        scheduler = new LocalJobScheduler();
        // 并发1、排队容量1：第1个运行，第2个排队，第3个即使是排队策略也因容量满被拒绝。
        scheduler.registerQueue(new QueueQuota("q", 1, 100, Long.MAX_VALUE, 1));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.builder("t1").queue("q").body(ctx -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
        }).build());
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        scheduler.submit(TaskDefinition.builder("t2").queue("q").build());
        scheduler.submit(TaskDefinition.builder("t3").queue("q").build());
        log.info("[输入] maxConcurrency=1,maxQueued=1，连续提交 t1,t2,t3");

        TaskSnapshot t2 = scheduler.snapshot("t2");
        TaskSnapshot t3 = scheduler.snapshot("t3");
        log.info("[判定] t2={} t3={}/{} detail={}", t2.getStatus(), t3.getStatus(),
                t3.getFailureReason(), t3.getReasonDetail());
        assertEquals(TaskStatus.QUEUED, t2.getStatus());
        assertEquals(TaskStatus.REJECTED, t3.getStatus());
        assertEquals(FailureReason.QUOTA_REJECTED, t3.getFailureReason());
        release.countDown();
        scheduler.shutdown();
    }

    @Test
    void noOvercommitUnderConcurrentSubmission() throws Exception {
        scheduler = new LocalJobScheduler();
        scheduler.registerQueue(new QueueQuota("q", 2, 4, Long.MAX_VALUE, -1));
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        AtomicInteger maxCpu = new AtomicInteger();
        AtomicInteger cpuNow = new AtomicInteger();

        int n = 12;
        CountDownLatch done = new CountDownLatch(n);
        List<Thread> submitters = new java.util.ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final int base = i * (n / 4);
            Thread th = new Thread(() -> {
                for (int j = 0; j < n / 4; j++) {
                    int idx = base + j + 1;
                    scheduler.submit(TaskDefinition.builder("c" + idx).queue("q").cpuUnits(2).body(ctx -> {
                        int f = inFlight.incrementAndGet();
                        maxInFlight.accumulateAndGet(f, Math::max);
                        int c = cpuNow.addAndGet(2);
                        maxCpu.accumulateAndGet(c, Math::max);
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        cpuNow.addAndGet(-2);
                        inFlight.decrementAndGet();
                        done.countDown();
                    }).build());
                }
            });
            submitters.add(th);
            th.start();
        }
        log.info("[输入] 4 线程并发提交 {} 个 2cpu 任务（cpuLimit=4, maxConcurrency=2）", n);
        for (Thread t : submitters) {
            t.join(5000);
        }
        assertTrue(done.await(10, TimeUnit.SECONDS), "全部任务应完成");
        log.info("[判定] 峰值并发={}（<=2） 峰值cpu={}（<=4），无超发", maxInFlight.get(), maxCpu.get());
        assertTrue(maxInFlight.get() <= 2, "并发超发: " + maxInFlight.get());
        assertTrue(maxCpu.get() <= 4, "cpu 超发: " + maxCpu.get());
        for (int idx = 1; idx <= n; idx++) {
            assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("c" + idx).getStatus());
        }
    }
}
