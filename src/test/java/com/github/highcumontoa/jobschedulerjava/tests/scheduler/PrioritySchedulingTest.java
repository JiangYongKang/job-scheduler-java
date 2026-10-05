package com.github.highcumontoa.jobschedulerjava.tests.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.core.LocalJobScheduler;
import com.github.highcumontoa.jobschedulerjava.scheduler.dag.DependencyValidator;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;
import com.github.highcumontoa.jobschedulerjava.scheduler.quota.QuotaManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 优先级排队与等待老化：同队列内高优先级先调度；等待够久的低优先级任务
 * 通过老化加成获得提升，不会被无限期压住；优先级不突破配额与拒绝语义。
 */
class PrioritySchedulingTest {

    private static final Logger log = LoggerFactory.getLogger(PrioritySchedulingTest.class);

    private LocalJobScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void higherPriorityRunsFirstWhenSlotFrees() throws Exception {
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("q", 1, 10, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.setAgingIntervalMillis(0); // 关闭老化，纯优先级判定
        scheduler.start();

        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> executionOrder = new ConcurrentLinkedQueue<>();

        scheduler.submit(TaskDefinition.of("blocker", "q", Set.of(), 1, 0, 0, () -> {
            blockerEntered.countDown();
            releaseBlocker.await(5, TimeUnit.SECONDS);
        }));
        assertTrue(blockerEntered.await(5, TimeUnit.SECONDS));

        // 依次提交低、中、高优先级任务（提交顺序与优先级相反，排除提交顺序干扰）
        scheduler.submit(TaskDefinition.of("low", "q", Set.of(), 1, 0, 0, 0,
                () -> executionOrder.add("low")));
        scheduler.submit(TaskDefinition.of("mid", "q", Set.of(), 1, 0, 0, 2,
                () -> executionOrder.add("mid")));
        scheduler.submit(TaskDefinition.of("high", "q", Set.of(), 1, 0, 0, 5,
                () -> executionOrder.add("high")));
        Thread.sleep(100); // 确保三个任务都已进入 QUEUED
        log.info("输入: 并发=1, blocker 占用中, 排队 low(p0) mid(p2) high(p5), 老化关闭; "
                + "判定依据: 释放后执行顺序为 high,mid,low");

        releaseBlocker.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: executionOrder={}", executionOrder);
        assertEquals(List.of("high", "mid", "low"), new ArrayList<>(executionOrder),
                "同队列内应按优先级降序执行");
        assertTrue(scheduler.snapshots().stream().allMatch(s -> s.status() == TaskStatus.SUCCEEDED));
    }

    @Test
    void agingPromotesLongWaitingLowPriorityTask() throws Exception {
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("q", 1, 10, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.setAgingIntervalMillis(50); // 每等待 50ms 有效优先级 +1
        scheduler.start();

        CountDownLatch blockerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> executionOrder = new ConcurrentLinkedQueue<>();

        scheduler.submit(TaskDefinition.of("blocker", "q", Set.of(), 1, 0, 0, () -> {
            blockerEntered.countDown();
            releaseBlocker.await(5, TimeUnit.SECONDS);
        }));
        assertTrue(blockerEntered.await(5, TimeUnit.SECONDS));

        // 低优先级任务先排队并充分等待：等待 300ms => 老化加成约 6
        scheduler.submit(TaskDefinition.of("old-low", "q", Set.of(), 1, 0, 0, 0,
                () -> executionOrder.add("old-low")));
        Thread.sleep(300);
        // 后到的中等优先级任务：基础优先级 2，等待极短加成约 0
        scheduler.submit(TaskDefinition.of("newer-mid", "q", Set.of(), 1, 0, 0, 2,
                () -> executionOrder.add("newer-mid")));
        Thread.sleep(30);
        log.info("输入: 老化间隔=50ms, old-low(p0) 已等待约300ms(加成约6), newer-mid(p2) 刚到; "
                + "判定依据: 老化后 old-low 有效优先级(约6) > newer-mid(约2), old-low 先执行");

        releaseBlocker.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: executionOrder={}", executionOrder);
        assertEquals("old-low", executionOrder.peek(),
                "等待够久的低优先级任务应被老化提升，先于后到的高优先级任务执行");
        assertTrue(scheduler.snapshots().stream().allMatch(s -> s.status() == TaskStatus.SUCCEEDED));
    }

    @Test
    void priorityNeverExceedsQuotaAndRejectPolicyStillApplies() throws Exception {
        // 拒绝型队列：高优先级任务在配额满时仍按原规则被拒绝
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("qr", 1, 10, true);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.setAgingIntervalMillis(0);
        scheduler.start();

        CountDownLatch holderEntered = new CountDownLatch(1);
        CountDownLatch releaseHolder = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.of("holder", "qr", Set.of(), 1, 0, 0, 0, () -> {
            holderEntered.countDown();
            releaseHolder.await(5, TimeUnit.SECONDS);
        }));
        assertTrue(holderEntered.await(5, TimeUnit.SECONDS));

        scheduler.submit(TaskDefinition.of("vip", "qr", Set.of(), 1, 0, 0, 100, () -> { }));
        log.info("输入: 拒绝型队列并发=1, holder 占用中, 提交 priority=100 的 vip; "
                + "判定依据: 优先级不突破配额限制, vip=FAILED/QUOTA_REJECTED");
        long deadline = System.currentTimeMillis() + 5_000;
        TaskSnapshot vip;
        do {
            vip = scheduler.snapshot("vip");
            Thread.sleep(10);
        } while (vip.status() != TaskStatus.FAILED && System.currentTimeMillis() < deadline);
        log.info("结果: vip status={} reason={}", vip.status(), vip.reason());
        assertEquals(TaskStatus.FAILED, vip.status());
        assertEquals(FailureReason.QUOTA_REJECTED, vip.reason());
        releaseHolder.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        scheduler.shutdown();

        // 排队型队列：高优先级不抢占运行中任务，全程并发不超上限
        QuotaManager quota2 = new QuotaManager();
        quota2.configureQueue("qw", 1, 10, false);
        scheduler = new LocalJobScheduler(quota2, new DependencyValidator());
        scheduler.setAgingIntervalMillis(0);
        scheduler.start();

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        CountDownLatch lowEntered = new CountDownLatch(1);
        CountDownLatch releaseLow = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.of("running-low", "qw", Set.of(), 1, 0, 0, 0, () -> {
            inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(inFlight.get(), Math::max);
            lowEntered.countDown();
            releaseLow.await(5, TimeUnit.SECONDS);
            inFlight.decrementAndGet();
        }));
        assertTrue(lowEntered.await(5, TimeUnit.SECONDS));
        scheduler.submit(TaskDefinition.of("urgent", "qw", Set.of(), 1, 0, 0, 50, () -> {
            inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(inFlight.get(), Math::max);
            inFlight.decrementAndGet();
        }));
        Thread.sleep(100);
        log.info("输入: 排队型队列并发=1, running-low(p0) 运行中, 提交 urgent(p50); "
                + "判定依据: 优先级不抢占运行中任务, urgent 保持 QUEUED");
        TaskSnapshot urgent = scheduler.snapshot("urgent");
        log.info("结果: urgent={} maxInFlight={}", urgent.status(), maxInFlight.get());
        assertEquals(TaskStatus.QUEUED, urgent.status(), "高优先级不得抢占运行中任务");
        releaseLow.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        assertTrue(maxInFlight.get() <= 1, "并发不得超过队列上限");
        assertTrue(scheduler.snapshots().stream().allMatch(s -> s.status() == TaskStatus.SUCCEEDED));
    }
}
