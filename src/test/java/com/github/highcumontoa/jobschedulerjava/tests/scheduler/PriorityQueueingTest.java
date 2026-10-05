package com.github.highcumontoa.jobschedulerjava.tests.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.core.LocalJobScheduler;
import com.github.highcumontoa.jobschedulerjava.scheduler.dag.DependencyValidator;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同队列优先级排队与等待老化：空位先给高优先级；等待够久的低优先级任务
 * 经老化提升后不会被无限期压住；优先级不突破并发/资源限制，拒绝型队列照常拒绝。
 */
class PriorityQueueingTest {

    private static final Logger log = LoggerFactory.getLogger(PriorityQueueingTest.class);
    /** 老化间隔：每等待 50ms 等效优先级 +1，便于测试内快速验证 */
    private static final long AGING_INTERVAL_MILLIS = 50L;

    private LocalJobScheduler scheduler;
    private QuotaManager quota;

    @BeforeEach
    void setUp() {
        quota = new QuotaManager();
        quota.configureQueue("q", 1, 16, false);
        quota.configureQueue("q2", 2, 16, false);
        quota.configureQueue("qr", 1, 4, true);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator(), AGING_INTERVAL_MILLIS);
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

    private TaskDefinition blocking(String id, String queue, int priority, CountDownLatch latch) {
        return TaskDefinition.of(id, queue, Set.of(), 1, 0, 0, priority, () -> {
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Test
    void higherPriorityRunsFirstWhenSlotFrees() throws Exception {
        CountDownLatch blockerRelease = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> executionOrder = new ConcurrentLinkedQueue<>();
        // 输入：并发=1；blocker 占位；排队 low(p=0)/mid(p=5)/high(p=10)；放行
        scheduler.submit(blocking("blocker", "q", 0, blockerRelease));
        awaitStatus("blocker", TaskStatus.RUNNING, 5_000);
        scheduler.submit(TaskDefinition.of("low", "q", Set.of(), 1, 0, 0, 0,
                () -> executionOrder.add("low")));
        scheduler.submit(TaskDefinition.of("mid", "q", Set.of(), 1, 0, 0, 5,
                () -> executionOrder.add("mid")));
        scheduler.submit(TaskDefinition.of("high", "q", Set.of(), 1, 0, 0, 10,
                () -> executionOrder.add("high")));
        awaitStatus("high", TaskStatus.QUEUED, 5_000);
        log.info("输入: 并发=1, 排队 low(p=0)/mid(p=5)/high(p=10); 判定依据: 空位释放后执行顺序 high,mid,low");
        blockerRelease.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: executionOrder={}", executionOrder);
        assertEquals(List.of("high", "mid", "low"), List.copyOf(executionOrder),
                "同队列空位必须先给高优先级任务");
        for (String id : List.of("low", "mid", "high")) {
            assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot(id).status());
        }
    }

    @Test
    void agingPromotesLongWaitingLowPriorityTask() throws Exception {
        CountDownLatch blockerRelease = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> executionOrder = new ConcurrentLinkedQueue<>();
        // 输入：并发=1, 老化间隔=50ms；old(p=0) 先排队并等待 ~400ms，再提交 new(p=3)；放行
        scheduler.submit(blocking("blocker", "q", 0, blockerRelease));
        awaitStatus("blocker", TaskStatus.RUNNING, 5_000);
        scheduler.submit(TaskDefinition.of("old", "q", Set.of(), 1, 0, 0, 0,
                () -> executionOrder.add("old")));
        awaitStatus("old", TaskStatus.QUEUED, 5_000);
        Thread.sleep(400); // old 等待 400ms -> 老化提升约 8 级
        scheduler.submit(TaskDefinition.of("new", "q", Set.of(), 1, 0, 0, 3,
                () -> executionOrder.add("new")));
        awaitStatus("new", TaskStatus.QUEUED, 5_000);
        log.info("输入: 老化间隔={}ms, old(p=0) 已等待~400ms(提升~8级), new(p=3) 刚入队; 判定依据: old 等效优先级(~8) > new(~3+少量), 执行顺序 old,new —— 低优先级不被无限期压住",
                AGING_INTERVAL_MILLIS);
        blockerRelease.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: executionOrder={}", executionOrder);
        assertEquals(List.of("old", "new"), List.copyOf(executionOrder),
                "等待够久的低优先级任务必须被老化机制提升，不能被后到的稍高优先级任务无限压住");
    }

    @Test
    void rejectQueueRejectsHighPriorityTaskWhenFull() throws Exception {
        CountDownLatch blockerRelease = new CountDownLatch(1);
        AtomicInteger rejectedRuns = new AtomicInteger();
        // 输入：拒绝型队列 qr 并发=1；blocker 占位；提交高优先级任务
        scheduler.submit(blocking("blocker", "qr", 0, blockerRelease));
        awaitStatus("blocker", TaskStatus.RUNNING, 5_000);
        scheduler.submit(TaskDefinition.of("vip", "qr", Set.of(), 1, 0, 0, 100,
                rejectedRuns::incrementAndGet));
        log.info("输入: 拒绝型队列满, 提交 priority=100 的任务; 判定依据: 仍 FAILED/QUOTA_REJECTED, 优先级不能突破排队位满拒绝规则");
        awaitStatus("vip", TaskStatus.FAILED, 5_000);
        TaskSnapshot snap = scheduler.snapshot("vip");
        log.info("结果: vip={}/{} runs={}", snap.status(), snap.reason(), rejectedRuns.get());
        assertEquals(FailureReason.QUOTA_REJECTED, snap.reason());
        assertEquals(0, rejectedRuns.get(), "被拒绝的任务不得执行");
        blockerRelease.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
    }

    @Test
    void priorityDoesNotExceedConcurrencyLimit() throws Exception {
        int taskCount = 6;
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(taskCount);
        CountDownLatch release = new CountDownLatch(1);
        // 输入：并发=2；6 个任务优先级 0..5 混杂，handler 记录并发峰值
        log.info("输入: 并发=2, {} 个任务优先级 0..5; 判定依据: 运行中并发峰值<=2, 全部 SUCCEEDED", taskCount);
        for (int i = 0; i < taskCount; i++) {
            final int priority = i % 3 == 0 ? 10 : (i % 3 == 1 ? 5 : 0);
            scheduler.submit(TaskDefinition.of("p" + i, "q2", Set.of(), 1, 0, 0, priority, () -> {
                int now = inFlight.incrementAndGet();
                maxInFlight.accumulateAndGet(now, Math::max);
                entered.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                inFlight.decrementAndGet();
            }));
        }
        // 等两个槽位都被占上，确认并发被压在 2，然后放行
        assertTrue(entered.await(5, TimeUnit.SECONDS) || inFlight.get() > 0);
        Thread.sleep(100);
        release.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: maxInFlight={} (并发上限=2)", maxInFlight.get());
        assertTrue(maxInFlight.get() <= 2, "优先级不得突破队列并发限制, maxInFlight=" + maxInFlight.get());
        for (int i = 0; i < taskCount; i++) {
            assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("p" + i).status());
        }
    }

    @Test
    void equalPriorityKeepsFifoOrder() throws Exception {
        CountDownLatch blockerRelease = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> executionOrder = new ConcurrentLinkedQueue<>();
        scheduler.submit(blocking("blocker", "q", 0, blockerRelease));
        awaitStatus("blocker", TaskStatus.RUNNING, 5_000);
        for (String id : List.of("f1", "f2", "f3")) {
            scheduler.submit(TaskDefinition.of(id, "q", Set.of(), 1, 0, 0, 7,
                    () -> executionOrder.add(id)));
            awaitStatus(id, TaskStatus.QUEUED, 5_000);
        }
        log.info("输入: 并发=1, f1/f2/f3 同优先级(7)依次入队; 判定依据: 执行顺序保持 f1,f2,f3 (FIFO)");
        blockerRelease.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: executionOrder={}", executionOrder);
        assertEquals(List.of("f1", "f2", "f3"), List.copyOf(executionOrder),
                "同优先级应保持先入队先执行");
    }
}
