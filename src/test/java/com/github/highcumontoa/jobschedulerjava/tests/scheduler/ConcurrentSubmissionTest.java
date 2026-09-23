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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * 并发提交：配额不超发、同一任务不重复执行、重复提交被拒绝、取消幂等。
 */
class ConcurrentSubmissionTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmissionTest.class);

    private LocalJobScheduler scheduler;
    private QuotaManager quota;

    @BeforeEach
    void setUp() {
        quota = new QuotaManager();
        quota.configureQueue("q", 3, 6, false); // 并发 3、资源 6
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    void concurrentSubmissionNeverExceedsQuotaAndNeverDuplicatesExecution() throws Exception {
        int taskCount = 40;
        int submitThreads = 8;
        Map<String, AtomicInteger> executions = new ConcurrentHashMap<>();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        AtomicInteger maxResource = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(submitThreads);
        CountDownLatch startGun = new CountDownLatch(1);
        List<Future<SubmitResult>> futures = new ArrayList<>();
        // 输入：8 个线程并发提交 40 个 cost=1 的任务到并发=3 的队列
        for (int i = 0; i < taskCount; i++) {
            final String id = "c" + i;
            futures.add(pool.submit(() -> {
                startGun.await();
                return scheduler.submit(TaskDefinition.of(id, "q", Set.of(), 1, 0, 0, () -> {
                    executions.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
                    int now = inFlight.incrementAndGet();
                    maxInFlight.accumulateAndGet(now, Math::max);
                    QueueUsage usage = quota.snapshot().get(0);
                    maxResource.accumulateAndGet(usage.usedResource(), Math::max);
                    Thread.sleep(5);
                    inFlight.decrementAndGet();
                }));
            }));
        }
        log.info("输入: {}个线程并发提交{}个任务, 队列并发=3 资源=6; 判定依据: 全部受理, 每个任务恰好执行1次, "
                + "maxInFlight<=3, usedResource<=6", submitThreads, taskCount);
        startGun.countDown();
        for (Future<SubmitResult> f : futures) {
            SubmitResult result = f.get(10, TimeUnit.SECONDS);
            assertTrue(result.accepted(), "并发提交应全部受理: " + result);
        }
        pool.shutdown();

        assertTrue(scheduler.awaitTerminal(30_000));
        int totalExecutions = executions.values().stream().mapToInt(AtomicInteger::get).sum();
        long duplicated = executions.values().stream().filter(c -> c.get() != 1).count();
        log.info("结果: totalExecutions={} duplicatedTasks={} maxInFlight={} maxResourceObserved={} succeeded={}",
                totalExecutions, duplicated, maxInFlight.get(), maxResource.get(),
                scheduler.snapshots().stream().filter(s -> s.status() == TaskStatus.SUCCEEDED).count());

        assertEquals(taskCount, totalExecutions, "每个任务必须恰好执行一次");
        assertEquals(0, duplicated, "不得重复执行同一任务");
        assertTrue(maxInFlight.get() <= 3, "并发不得超过队列上限");
        assertTrue(maxResource.get() <= 6, "资源不得超过队列上限");
        assertTrue(scheduler.snapshots().stream().allMatch(s -> s.status() == TaskStatus.SUCCEEDED));
        QueueUsage usage = quota.snapshot().get(0);
        assertEquals(0, usage.running());
        assertEquals(0, usage.usedResource());
    }

    @Test
    void concurrentDuplicateSubmissionIsRejectedExactlyOnce() throws Exception {
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        List<Future<SubmitResult>> futures = new ArrayList<>();
        // 输入：16 个线程同时提交同一个 taskId
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                startGun.await();
                return scheduler.submit(TaskDefinition.of("same", "q", Set.of(), 1, 0, 0, () -> { }));
            }));
        }
        log.info("输入: {}个线程并发提交同一taskId=same; 判定依据: 恰好1个受理, 其余{}个拒绝且原因=DUPLICATE_TASK",
                threads, threads - 1);
        startGun.countDown();
        List<SubmitResult> results = new ArrayList<>();
        for (Future<SubmitResult> f : futures) {
            results.add(f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();
        long accepted = results.stream().filter(SubmitResult::accepted).count();
        long rejectedDup = results.stream()
                .filter(r -> !r.accepted() && r.reason() == FailureReason.DUPLICATE_TASK).count();
        log.info("结果: accepted={} rejectedAsDuplicate={}", accepted, rejectedDup);
        assertEquals(1, accepted);
        assertEquals(threads - 1, rejectedDup);

        assertTrue(scheduler.awaitTerminal(10_000));
        assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("same").status());
    }

    @Test
    void cancelIsIdempotentAndDoesNotOversellQuota() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.of("running", "q", Set.of(), 1, 0, 0, () -> {
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        // 并发重复取消同一任务
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                startGun.await();
                return scheduler.cancel("running");
            }));
        }
        log.info("输入: {}个线程并发取消同一运行中任务; 判定依据: 恰好1个返回true, 其余幂等返回false, "
                + "状态=CANCELLED/CANCELLED_BY_USER, 配额恰好释放一次", threads);
        startGun.countDown();
        int trueCount = 0;
        for (Future<Boolean> f : futures) {
            if (f.get(10, TimeUnit.SECONDS)) {
                trueCount++;
            }
        }
        pool.shutdown();
        release.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));

        TaskSnapshot snap = scheduler.snapshot("running");
        QueueUsage usage = quota.snapshot().get(0);
        log.info("结果: cancelReturnedTrue={} status={} reason={} usage={}",
                trueCount, snap.status(), snap.reason(), usage);
        assertEquals(1, trueCount, "取消幂等：仅一次生效");
        assertEquals(TaskStatus.CANCELLED, snap.status());
        assertEquals(FailureReason.CANCELLED_BY_USER, snap.reason());
        assertEquals(0, usage.running(), "配额必须恰好释放一次");
        assertEquals(0, usage.usedResource());

        // 重复取消终态任务：幂等返回 false，无额外效果
        assertFalse(scheduler.cancel("running"));
        assertEquals(TaskStatus.CANCELLED, scheduler.snapshot("running").status());
    }
}
