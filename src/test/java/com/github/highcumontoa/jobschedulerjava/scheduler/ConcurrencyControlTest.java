package com.github.highcumontoa.jobschedulerjava.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.QueueQuota;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.SubmitErrorCode;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.SubmitException;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发提交下的一致性：重复 id 仅一个成功、每个任务仅执行一次、配额不超发、
 * 并发取消不会导致资源泄漏或重复结论。
 */
class ConcurrencyControlTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyControlTest.class);

    private LocalJobScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void concurrentDuplicateSubmissionOnlyOneWins() throws Exception {
        scheduler = new LocalJobScheduler();
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicateRejected = new AtomicInteger();
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    scheduler.submit(TaskDefinition.builder("only-one")
                            .body(ctx -> runs.incrementAndGet()).build());
                    accepted.incrementAndGet();
                } catch (SubmitException ex) {
                    if (ex.getErrorCode() == SubmitErrorCode.DUPLICATE_ID) {
                        duplicateRejected.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        log.info("[输入] {} 个线程同时提交同一 id=only-one", threads);
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdown();

        scheduler.awaitTerminal("only-one", 5000);
        TaskSnapshot snap = scheduler.snapshot("only-one");
        log.info("[判定] accepted={} duplicateRejected={} 最终={} 执行次数={}",
                accepted.get(), duplicateRejected.get(), snap.getStatus(), runs.get());
        assertEquals(1, accepted.get(), "同一 id 有且仅有一次提交成功");
        assertEquals(threads - 1, duplicateRejected.get(), "其余必须以 DUPLICATE_ID 拒绝");
        assertEquals(TaskStatus.SUCCEEDED, snap.getStatus());
        assertEquals(1, runs.get(), "重复提交不得产生重复执行");
    }

    @Test
    void concurrentBatchSubmitPreservesQuotas() throws Exception {
        scheduler = new LocalJobScheduler();
        scheduler.registerQueue(new QueueQuota("q", 3, 9, Long.MAX_VALUE, -1));
        int taskCount = 30;
        CountDownLatch allDone = new CountDownLatch(taskCount);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxFlight = new AtomicInteger();
        AtomicInteger cpuNow = new AtomicInteger();
        AtomicInteger maxCpu = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(6);
        CountDownLatch start = new CountDownLatch(1);

        for (int i = 1; i <= taskCount; i++) {
            final int id = i;
            pool.submit(() -> {
                try {
                    start.await();
                    scheduler.submit(TaskDefinition.builder("b" + id).queue("q").cpuUnits(3)
                            .body(ctx -> {
                                int f = inFlight.incrementAndGet();
                                maxFlight.accumulateAndGet(f, Math::max);
                                int c = cpuNow.addAndGet(3);
                                maxCpu.accumulateAndGet(c, Math::max);
                                Thread.sleep(15);
                                cpuNow.addAndGet(-3);
                                inFlight.decrementAndGet();
                                allDone.countDown();
                            }).build());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        log.info("[输入] 6 线程并发提交 {} 个 3cpu 任务（并发3 / cpu9）", taskCount);
        start.countDown();
        assertTrue(allDone.await(15, TimeUnit.SECONDS));
        pool.shutdown();
        log.info("[判定] 峰值并发={} 峰值cpu={}", maxFlight.get(), maxCpu.get());
        assertTrue(maxFlight.get() <= 3);
        assertTrue(maxCpu.get() <= 9);
        for (int i = 1; i <= taskCount; i++) {
            assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("b" + i).getStatus());
        }
    }

    @Test
    void concurrentCancelDoesNotLeakQuota() throws Exception {
        scheduler = new LocalJobScheduler();
        scheduler.registerQueue(new QueueQuota("q", 1, 5, Long.MAX_VALUE, -1));

        // 先用一个长任务占满唯一并发槽，确定性地制造 QUEUED 任务。
        CountDownLatch holderEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.builder("holder").queue("q").body(ctx -> {
            holderEntered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).build());
        assertTrue(holderEntered.await(2, TimeUnit.SECONDS));

        int n = 8;
        AtomicInteger runCount = new AtomicInteger();
        for (int i = 1; i <= n; i++) {
            final int id = i;
            scheduler.submit(TaskDefinition.builder("k" + id).queue("q")
                    .body(ctx -> {
                        runCount.incrementAndGet();
                        Thread.sleep(20);
                    }).build());
        }
        // 此刻 k1..k8 必然全部 QUEUED；并发对它们发起取消。
        int cancelThreads = 8;
        CountDownLatch cancelStart = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(cancelThreads);
        for (int t = 0; t < cancelThreads; t++) {
            final int id = t + 1;
            pool.submit(() -> {
                try {
                    cancelStart.await();
                    scheduler.cancel("k" + id);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        log.info("[输入] 占满并发槽后提交 {} 个排队任务，{} 线程并发取消", n, cancelThreads);
        cancelStart.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        release.countDown();
        assertTrue(scheduler.awaitQuiet(10000), "所有任务必须到达终态");

        int cancelled = 0;
        for (int i = 1; i <= n; i++) {
            TaskSnapshot s = scheduler.snapshot("k" + i);
            assertNotNull(s);
            assertTrue(s.getStatus().isTerminal(), "k" + i + " 未终态: " + s.getStatus());
            if (s.getStatus() == TaskStatus.CANCELLED) {
                cancelled++;
            }
        }
        log.info("[判定] 被取消任务数={}/{} 排队任务实际执行次数={}（必须为0）",
                cancelled, n, runCount.get());
        assertEquals(n, cancelled, "并发取消必须覆盖全部排队任务，且取消结论唯一");
        assertEquals(0, runCount.get(), "被取消的排队任务绝不允许执行");
        // 取消后再提交新任务能立即获得被回收的排队/配额空间（不泄漏）。
        CountDownLatch newDone = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.builder("after").queue("q").body(ctx -> newDone.countDown()).build());
        assertTrue(newDone.await(3, TimeUnit.SECONDS), "取消/完成后队列必须已腾出，新任务可执行");
        assertEquals(TaskStatus.SUCCEEDED, scheduler.awaitTerminal("after", 3000).getStatus());
    }
}
