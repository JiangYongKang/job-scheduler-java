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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败重试与超时控制：次数/超时按配置生效，耗尽进入终态，原因与依赖失败、配额拒绝可区分；
 * 重试不产生重复对外可见效果（同一时刻只有一次执行）。
 */
class RetryTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(RetryTimeoutTest.class);

    private LocalJobScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void retriesExhaustedReachesFailedTerminal() throws Exception {
        scheduler = new LocalJobScheduler();
        AtomicInteger attempts = new AtomicInteger();
        scheduler.submit(TaskDefinition.builder("flaky")
                .maxRetries(2)
                .body(ctx -> {
                    int n = attempts.incrementAndGet();
                    log.info("[执行] flaky 第 {} 次执行（共允许3次）", n);
                    throw new IllegalStateException("fail#" + n);
                }).build());
        log.info("[输入] maxRetries=2（最多3次执行），任务体恒定失败");
        TaskSnapshot snap = scheduler.awaitTerminal("flaky", 5000);
        log.info("[判定] {} 实际执行次数={}", snap, attempts.get());
        assertEquals(TaskStatus.FAILED, snap.getStatus());
        assertEquals(FailureReason.RETRIES_EXHAUSTED, snap.getFailureReason());
        assertEquals(3, snap.getAttempt(), "快照 attempt 应记录最后一次执行序号");
        assertEquals(3, attempts.get(), "必须恰好执行 maxRetries+1 次，不多不少");
        assertTrue(snap.getReasonDetail().contains("fail#3"), "末次错误需可追溯");
    }

    @Test
    void succeedsOnSecondAttemptHasNoFailureReason() throws Exception {
        scheduler = new LocalJobScheduler();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger concurrentExecutions = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        scheduler.submit(TaskDefinition.builder("flaky-ok")
                .maxRetries(3)
                .body(ctx -> {
                    int c = concurrentExecutions.incrementAndGet();
                    maxConcurrent.accumulateAndGet(c, Math::max);
                    if (attempts.incrementAndGet() < 2) {
                        concurrentExecutions.decrementAndGet();
                        throw new IllegalStateException("transient");
                    }
                    concurrentExecutions.decrementAndGet();
                }).build());
        log.info("[输入] maxRetries=3，任务体第2次执行成功");
        TaskSnapshot snap = scheduler.awaitTerminal("flaky-ok", 5000);
        log.info("[判定] {} 执行次数={} 重试期峰值并发={}（必须为1，不得重复执行）",
                snap, attempts.get(), maxConcurrent.get());
        assertEquals(TaskStatus.SUCCEEDED, snap.getStatus());
        assertEquals(FailureReason.NONE, snap.getFailureReason());
        assertEquals(2, attempts.get());
        assertEquals(1, maxConcurrent.get(), "同一任务同一时刻只允许一次执行");
    }

    @Test
    void timeoutWithoutRetryEntersTimedOut() throws Exception {
        scheduler = new LocalJobScheduler();
        scheduler.submit(TaskDefinition.builder("slow")
                .timeoutMillis(150)
                .body(ctx -> {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).build());
        log.info("[输入] timeoutMillis=150，任务体睡眠 2s 并响应中断");
        long t0 = System.currentTimeMillis();
        TaskSnapshot snap = scheduler.awaitTerminal("slow", 5000);
        long elapsed = System.currentTimeMillis() - t0;
        log.info("[判定] {} 耗时={}ms", snap, elapsed);
        assertEquals(TaskStatus.TIMED_OUT, snap.getStatus());
        assertEquals(FailureReason.TIMEOUT, snap.getFailureReason());
        assertTrue(elapsed < 1500, "超时后应尽快进入终态，实际 " + elapsed + "ms");
    }

    @Test
    void timeoutThenRetryFailureReasonIsTimeoutNotRetriesExhausted() throws Exception {
        scheduler = new LocalJobScheduler();
        AtomicInteger attempts = new AtomicInteger();
        scheduler.submit(TaskDefinition.builder("slow-retry")
                .timeoutMillis(100)
                .maxRetries(1)
                .body(ctx -> {
                    attempts.incrementAndGet();
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).build());
        log.info("[输入] timeoutMillis=100,maxRetries=1，两次均超时");
        TaskSnapshot snap = scheduler.awaitTerminal("slow-retry", 5000);
        log.info("[判定] {} 实际执行次数={}", snap, attempts.get());
        assertEquals(TaskStatus.TIMED_OUT, snap.getStatus());
        assertEquals(FailureReason.TIMEOUT, snap.getFailureReason(),
                "超时耗尽必须与普通失败耗尽可区分");
        assertEquals(2, attempts.get());
    }

    @Test
    void timeoutOnFirstAttemptButSucceedsOnRetry() throws Exception {
        scheduler = new LocalJobScheduler();
        AtomicInteger attempts = new AtomicInteger();
        scheduler.submit(TaskDefinition.builder("slow-then-fast")
                .timeoutMillis(300)
                .maxRetries(1)
                .body(ctx -> {
                    if (attempts.incrementAndGet() == 1) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }).build());
        log.info("[输入] 首次超时（300ms），第2次快速成功");
        TaskSnapshot snap = scheduler.awaitTerminal("slow-then-fast", 5000);
        log.info("[判定] {} 次数={}", snap, attempts.get());
        assertEquals(TaskStatus.SUCCEEDED, snap.getStatus());
        assertEquals(2, snap.getAttempt());
    }

    @Test
    void manualRetryAfterExhaustionGainsOneMoreAttempt() throws Exception {
        scheduler = new LocalJobScheduler();
        AtomicInteger attempts = new AtomicInteger();
        scheduler.submit(TaskDefinition.builder("manual")
                .maxRetries(0)
                .body(ctx -> {
                    int n = attempts.incrementAndGet();
                    if (n < 3) {
                        throw new IllegalStateException("fail#" + n);
                    }
                }).build());
        TaskSnapshot failed = scheduler.awaitTerminal("manual", 5000);
        log.info("[输入] 首次 FAILED: {}，随后两次手动 retry", failed);
        assertEquals(TaskStatus.FAILED, failed.getStatus());

        assertTrue(scheduler.retry("manual"));
        TaskSnapshot second = scheduler.awaitTerminal("manual", 5000);
        log.info("[判定1] retry后再失败: {} 次数={}", second, attempts.get());
        assertEquals(TaskStatus.FAILED, second.getStatus());
        assertEquals(2, attempts.get());

        assertTrue(scheduler.retry("manual"));
        TaskSnapshot third = scheduler.awaitTerminal("manual", 5000);
        log.info("[判定2] 再次retry后: {} 次数={}", third, attempts.get());
        assertEquals(TaskStatus.SUCCEEDED, third.getStatus());
        assertEquals(3, attempts.get());
    }

    @Test
    void retryIsIdempotentOnNonTerminalStates() throws Exception {
        scheduler = new LocalJobScheduler();
        scheduler.registerQueue(new QueueQuota("q", 1, 100, Long.MAX_VALUE, -1));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.builder("running").queue("q").body(ctx -> {
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).build());
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        log.info("[输入] 对 RUNNING 任务调用 retry，应被忽略");
        assertFalse(scheduler.retry("running"));
        assertEquals(TaskStatus.RUNNING, scheduler.snapshot("running").getStatus());
        assertFalse(scheduler.retry("ghost"), "不存在任务也应返回 false");
        release.countDown();
        scheduler.awaitTerminal("running", 5000);
    }
}
