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

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 迟到结果归属：任务配了超时和重试，执行体被判超时后不理会中断继续运行，
 * 调度器已重新排队并派发下一次执行，前一次执行的迟到结果此时才到达。
 * 无论迟到的是成功还是失败，都必须被忽略：
 * 最终状态只由最后一次执行的真实结果决定，配额恰好归还一次，
 * 过程中同一任务不会出现两次执行同时占用队列额度。
 *
 * 时序用闩锁精确编排（不依赖睡眠凑巧）：
 *   t0        第 1 次执行派发（timeout=2000ms）
 *   t0+2000   第 1 次被判超时、中断、释放额度、重新排队并派发第 2 次
 *   第 1 次执行看到第 2 次已开始后，再滞留 300ms 才带回迟到结果
 *   第 2 次执行等迟到结果落地后再运行 500ms，确认任务仍为 RUNNING 才给出真实结果
 */
class LateOutcomeAfterTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(LateOutcomeAfterTimeoutTest.class);

    /** 单次执行超时：要容纳“第2次开始 -> 第1次迟到返回 -> 第2次真实结束”的完整编排 */
    private static final long TIMEOUT_MS = 2_000L;
    /** 第 1 次执行看到第 2 次已派发后，继续滞留多久才带回迟到结果 */
    private static final long LINGER_AFTER_RETRY_MS = 300L;
    /** 第 2 次执行在迟到结果落地后再运行多久才给出真实结果（须 < TIMEOUT - LINGER） */
    private static final long LAST_ATTEMPT_TAIL_MS = 500L;

    /** 统计配额获取/归还次数与并发占用水位的 QuotaManager。 */
    private static final class CountingQuotaManager extends QuotaManager {
        private final AtomicInteger acquires = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();
        private int currentRunning;
        private int maxRunning;

        @Override
        public synchronized boolean tryAcquire(String queue, int cost) {
            boolean ok = super.tryAcquire(queue, cost);
            if (ok) {
                acquires.incrementAndGet();
                currentRunning++;
                maxRunning = Math.max(maxRunning, currentRunning);
            }
            return ok;
        }

        @Override
        public synchronized void release(String queue, int cost) {
            super.release(queue, cost);
            releases.incrementAndGet();
            currentRunning--;
        }

        int acquires() {
            return acquires.get();
        }

        int releases() {
            return releases.get();
        }

        synchronized int maxRunning() {
            return maxRunning;
        }
    }

    private CountingQuotaManager quota;
    private LocalJobScheduler scheduler;

    @BeforeEach
    void setUp() {
        quota = new CountingQuotaManager();
        // 单并发队列：任何一次额度重复归还都会让并发水位超过 1
        quota.configureQueue("q", 1, 64, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    /** 不理会中断地沉睡指定时长（模拟被判超时后仍继续运行的执行体）。 */
    private static void sleepIgnoringInterrupts(long millis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            try {
                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(remaining) + 1);
            } catch (InterruptedException ignored) {
                // 故意吞掉中断：复现“被判超时后执行体没有立刻停下来”的现场
            }
        }
    }

    /** 不理会中断地等待闩锁。 */
    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException ignored) {
                // 同上：故意吞掉中断
            }
        }
    }

    @Test
    void lateFailureAfterTimeoutIsIgnoredAndLastAttemptSuccessWins() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch secondAttemptStarted = new CountDownLatch(1);
        AtomicReference<TaskStatus> statusBeforeLastReturn = new AtomicReference<>();

        scheduler.submit(TaskDefinition.of("late-fail", "q", Set.of(), 1, 1, TIMEOUT_MS, () -> {
            int attempt = calls.incrementAndGet();
            if (attempt == 1) {
                // 第 1 次执行：被判超时后不理会中断，等第 2 次已派发再滞留 300ms，带回迟到失败
                awaitIgnoringInterrupts(secondAttemptStarted);
                sleepIgnoringInterrupts(LINGER_AFTER_RETRY_MS);
                log.info("时序: 第1次执行在第2次已派发后迟到返回失败");
                throw new IllegalStateException("late failure from attempt 1");
            }
            // 第 2 次执行（最后一次）：给迟到结果留出落地时间，再真实运行 500ms 后成功
            secondAttemptStarted.countDown();
            sleepIgnoringInterrupts(LINGER_AFTER_RETRY_MS + LAST_ATTEMPT_TAIL_MS);
            statusBeforeLastReturn.set(scheduler.snapshot("late-fail").status());
        }));
        log.info("输入: timeoutMs={} maxRetries=1, 第1次执行超时后滞留至第2次派发+{}ms带回迟到失败, "
                        + "第2次执行真实运行约{}ms后成功; 判定依据: 最终 SUCCEEDED 且 attempts=2, "
                        + "迟到失败被忽略、最后一次执行结束前任务不进入终态、额度获取=归还=2、并发水位<=1",
                TIMEOUT_MS, LINGER_AFTER_RETRY_MS, LINGER_AFTER_RETRY_MS + LAST_ATTEMPT_TAIL_MS);

        assertTrue(scheduler.awaitTerminal(30_000));
        TaskSnapshot snap = scheduler.snapshot("late-fail");
        log.info("结果: status={} reason={} attempts={} handlerCalls={} 最后一次返回前状态={} "
                        + "quotaAcquires={} quotaReleases={} maxRunning={}",
                snap.status(), snap.reason(), snap.attempts(), calls.get(),
                statusBeforeLastReturn.get(), quota.acquires(), quota.releases(), quota.maxRunning());

        // 最终结论与最后一次执行一致：第 2 次成功 -> SUCCEEDED
        assertEquals(TaskStatus.SUCCEEDED, snap.status());
        assertEquals(FailureReason.NONE, snap.reason());
        assertEquals(2, snap.attempts());
        assertEquals(2, calls.get());
        // 最后一次执行真正结束之前，任务不能被迟到结果推进终态
        assertEquals(TaskStatus.RUNNING, statusBeforeLastReturn.get());
        // 额度恰好归还一次/每次：两次派发两次获取、两次归还，无重复无遗漏
        assertEquals(2, quota.acquires());
        assertEquals(2, quota.releases());
        // 过程中不存在两次执行同时占用额度
        assertTrue(quota.maxRunning() <= 1, "同一任务不得有两次执行同时占用队列额度");
    }

    @Test
    void lateSuccessAfterTimeoutIsIgnoredAndLastAttemptFailureWins() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch secondAttemptStarted = new CountDownLatch(1);
        AtomicReference<TaskStatus> statusBeforeLastReturn = new AtomicReference<>();

        scheduler.submit(TaskDefinition.of("late-success", "q", Set.of(), 1, 1, TIMEOUT_MS, () -> {
            int attempt = calls.incrementAndGet();
            if (attempt == 1) {
                // 第 1 次执行：被判超时后不理会中断，等第 2 次已派发再滞留 300ms，带回迟到成功
                awaitIgnoringInterrupts(secondAttemptStarted);
                sleepIgnoringInterrupts(LINGER_AFTER_RETRY_MS);
                log.info("时序: 第1次执行在第2次已派发后迟到返回成功");
                return;
            }
            // 第 2 次执行（最后一次）：给迟到结果留出落地时间，再真实运行 500ms 后失败，重试已耗尽
            secondAttemptStarted.countDown();
            sleepIgnoringInterrupts(LINGER_AFTER_RETRY_MS + LAST_ATTEMPT_TAIL_MS);
            statusBeforeLastReturn.set(scheduler.snapshot("late-success").status());
            throw new IllegalStateException("real failure of last attempt");
        }));
        log.info("输入: timeoutMs={} maxRetries=1, 第1次执行超时后滞留至第2次派发+{}ms带回迟到成功, "
                        + "第2次执行真实运行约{}ms后失败; 判定依据: 最终 FAILED/RETRY_EXHAUSTED 且 attempts=2, "
                        + "迟到成功被忽略、最后一次执行结束前任务不进入终态、额度获取=归还=2、并发水位<=1",
                TIMEOUT_MS, LINGER_AFTER_RETRY_MS, LINGER_AFTER_RETRY_MS + LAST_ATTEMPT_TAIL_MS);

        assertTrue(scheduler.awaitTerminal(30_000));
        TaskSnapshot snap = scheduler.snapshot("late-success");
        log.info("结果: status={} reason={} attempts={} handlerCalls={} 最后一次返回前状态={} "
                        + "quotaAcquires={} quotaReleases={} maxRunning={}",
                snap.status(), snap.reason(), snap.attempts(), calls.get(),
                statusBeforeLastReturn.get(), quota.acquires(), quota.releases(), quota.maxRunning());

        // 最终结论与最后一次执行一致：第 2 次失败且重试耗尽 -> FAILED/RETRY_EXHAUSTED
        assertEquals(TaskStatus.FAILED, snap.status());
        assertEquals(FailureReason.RETRY_EXHAUSTED, snap.reason());
        assertEquals(2, snap.attempts());
        assertEquals(2, calls.get());
        // 任务还在跑（最后一次执行未结束）时不能被迟到成功标成 SUCCEEDED
        assertEquals(TaskStatus.RUNNING, statusBeforeLastReturn.get());
        // 额度恰好归还一次/每次：迟到结果不得替别的执行归还资源
        assertEquals(2, quota.acquires());
        assertEquals(2, quota.releases());
        assertTrue(quota.maxRunning() <= 1, "同一任务不得有两次执行同时占用队列额度");
    }
}
