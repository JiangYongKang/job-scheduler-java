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
 * 迟到执行结果归属（stale attempt result）：
 * 任务配置超时+重试，执行体被判超时后不理会中断继续运行，调度器已重新派发下一次执行，
 * 前一次执行的结果稍后才到达。该结果必须被忽略——不能改变任务状态、不能替新执行归还配额；
 * 同一任务同一时刻只能有一次执行在途；配额 acquire/release 恰好配对；
 * 最终结论必须与最后一次真正结束的执行一致。
 */
class StaleAttemptResultTest {

    private static final Logger log = LoggerFactory.getLogger(StaleAttemptResultTest.class);

    /** 统计 acquire/release 次数的配额管理器：验证额度恰好归还一次。 */
    private static class CountingQuotaManager extends QuotaManager {
        final AtomicInteger acquires = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();

        @Override
        public boolean tryAcquire(String queue, int cost) {
            boolean ok = super.tryAcquire(queue, cost);
            if (ok) {
                acquires.incrementAndGet();
            }
            return ok;
        }

        @Override
        public void release(String queue, int cost) {
            releases.incrementAndGet();
            super.release(queue, cost);
        }

        int running() {
            return snapshot().get(0).running();
        }
    }

    private LocalJobScheduler scheduler;
    private CountingQuotaManager quota;

    /** 任务执行体的实时并发：同时在跑的执行数与其峰值。 */
    private final AtomicInteger activeHandlers = new AtomicInteger();
    private final AtomicInteger peakHandlers = new AtomicInteger();
    /** 执行时序事件，用于断言先后顺序。 */
    private final ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();

    private void enterHandler() {
        int now = activeHandlers.incrementAndGet();
        peakHandlers.updateAndGet(peak -> Math.max(peak, now));
    }

    private void exitHandler() {
        activeHandlers.decrementAndGet();
    }

    @BeforeEach
    void setUp() {
        quota = new CountingQuotaManager();
        // 并发=1：任何“同一任务两次执行同时在跑”都会直接挤占唯一槽位，可稳定观测
        quota.configureQueue("q", 1, 16, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
        activeHandlers.set(0);
        peakHandlers.set(0);
        events.clear();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    private void awaitCondition(String description, long timeoutMillis,
                                java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("等待超时: " + description);
    }

    /**
     * 构造“超时后旧执行不理会打断、迟到结果在重试点运行期间到达”的确定性时序：
     * attempt1 在放行闩锁上等待（忽略中断）；超时后测试显式放行，attempt1 才退出
     * （按场景返回成功或抛出失败）；attempt2 固定工作 250ms 后结束（按场景成功/失败）。
     */
    private void runTimedOutThenLateResultScenario(String taskId, boolean attempt1LateFailure,
                                                   boolean attempt2Failure) throws Exception {
        final long timeoutMs = 150L;
        final int maxRetries = 1;
        CountDownLatch attempt1Entered = new CountDownLatch(1);
        CountDownLatch allowAttempt1ToExit = new CountDownLatch(1);
        AtomicInteger handlerEntries = new AtomicInteger();

        TaskDefinition definition = TaskDefinition.of(taskId, "q", Set.of(), 1,
                maxRetries, timeoutMs, () -> {
                    int entryNo = handlerEntries.incrementAndGet();
                    if (entryNo == 1) {
                        // 第 1 次执行：被判超时后忽略中断，直到测试放行才退出
                        enterHandler();
                        attempt1Entered.countDown();
                        boolean interrupted = false;
                        try {
                            while (allowAttempt1ToExit.getCount() > 0) {
                                try {
                                    allowAttempt1ToExit.await(50, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException e) {
                                    // 超时取消发出的中断到达，但执行体不理会，继续占用线程
                                    interrupted = true;
                                }
                            }
                        } finally {
                            events.add("attempt1-exit");
                            exitHandler();
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        if (attempt1LateFailure) {
                            throw new IllegalStateException("attempt1 late failure (must be ignored)");
                        }
                        return;
                    }
                    // 第 2 次执行（重试）：固定工作 60ms 后自然结束（小于 150ms 超时，
                    // 保证它不会被超时判定，从而让结果完全由自然回报决定）
                    enterHandler();
                    try {
                        events.add("attempt2-start");
                        Thread.sleep(60);
                        events.add("attempt2-end");
                    } finally {
                        exitHandler();
                    }
                    if (attempt2Failure) {
                        throw new IllegalStateException("attempt2 failure (the real final outcome)");
                    }
                });
        scheduler.submit(definition);

        // 1) 等待首次执行在跑
        assertTrue(attempt1Entered.await(5, TimeUnit.SECONDS));
        // 2) 等待超时判定发生：固定代码下任务停在 QUEUED 且额度已释放但旧执行仍在途（槽位保留）；
        //    旧代码会立即重新派发 attempt2（attempts 变为 2）
        awaitCondition("超时后重新排队(attempts=1,QUEUED) 或错误地重派(attempts=2)",
                5_000, () -> {
                    TaskSnapshot s = scheduler.snapshot(taskId);
                    return s.attempts() == 2
                            || (s.attempts() == 1 && s.status() == TaskStatus.QUEUED
                                && quota.running() == 0);
                });
        // 3) 再观察 150ms（远超 5ms 的 tick 周期）：旧执行未退出前，绝不能派发下一次执行
        Thread.sleep(150);
        TaskSnapshot duringOverlap = scheduler.snapshot(taskId);
        log.info("[{}] 超时后重叠期: status={} attempts={} activeHandlers={} peakHandlers={} quotaRunning={} acquires={} releases={}",
                taskId, duringOverlap.status(), duringOverlap.attempts(),
                activeHandlers.get(), peakHandlers.get(), quota.running(),
                quota.acquires.get(), quota.releases.get());
        assertEquals(1, activeHandlers.get(),
                "旧执行尚未退出，不得派发下一次执行（同一任务同时只能有一次执行在途）");
        assertEquals(TaskStatus.QUEUED, duringOverlap.status(),
                "旧执行仍在途时，重试必须停在 QUEUED，不能与旧执行同时占用额度");
        assertEquals(0, quota.running(), "超时后旧执行的额度必须已释放");

        // 4) 放行旧执行：其迟到结果此刻才回报；固定代码下 attempt2 必须在旧执行退出后才启动
        allowAttempt1ToExit.countDown();
        assertTrue(scheduler.awaitTerminal(10_000), "任务必须在最后一次执行结束后到达终态");

        TaskSnapshot fin = scheduler.snapshot(taskId);
        List<String> order = List.copyOf(events);
        log.info("[{}] 输入: timeoutMs={} maxRetries={} attempt1迟到{} attempt2实际{}; 判定依据: 最终状态与 attempt2 一致, "
                        + "时序={}, active峰值=1, acquires=releases=2, 终态时无在途执行",
                taskId, timeoutMs, maxRetries,
                attempt1LateFailure ? "失败" : "成功",
                attempt2Failure ? "失败" : "成功", order);
        log.info("[{}] 结果: status={} reason={} attempts={} order={} peakHandlers={} acquires={} releases={} quotaRunning={}",
                taskId, fin.status(), fin.reason(), fin.attempts(), order, peakHandlers.get(),
                quota.acquires.get(), quota.releases.get(), quota.running());

        // 时序：attempt1 退出 → attempt2 开始 → attempt2 结束
        assertEquals(List.of("attempt1-exit", "attempt2-start", "attempt2-end"), order,
                "下一次执行必须在上一次执行真正退出之后才启动");
        assertEquals(1, peakHandlers.get(), "整个过程中同一任务绝不能两次执行同时在跑");
        assertEquals(2, quota.acquires.get(), "两次执行各获取一次额度");
        assertEquals(2, quota.releases.get(), "额度必须恰好归还两次，迟到结果不得多归还");
        assertEquals(0, quota.running(), "终态后额度必须全部释放");
        assertEquals(2, fin.attempts(), "最终共两次尝试");
        assertEquals(0, activeHandlers.get(), "进入终态时不得还有执行在跑");
    }

    @Test
    void lateFailureFromPreviousAttemptIsIgnoredAndRetrySuccessWins() throws Exception {
        // 场景：attempt1 超时后迟到“失败”，重试 attempt2 实际成功
        // 旧代码会把迟到失败当成当前结果 -> 错误地 FAILED/RETRY_EXHAUSTED，并多释放一次配额
        runTimedOutThenLateResultScenario("late-fail", true, false);
        TaskSnapshot fin = scheduler.snapshot("late-fail");
        assertEquals(TaskStatus.SUCCEEDED, fin.status(),
                "迟到失败必须被忽略，最终结论取最后一次执行（attempt2 成功）");
        assertEquals(FailureReason.NONE, fin.reason());
    }

    @Test
    void lateSuccessFromPreviousAttemptIsIgnoredAndRetryFailureWins() throws Exception {
        // 场景：attempt1 超时后迟到“成功”，重试 attempt2 实际失败
        // 旧代码会提前把任务标成 SUCCEEDED（此时 attempt2 其实还在跑），迟到成功被错误采纳
        runTimedOutThenLateResultScenario("late-success", false, true);
        TaskSnapshot fin = scheduler.snapshot("late-success");
        assertEquals(TaskStatus.FAILED, fin.status(),
                "迟到成功必须被忽略，最终结论取最后一次执行（attempt2 失败）");
        assertEquals(FailureReason.RETRY_EXHAUSTED, fin.reason());
    }
}
