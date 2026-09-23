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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重试与超时：重试次数按任务配置生效；耗尽后进入终态；
 * 超时与重试耗尽、依赖失败、配额拒绝原因互斥可区分。
 */
class RetryAndTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndTimeoutTest.class);

    private LocalJobScheduler scheduler;
    private QuotaManager quota;

    @BeforeEach
    void setUp() {
        quota = new QuotaManager();
        quota.configureQueue("q", 4, 64, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    void retriesUntilSuccessWithConfiguredAttempts() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        // 输入：maxRetries=2，前 2 次失败、第 3 次成功
        scheduler.submit(TaskDefinition.of("flaky", "q", Set.of(), 1, 2, 0, () -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("attempt " + calls.get() + " failed");
            }
        }));
        log.info("输入: maxRetries=2, 前2次失败第3次成功; 判定依据: 最终 SUCCEEDED 且 attempts=3, handler 调用=3");
        assertTrue(scheduler.awaitTerminal(10_000));
        TaskSnapshot snap = scheduler.snapshot("flaky");
        log.info("结果: status={} attempts={} handlerCalls={}", snap.status(), snap.attempts(), calls.get());
        assertEquals(TaskStatus.SUCCEEDED, snap.status());
        assertEquals(3, snap.attempts());
        assertEquals(3, calls.get());
    }

    @Test
    void retryExhaustionLeadsToTerminalFailureWithDistinctReason() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        // 输入：maxRetries=2，永远失败 -> 共 1+2=3 次尝试
        scheduler.submit(TaskDefinition.of("doomed", "q", Set.of(), 1, 2, 0, () -> {
            calls.incrementAndGet();
            throw new IllegalStateException("always fails");
        }));
        log.info("输入: maxRetries=2, 永远失败; 判定依据: FAILED/RETRY_EXHAUSTED, attempts=3(1次首试+2次重试)");
        assertTrue(scheduler.awaitTerminal(10_000));
        TaskSnapshot snap = scheduler.snapshot("doomed");
        log.info("结果: status={} reason={} attempts={} handlerCalls={}",
                snap.status(), snap.reason(), snap.attempts(), calls.get());
        assertEquals(TaskStatus.FAILED, snap.status());
        assertEquals(FailureReason.RETRY_EXHAUSTED, snap.reason());
        assertEquals(3, snap.attempts());
        assertEquals(3, calls.get());
    }

    @Test
    void timeoutTriggersRetryAndEventuallyTerminalTimeout() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        // 输入：timeout=100ms, maxRetries=1，任务每次睡眠 5s（会被超时打断）
        scheduler.submit(TaskDefinition.of("slow", "q", Set.of(), 1, 1, 100, () -> {
            calls.incrementAndGet();
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        }));
        log.info("输入: timeoutMs=100, maxRetries=1, 任务每次睡眠5s; 判定依据: FAILED/TIMEOUT, attempts=2, 总耗时远小于10s");
        long start = System.currentTimeMillis();
        assertTrue(scheduler.awaitTerminal(30_000));
        long elapsed = System.currentTimeMillis() - start;
        TaskSnapshot snap = scheduler.snapshot("slow");
        log.info("结果: status={} reason={} attempts={} elapsedMs={}",
                snap.status(), snap.reason(), snap.attempts(), elapsed);
        assertEquals(TaskStatus.FAILED, snap.status());
        assertEquals(FailureReason.TIMEOUT, snap.reason());
        assertEquals(2, snap.attempts());
        assertTrue(elapsed < 5_000, "超时控制必须生效，不能等任务自然结束");
        // 超时任务释放配额
        assertEquals(0, quota.snapshot().get(0).running());
    }

    @Test
    void zeroRetriesMeansSingleAttempt() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        scheduler.submit(TaskDefinition.of("once", "q", Set.of(), 1, 0, 0, () -> {
            calls.incrementAndGet();
            throw new IllegalStateException("no retry configured");
        }));
        log.info("输入: maxRetries=0, 失败; 判定依据: FAILED/RETRY_EXHAUSTED 且 attempts=1(不重试)");
        assertTrue(scheduler.awaitTerminal(10_000));
        TaskSnapshot snap = scheduler.snapshot("once");
        log.info("结果: status={} reason={} attempts={}", snap.status(), snap.reason(), snap.attempts());
        assertEquals(TaskStatus.FAILED, snap.status());
        assertEquals(FailureReason.RETRY_EXHAUSTED, snap.reason());
        assertEquals(1, snap.attempts());
        assertEquals(1, calls.get());
    }

    @Test
    void failureReasonsAreMutuallyDistinct() throws Exception {
        // 同一场景汇集多种终态原因，验证可区分性
        AtomicInteger failCalls = new AtomicInteger();
        scheduler.submit(TaskDefinition.of("dep-parent", "q", Set.of(), 1, 0, 0, () -> {
            throw new IllegalStateException("parent fails");
        }));
        scheduler.submit(TaskDefinition.of("dep-child", "q", Set.of("dep-parent"), 1, 0, 0, () -> { }));
        scheduler.submit(TaskDefinition.of("to", "q", Set.of(), 1, 0, 80, () -> {
            Thread.sleep(5_000);
        }));
        scheduler.submit(TaskDefinition.of("rx", "q", Set.of(), 1, 1, 0, () -> {
            failCalls.incrementAndGet();
            throw new IllegalStateException("retry me");
        }));
        log.info("输入: 依赖失败链 + 超时任务 + 重试耗尽任务; 判定依据: 四种原因 RETRY_EXHAUSTED/"
                + "DEPENDENCY_FAILED/TIMEOUT 互不相同且各自归属正确");
        assertTrue(scheduler.awaitTerminal(30_000));
        TaskSnapshot parent = scheduler.snapshot("dep-parent");
        TaskSnapshot child = scheduler.snapshot("dep-child");
        TaskSnapshot timeout = scheduler.snapshot("to");
        TaskSnapshot exhausted = scheduler.snapshot("rx");
        log.info("结果: parent={} child={} timeout={} exhausted={}",
                parent.reason(), child.reason(), timeout.reason(), exhausted.reason());
        assertEquals(FailureReason.RETRY_EXHAUSTED, parent.reason());
        assertEquals(FailureReason.DEPENDENCY_FAILED, child.reason());
        assertEquals(FailureReason.TIMEOUT, timeout.reason());
        assertEquals(FailureReason.RETRY_EXHAUSTED, exhausted.reason());
        assertEquals(2, failCalls.get());
    }
}
