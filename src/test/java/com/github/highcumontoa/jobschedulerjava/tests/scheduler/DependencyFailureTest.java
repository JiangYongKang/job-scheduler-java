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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 依赖失败/取消传播：下游不得被调度，状态与原因可追溯。
 */
class DependencyFailureTest {

    private static final Logger log = LoggerFactory.getLogger(DependencyFailureTest.class);

    private LocalJobScheduler scheduler;

    @BeforeEach
    void setUp() {
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("q", 8, 64, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    @Test
    void upstreamFailureBlocksDownstreamTransitively() throws Exception {
        AtomicInteger downstreamRuns = new AtomicInteger();
        // 输入：root 必失败；mid 依赖 root；leaf 依赖 mid
        scheduler.submitBatch(List.of(
                TaskDefinition.of("root", "q", Set.of(), 1, 0, 0, () -> {
                    throw new IllegalStateException("root failed");
                }),
                TaskDefinition.of("mid", "q", Set.of("root"), 1, 0, 0, downstreamRuns::incrementAndGet),
                TaskDefinition.of("leaf", "q", Set.of("mid"), 1, 0, 0, downstreamRuns::incrementAndGet)));
        log.info("输入: root必失败, mid依赖root, leaf依赖mid; 判定依据: root=FAILED/RETRY_EXHAUSTED, "
                + "mid=BLOCKED/DEPENDENCY_FAILED, leaf=BLOCKED/DEPENDENCY_FAILED(沿链传播), 下游执行次数=0");

        assertTrue(scheduler.awaitTerminal(10_000));
        TaskSnapshot root = scheduler.snapshot("root");
        TaskSnapshot mid = scheduler.snapshot("mid");
        TaskSnapshot leaf = scheduler.snapshot("leaf");
        log.info("结果: root=({},{}) mid=({},{}) leaf=({},{}) downstreamRuns={}",
                root.status(), root.reason(), mid.status(), mid.reason(),
                leaf.status(), leaf.reason(), downstreamRuns.get());

        assertEquals(TaskStatus.FAILED, root.status());
        assertEquals(FailureReason.RETRY_EXHAUSTED, root.reason());
        assertEquals(TaskStatus.BLOCKED, mid.status());
        assertEquals(FailureReason.DEPENDENCY_FAILED, mid.reason());
        // 阻塞沿依赖链传播：leaf 的上游 mid 未成功，leaf 同样被阻塞
        assertEquals(TaskStatus.BLOCKED, leaf.status());
        assertEquals(FailureReason.DEPENDENCY_FAILED, leaf.reason());
        assertEquals(0, downstreamRuns.get(), "下游任务不得被调度执行");
    }

    @Test
    void upstreamCancellationBlocksDownstreamWithDistinctReason() throws Exception {
        AtomicInteger downstreamRuns = new AtomicInteger();
        CountDownLatchHolder holder = new CountDownLatchHolder();
        scheduler.submitBatch(List.of(
                TaskDefinition.of("slow", "q", Set.of(), 1, 0, 0, () -> {
                    holder.entered.countDown();
                    holder.release.await();
                }),
                TaskDefinition.of("child", "q", Set.of("slow"), 1, 0, 0, downstreamRuns::incrementAndGet)));
        holder.entered.await();
        log.info("输入: slow 运行中被取消, child 依赖 slow; 判定依据: slow=CANCELLED/CANCELLED_BY_USER, "
                + "child=BLOCKED/DEPENDENCY_CANCELLED 且从未执行");

        assertTrue(scheduler.cancel("slow"));
        holder.release.countDown();
        assertTrue(scheduler.awaitTerminal(10_000));

        TaskSnapshot slow = scheduler.snapshot("slow");
        TaskSnapshot child = scheduler.snapshot("child");
        log.info("结果: slow=({},{}) child=({},{}) downstreamRuns={}",
                slow.status(), slow.reason(), child.status(), child.reason(), downstreamRuns.get());
        assertEquals(TaskStatus.CANCELLED, slow.status());
        assertEquals(FailureReason.CANCELLED_BY_USER, slow.reason());
        assertEquals(TaskStatus.BLOCKED, child.status());
        assertEquals(FailureReason.DEPENDENCY_CANCELLED, child.reason());
        assertEquals(0, downstreamRuns.get());
    }

    @Test
    void successfulUpstreamUnblocksDownstream() throws Exception {
        AtomicInteger order = new AtomicInteger();
        StringBuilder executionOrder = new StringBuilder();
        scheduler.submitBatch(List.of(
                TaskDefinition.of("a", "q", Set.of(), 1, 0, 0, () -> {
                    Thread.sleep(50);
                    synchronized (executionOrder) {
                        executionOrder.append("a");
                    }
                }),
                TaskDefinition.of("b", "q", Set.of("a"), 1, 0, 0, () -> {
                    synchronized (executionOrder) {
                        executionOrder.append("b");
                    }
                })));
        log.info("输入: a->b 依赖链均成功; 判定依据: 两任务都 SUCCEEDED 且执行顺序为 ab");
        assertTrue(scheduler.awaitTerminal(10_000));
        log.info("结果: a={} b={} order={}", scheduler.snapshot("a").status(),
                scheduler.snapshot("b").status(), executionOrder);
        assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("a").status());
        assertEquals(TaskStatus.SUCCEEDED, scheduler.snapshot("b").status());
        assertEquals("ab", executionOrder.toString());
        assertEquals(0, order.get());
    }

    /** 简单栅栏持有者，避免在测试方法里声明过多局部变量。 */
    private static final class CountDownLatchHolder {
        final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    }
}
