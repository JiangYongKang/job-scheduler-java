package com.github.highcumontoa.jobschedulerjava.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
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
 * 上游失败/取消/超时/拒绝后，下游绝不被调度，状态为 SKIPPED 且原因为 DEPENDENCY_FAILED，可追溯到具体上游。
 */
class DependencyCascadeTest {

    private static final Logger log = LoggerFactory.getLogger(DependencyCascadeTest.class);

    private LocalJobScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void failedUpstreamSkipsTransitiveDownstream() throws Exception {
        scheduler = new LocalJobScheduler();
        AtomicInteger downstreamRuns = new AtomicInteger();
        scheduler.submit(TaskDefinition.builder("root").body(ctx -> {
            throw new IllegalStateException("root-boom");
        }).build());
        scheduler.submit(TaskDefinition.builder("mid").dependsOn("root")
                .body(ctx -> downstreamRuns.incrementAndGet()).build());
        scheduler.submit(TaskDefinition.builder("leaf").dependsOn("mid")
                .body(ctx -> downstreamRuns.incrementAndGet()).build());
        log.info("[输入] root 恒定失败，mid、leaf 依次依赖");

        TaskSnapshot root = scheduler.awaitTerminal("root", 5000);
        TaskSnapshot mid = scheduler.awaitTerminal("mid", 5000);
        TaskSnapshot leaf = scheduler.awaitTerminal("leaf", 5000);
        log.info("[判定] root={}/{} mid={}/{} leaf={}/{} detail(leaf)={}",
                root.getStatus(), root.getFailureReason(),
                mid.getStatus(), mid.getFailureReason(),
                leaf.getStatus(), leaf.getFailureReason(), leaf.getReasonDetail());

        assertEquals(TaskStatus.FAILED, root.getStatus());
        assertEquals(FailureReason.RETRIES_EXHAUSTED, root.getFailureReason());
        assertEquals(TaskStatus.SKIPPED, mid.getStatus());
        assertEquals(TaskStatus.SKIPPED, leaf.getStatus());
        assertEquals(FailureReason.DEPENDENCY_FAILED, mid.getFailureReason());
        assertEquals(FailureReason.DEPENDENCY_FAILED, leaf.getFailureReason());
        assertTrue(leaf.getReasonDetail().contains("mid"), "需可追溯到直接上游: " + leaf.getReasonDetail());
        assertEquals(0, downstreamRuns.get(), "下游任务体绝不允许执行");
    }

    @Test
    void cancelledUpstreamSkipsDownstream() throws Exception {
        scheduler = new LocalJobScheduler();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.builder("up").body(ctx -> {
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).build());
        AtomicInteger ran = new AtomicInteger();
        scheduler.submit(TaskDefinition.builder("down").dependsOn("up")
                .body(ctx -> ran.incrementAndGet()).build());
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        log.info("[输入] up 运行中被取消，down 依赖 up");
        boolean cancelled = scheduler.cancel("up");
        TaskSnapshot up = scheduler.awaitTerminal("up", 5000);
        TaskSnapshot down = scheduler.awaitTerminal("down", 5000);
        log.info("[判定] cancel返回={} up={}/{} down={}/{} down执行次数={}",
                cancelled, up.getStatus(), up.getFailureReason(),
                down.getStatus(), down.getFailureReason(), ran.get());
        assertTrue(cancelled);
        assertEquals(TaskStatus.CANCELLED, up.getStatus());
        assertEquals(FailureReason.CANCELLED, up.getFailureReason());
        assertEquals(TaskStatus.SKIPPED, down.getStatus());
        assertEquals(FailureReason.DEPENDENCY_FAILED, down.getFailureReason());
        assertEquals(0, ran.get());
        release.countDown();
    }

    @Test
    void rejectUpstreamSkipsDownstreamButSiblingStillRuns() throws Exception {
        scheduler = new LocalJobScheduler();
        // 不额外注册队列：default 无限额，这里用一个自定义队列构造拒绝。
        com.github.highcumontoa.jobschedulerjava.scheduler.model.QueueQuota q =
                new com.github.highcumontoa.jobschedulerjava.scheduler.model.QueueQuota(
                        "tight", 1, 100, Long.MAX_VALUE, -1);
        scheduler.registerQueue(q);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.builder("hold").queue("tight").body(ctx -> {
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).build());
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        scheduler.submit(TaskDefinition.builder("bad").queue("tight").rejectWhenBusy(true).build());
        scheduler.submit(TaskDefinition.builder("child").dependsOn("bad").build());
        scheduler.submit(TaskDefinition.builder("independent").build());
        log.info("[输入] bad 被配额拒绝，child 依赖 bad，independent 无依赖");

        TaskSnapshot bad = scheduler.awaitTerminal("bad", 5000);
        TaskSnapshot child = scheduler.awaitTerminal("child", 5000);
        TaskSnapshot independent = scheduler.awaitTerminal("independent", 5000);
        log.info("[判定] bad={}/{} child={}/{} independent={}",
                bad.getStatus(), bad.getFailureReason(),
                child.getStatus(), child.getFailureReason(), independent.getStatus());
        assertEquals(TaskStatus.REJECTED, bad.getStatus());
        assertEquals(FailureReason.QUOTA_REJECTED, bad.getFailureReason());
        assertEquals(TaskStatus.SKIPPED, child.getStatus());
        assertEquals(FailureReason.DEPENDENCY_FAILED, child.getFailureReason());
        assertEquals(TaskStatus.SUCCEEDED, independent.getStatus(), "无关节点不受影响");
        release.countDown();
    }

    @Test
    void cancelPendingBeforeDependenciesResolved() throws Exception {
        scheduler = new LocalJobScheduler();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        scheduler.submit(TaskDefinition.builder("slow").body(ctx -> {
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).build());
        AtomicInteger ran = new AtomicInteger();
        scheduler.submit(TaskDefinition.builder("waiter").dependsOn("slow")
                .body(ctx -> ran.incrementAndGet()).build());
        assertTrue(entered.await(2, TimeUnit.SECONDS));

        log.info("[输入] waiter 仍在 PENDING（依赖未满足）时直接取消");
        assertTrue(scheduler.cancel("waiter"));
        TaskSnapshot waiter = scheduler.snapshot("waiter");
        log.info("[判定] waiter={}/{} detail={}", waiter.getStatus(), waiter.getFailureReason(), waiter.getReasonDetail());
        assertEquals(TaskStatus.CANCELLED, waiter.getStatus());
        assertEquals(FailureReason.CANCELLED, waiter.getFailureReason());
        // 对终态再取消应无效
        assertFalse(scheduler.cancel("waiter"));
        release.countDown();
        scheduler.awaitTerminal("slow", 5000);
        assertEquals(0, ran.get());
    }
}
