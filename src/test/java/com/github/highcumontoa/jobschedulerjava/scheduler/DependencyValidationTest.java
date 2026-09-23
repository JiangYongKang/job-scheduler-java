package com.github.highcumontoa.jobschedulerjava.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.SubmitErrorCode;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.SubmitException;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 提交期依赖校验：空 id / 重复 id / 未知依赖 / 成环 / 未知队列必须被拒绝，且错误码可区分。
 */
class DependencyValidationTest {

    private static final Logger log = LoggerFactory.getLogger(DependencyValidationTest.class);

    private LocalJobScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void missingIdIsRejected() {
        scheduler = new LocalJobScheduler();
        log.info("[输入] 提交一个 id 为空的任务");
        TaskDefinition def = TaskDefinition.builder(" ").build();
        SubmitException ex = assertThrows(SubmitException.class, () -> scheduler.submit(def));
        log.info("[判定] errorCode={} message={}", ex.getErrorCode(), ex.getMessage());
        assertEquals(SubmitErrorCode.MISSING_ID, ex.getErrorCode());
        assertNull(scheduler.snapshot(" "), "被拒绝的任务不应进入状态机");
    }

    @Test
    void duplicateIdWithinBatchIsRejectedAndBatchIsAtomic() {
        scheduler = new LocalJobScheduler();
        TaskDefinition a1 = TaskDefinition.builder("a").build();
        TaskDefinition a2 = TaskDefinition.builder("a").build();
        log.info("[输入] 同批提交两个 id=a 的任务，期望整批失败");
        SubmitException ex = assertThrows(SubmitException.class, () -> scheduler.submit(List.of(a1, a2)));
        log.info("[判定] errorCode={} message={}", ex.getErrorCode(), ex.getMessage());
        assertEquals(SubmitErrorCode.DUPLICATE_ID, ex.getErrorCode());
        assertNull(scheduler.snapshot("a"), "原子批次：重复 id 时任何任务都不得注册");
    }

    @Test
    void duplicateIdAcrossBatchesIsRejected() throws Exception {
        scheduler = new LocalJobScheduler();
        scheduler.submit(TaskDefinition.builder("a").build());
        scheduler.awaitTerminal("a", 2000);
        log.info("[输入] 再次提交已存在的 id=a");
        SubmitException ex = assertThrows(SubmitException.class,
                () -> scheduler.submit(TaskDefinition.builder("a").build()));
        log.info("[判定] errorCode={} message={}", ex.getErrorCode(), ex.getMessage());
        assertEquals(SubmitErrorCode.DUPLICATE_ID, ex.getErrorCode());
    }

    @Test
    void unknownDependencyIsRejected() {
        scheduler = new LocalJobScheduler();
        log.info("[输入] 任务 b 依赖不存在的任务 ghost");
        TaskDefinition b = TaskDefinition.builder("b").dependsOn("ghost").build();
        SubmitException ex = assertThrows(SubmitException.class, () -> scheduler.submit(b));
        log.info("[判定] errorCode={} message={}", ex.getErrorCode(), ex.getMessage());
        assertEquals(SubmitErrorCode.UNKNOWN_DEPENDENCY, ex.getErrorCode());
        assertNull(scheduler.snapshot("b"));
    }

    @Test
    void selfDependencyTreatedAsCycle() {
        scheduler = new LocalJobScheduler();
        log.info("[输入] 任务 x 依赖自身");
        SubmitException ex = assertThrows(SubmitException.class,
                () -> scheduler.submit(TaskDefinition.builder("x").dependsOn("x").build()));
        log.info("[判定] errorCode={} message={}", ex.getErrorCode(), ex.getMessage());
        assertEquals(SubmitErrorCode.CYCLIC_DEPENDENCY, ex.getErrorCode());
    }

    @Test
    void cyclicDependencyIsRejected() {
        scheduler = new LocalJobScheduler();
        log.info("[输入] 提交成环批次 a->b->c->a");
        TaskDefinition a = TaskDefinition.builder("a").dependsOn("c").build();
        TaskDefinition b = TaskDefinition.builder("b").dependsOn("a").build();
        TaskDefinition c = TaskDefinition.builder("c").dependsOn("b").build();
        SubmitException ex = assertThrows(SubmitException.class,
                () -> scheduler.submit(List.of(a, b, c)));
        log.info("[判定] errorCode={} message={}", ex.getErrorCode(), ex.getMessage());
        assertEquals(SubmitErrorCode.CYCLIC_DEPENDENCY, ex.getErrorCode());
        assertNull(scheduler.snapshot("a"));
        assertNull(scheduler.snapshot("b"));
        assertNull(scheduler.snapshot("c"));
    }

    @Test
    void unknownQueueIsRejected() {
        scheduler = new LocalJobScheduler();
        log.info("[输入] 提交到未注册队列 q-x");
        TaskDefinition def = TaskDefinition.builder("z").queue("q-x").build();
        SubmitException ex = assertThrows(SubmitException.class, () -> scheduler.submit(def));
        log.info("[判定] errorCode={} message={}", ex.getErrorCode(), ex.getMessage());
        assertEquals(SubmitErrorCode.UNKNOWN_QUEUE, ex.getErrorCode());
    }

    @Test
    void validDiamondDagRunsOncePerTask() throws Exception {
        scheduler = new LocalJobScheduler();
        AtomicInteger runs = new AtomicInteger();
        TaskDefinition a = TaskDefinition.builder("a").body(ctx -> runs.incrementAndGet()).build();
        TaskDefinition b = TaskDefinition.builder("b").dependsOn("a").body(ctx -> runs.incrementAndGet()).build();
        TaskDefinition c = TaskDefinition.builder("c").dependsOn("a").body(ctx -> runs.incrementAndGet()).build();
        TaskDefinition d = TaskDefinition.builder("d").dependsOn("b", "c").body(ctx -> runs.incrementAndGet()).build();
        log.info("[输入] 提交菱形 DAG a->{b,c}->d");
        scheduler.submit(List.of(a, b, c, d));
        scheduler.awaitQuiet(3000);
        TaskSnapshot snap = scheduler.snapshot("d");
        log.info("[判定] d={} 总执行次数={}（期望4，无重复执行）", snap, runs.get());
        assertEquals(TaskStatus.SUCCEEDED, snap.getStatus());
        assertEquals(4, runs.get(), "每个任务必须恰好执行一次");
    }
}
