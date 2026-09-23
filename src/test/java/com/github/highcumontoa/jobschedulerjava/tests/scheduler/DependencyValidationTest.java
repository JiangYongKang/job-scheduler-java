package com.github.highcumontoa.jobschedulerjava.tests.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.core.LocalJobScheduler;
import com.github.highcumontoa.jobschedulerjava.scheduler.core.SubmitResult;
import com.github.highcumontoa.jobschedulerjava.scheduler.dag.DependencyValidator;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.quota.QuotaManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 依赖校验：重复定义、未知依赖、成环必须被拒绝且原因互斥可区分。
 */
class DependencyValidationTest {

    private static final Logger log = LoggerFactory.getLogger(DependencyValidationTest.class);

    private LocalJobScheduler scheduler;

    @BeforeEach
    void setUp() {
        QuotaManager quota = new QuotaManager();
        quota.configureQueue("q", 4, 16, false);
        scheduler = new LocalJobScheduler(quota, new DependencyValidator());
        scheduler.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdown();
    }

    private TaskDefinition task(String id, Set<String> deps) {
        return TaskDefinition.of(id, "q", deps, 1, 0, 0, () -> { });
    }

    @Test
    void duplicateTaskIdIsRejectedWithDistinctReason() {
        SubmitResult first = scheduler.submit(task("a", Set.of()));
        SubmitResult dup = scheduler.submit(task("a", Set.of()));
        log.info("输入: 两次提交 taskId=a; 判定依据: 第二次应与第一次结果不同且原因=DUPLICATE_TASK");
        log.info("结果: first={} dup={}", first, dup);
        assertTrue(first.accepted());
        assertFalse(dup.accepted());
        assertEquals(FailureReason.DUPLICATE_TASK, dup.reason());
    }

    @Test
    void unknownDependencyIsRejectedWithDistinctReason() {
        SubmitResult result = scheduler.submit(task("b", Set.of("ghost")));
        log.info("输入: 任务 b 依赖不存在的 ghost; 判定依据: 拒绝且原因=UNKNOWN_DEPENDENCY");
        log.info("结果: {}", result);
        assertFalse(result.accepted());
        assertEquals(FailureReason.UNKNOWN_DEPENDENCY, result.reason());
    }

    @Test
    void dependencyCycleIsRejectedWithDistinctReason() {
        List<SubmitResult> results = scheduler.submitBatch(List.of(
                task("x", Set.of("z")),
                task("y", Set.of("x")),
                task("z", Set.of("y"))));
        log.info("输入: x->z, y->x, z->y 构成环; 判定依据: 整批拒绝且原因=DEPENDENCY_CYCLE");
        log.info("结果: {}", results);
        assertTrue(results.stream().noneMatch(SubmitResult::accepted));
        assertTrue(results.stream().allMatch(r -> r.reason() == FailureReason.DEPENDENCY_CYCLE));
    }

    @Test
    void selfDependencyIsRejectedAsCycle() {
        SubmitResult result = scheduler.submit(task("s", Set.of("s")));
        log.info("输入: 任务 s 自依赖; 判定依据: 拒绝且原因=DEPENDENCY_CYCLE");
        log.info("结果: {}", result);
        assertFalse(result.accepted());
        assertEquals(FailureReason.DEPENDENCY_CYCLE, result.reason());
    }

    @Test
    void duplicateInsideBatchRejectsWholeBatch() {
        List<SubmitResult> results = scheduler.submitBatch(List.of(
                task("m", Set.of()),
                task("m", Set.of())));
        log.info("输入: 同批次两个 taskId=m; 判定依据: 整批拒绝且原因=DUPLICATE_TASK，且系统内不应登记 m");
        log.info("结果: {}", results);
        assertTrue(results.stream().noneMatch(SubmitResult::accepted));
        assertTrue(results.stream().allMatch(r -> r.reason() == FailureReason.DUPLICATE_TASK));
        // 整批拒绝：再次提交 m 应被受理，证明第一批未残留
        SubmitResult retry = scheduler.submit(task("m", Set.of()));
        log.info("判定依据: 整批拒绝后同名任务可重新提交; 结果: {}", retry);
        assertTrue(retry.accepted());
    }

    @Test
    void validChainIsAccepted() {
        List<SubmitResult> results = scheduler.submitBatch(List.of(
                task("t1", Set.of()),
                task("t2", Set.of("t1")),
                task("t3", Set.of("t1", "t2"))));
        log.info("输入: 合法链 t1<-t2<-t3; 判定依据: 全部受理");
        log.info("结果: {}", results);
        assertTrue(results.stream().allMatch(SubmitResult::accepted));
    }
}
