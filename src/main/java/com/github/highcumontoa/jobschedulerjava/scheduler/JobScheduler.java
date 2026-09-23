package com.github.highcumontoa.jobschedulerjava.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.QueueQuota;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.SubmitException;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;

import java.util.Collection;
import java.util.List;

/**
 * 本地进程内批处理任务编排与资源配额调度器。
 *
 * <p>所有方法均线程安全；任务 id 全局唯一，重复提交以 {@link SubmitException} 拒绝。</p>
 */
public interface JobScheduler {

    /** 注册/覆盖队列配额。已有运行中任务时不做迁移，仅影响后续调度。 */
    void registerQueue(QueueQuota quota);

    /**
     * 原子地提交一批任务：同批内重复 id、跨批次重复 id、未知依赖、成环、未知队列均拒绝，
     * 且整批不产生任何已注册任务（全成功或全失败）。
     */
    void submit(Collection<TaskDefinition> definitions);

    /** 单任务提交，语义等价于只含一个元素的批次。 */
    default void submit(TaskDefinition definition) {
        submit(List.of(definition));
    }

    /** 查询任务快照；任务不存在返回 null。 */
    TaskSnapshot snapshot(String taskId);

    /**
     * 请求取消任务：仅 PENDING/QUEUED 可直接取消；RUNNING 任务发出中断，
     * 由任务体响应中断后进入 CANCELLED。终态任务取消无效果。
     *
     * @return 若调用本身驱动了一次取消（或已取消）返回 true
     */
    boolean cancel(String taskId);

    /**
     * 对失败类终态任务（FAILED/TIMED_OUT/REJECTED）发起一次手动重试：
     * 重新进入 PENDING 并额外获得一次尝试机会。幂等：非可重试状态直接返回 false。
     */
    boolean retry(String taskId);

    /** 阻塞直到任务进入任意终态或超时；返回当时快照。 */
    TaskSnapshot awaitTerminal(String taskId, long timeoutMillis) throws InterruptedException;

    /** 阻塞直到调度器内不存在 RUNNING/QUEUED/PENDING 任务，或超时。 */
    boolean awaitQuiet(long timeoutMillis) throws InterruptedException;

    /** 关闭调度器，释放执行线程；不改变任务状态。 */
    void shutdown();
}
