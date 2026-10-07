package com.github.highcumontoa.jobschedulerjava.scheduler.core;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;

import java.util.concurrent.Future;

/**
 * 调度器内部的可变任务记录。所有字段仅由调度器在持有自身锁时读写，
 * runningFuture 用于超时/取消时中断执行线程。
 *
 * 单次执行结果归属由两个序号保证：attemptSeq 每次派发单调递增，作为该次执行的
 * 归属凭证；inFlightSeq 是当前仍在途（线程未退出）的执行序号，0 表示无在途执行。
 * 超时/取消/停止收尾只释放配额、保留 inFlightSeq，直到旧线程回报时按序号识别为
 * 迟到结果并归还槽位，从而杜绝同一任务两次执行重叠与结果串用。
 */
class TaskRecord {

    final TaskDefinition definition;
    TaskStatus status = TaskStatus.PENDING;
    FailureReason reason = FailureReason.NONE;
    int attempts = 0;
    /**
     * 单调递增的派发序号：每次真正派发一次执行 +1，作为该次执行的归属凭证。
     * 执行线程回报结果时必须带回派发时的序号，只有与当前派发出的序号一致才允许采纳。
     */
    long attemptSeq = 0L;
    /**
     * 在途执行序号：>0 表示该任务有一次执行线程尚未退出（包括被判超时/取消后
     * 仍不理会中断、继续运行的“僵尸”执行）；0 表示没有执行线程在途。
     * 同一任务在途槽位全任务唯一：必须等旧执行线程真正退出后才能派发下一次执行。
     */
    long inFlightSeq = 0L;
    /** 当前这次尝试是否已被派发执行（防止重复派发） */
    boolean dispatched = false;
    /** 当前尝试是否持有队列配额（保证恰好释放一次） */
    boolean quotaHeld = false;
    /** 本次尝试的开始时间（超时判定依据） */
    long attemptStartMillis = 0L;
    /** 本次进入 QUEUED 的时间（排队老化与先到先得的判定依据） */
    long queuedAtMillis = 0L;
    /** 当前执行句柄 */
    volatile Future<?> runningFuture;

    TaskRecord(TaskDefinition definition) {
        this.definition = definition;
    }

    boolean isTerminal() {
        return status == TaskStatus.SUCCEEDED || status == TaskStatus.FAILED
                || status == TaskStatus.CANCELLED || status == TaskStatus.BLOCKED;
    }

    TaskSnapshot toSnapshot() {
        return new TaskSnapshot(definition.taskId(), definition.queue(), status, reason, attempts,
                definition.priority());
    }
}
