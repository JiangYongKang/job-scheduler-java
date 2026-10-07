package com.github.highcumontoa.jobschedulerjava.scheduler.core;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskSnapshot;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskStatus;

import java.util.concurrent.Future;

/**
 * 调度器内部的可变任务记录。所有字段仅由调度器在持有自身锁时读写，
 * runningFuture 用于超时/取消时中断执行线程。
 */
class TaskRecord {

    final TaskDefinition definition;
    TaskStatus status = TaskStatus.PENDING;
    FailureReason reason = FailureReason.NONE;
    int attempts = 0;
    /** 当前这次尝试是否已被派发执行（防止重复派发） */
    boolean dispatched = false;
    /** 当前尝试是否持有队列配额（保证恰好释放一次） */
    boolean quotaHeld = false;
    /** 本次尝试的开始时间（超时判定依据） */
    long attemptStartMillis = 0L;
    /** 本次进入 QUEUED 的时间（排队老化与先到先得的判定依据） */
    long queuedAtMillis = 0L;
    /**
     * 执行序号：每次派发递增，唯一标识一次执行。
     * 执行体回报结果时必须携带派发时的序号，序号不匹配即为迟到结果，一律忽略，
     * 保证一次执行的结果只被它自己那一次采纳。
     */
    long executionSeq = 0L;
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
