package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务对外可见的不可变状态快照。
 */
public record TaskSnapshot(
        String taskId,
        String queue,
        TaskStatus status,
        FailureReason reason,
        int attempts) {
}
