package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务体执行时可见的上下文（只读）。
 */
public final class TaskContext {

    private final String taskId;
    private final int attempt;
    private final long deadlineEpochMillis;

    public TaskContext(String taskId, int attempt, long deadlineEpochMillis) {
        this.taskId = taskId;
        this.attempt = attempt;
        this.deadlineEpochMillis = deadlineEpochMillis;
    }

    public String getTaskId() {
        return taskId;
    }

    /** 从 1 开始的执行序号（1 为首次执行）。 */
    public int getAttempt() {
        return attempt;
    }

    public long getDeadlineEpochMillis() {
        return deadlineEpochMillis;
    }

    /** 距本次执行截止还剩的毫秒数；已超时返回非正数。 */
    public long remainingMillis() {
        return deadlineEpochMillis - System.currentTimeMillis();
    }
}
