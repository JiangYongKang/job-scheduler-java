package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务运行态的不可变快照，用于外部查询与日志断言。
 */
public final class TaskSnapshot {

    private final String taskId;
    private final String queue;
    private final TaskStatus status;
    private final FailureReason failureReason;
    private final String reasonDetail;
    private final int attempt;
    private final int maxRetries;
    private final int cpuUnits;
    private final long memoryBytes;

    public TaskSnapshot(String taskId, String queue, TaskStatus status, FailureReason failureReason,
                        String reasonDetail, int attempt, int maxRetries, int cpuUnits, long memoryBytes) {
        this.taskId = taskId;
        this.queue = queue;
        this.status = status;
        this.failureReason = failureReason;
        this.reasonDetail = reasonDetail;
        this.attempt = attempt;
        this.maxRetries = maxRetries;
        this.cpuUnits = cpuUnits;
        this.memoryBytes = memoryBytes;
    }

    public String getTaskId() { return taskId; }
    public String getQueue() { return queue; }
    public TaskStatus getStatus() { return status; }
    public FailureReason getFailureReason() { return failureReason; }
    public String getReasonDetail() { return reasonDetail; }
    public int getAttempt() { return attempt; }
    public int getMaxRetries() { return maxRetries; }
    public int getCpuUnits() { return cpuUnits; }
    public long getMemoryBytes() { return memoryBytes; }

    @Override
    public String toString() {
        return "TaskSnapshot{taskId='" + taskId + "', queue='" + queue + "', status=" + status
                + ", failureReason=" + failureReason + ", reasonDetail='" + reasonDetail
                + "', attempt=" + attempt + "/" + (maxRetries + 1) + "}";
    }
}
