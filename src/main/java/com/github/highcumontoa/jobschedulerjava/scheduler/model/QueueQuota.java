package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 单个队列的配额配置：
 * maxConcurrency 限制同时 RUNNING 的任务数；
 * cpuLimit/memoryLimit 限制同时 RUNNING 任务的资源占用总和；
 * maxQueued 限制 REJECT 之外的排队任务容量（-1 表示不限制）。
 */
public final class QueueQuota {

    private final String queue;
    private final int maxConcurrency;
    private final int cpuLimit;
    private final long memoryLimit;
    private final int maxQueued;

    public QueueQuota(String queue, int maxConcurrency, int cpuLimit, long memoryLimit, int maxQueued) {
        this.queue = queue;
        this.maxConcurrency = maxConcurrency;
        this.cpuLimit = cpuLimit;
        this.memoryLimit = memoryLimit;
        this.maxQueued = maxQueued;
    }

    public static QueueQuota of(String queue, int maxConcurrency, int cpuLimit, long memoryLimit, int maxQueued) {
        return new QueueQuota(queue, maxConcurrency, cpuLimit, memoryLimit, maxQueued);
    }

    public String getQueue() { return queue; }
    public int getMaxConcurrency() { return maxConcurrency; }
    public int getCpuLimit() { return cpuLimit; }
    public long getMemoryLimit() { return memoryLimit; }
    public int getMaxQueued() { return maxQueued; }
}
