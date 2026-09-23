package com.github.highcumontoa.jobschedulerjava.scheduler.quota;

/**
 * 队列配额使用快照。
 */
public record QueueUsage(String queue, int running, int maxConcurrent, int usedResource, int maxResource) {
}
