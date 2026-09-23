package com.github.highcumontoa.jobschedulerjava.scheduler.model;

import java.util.Set;

/**
 * 任务定义（提交时的不可变描述）。
 *
 * @param taskId        任务唯一标识
 * @param queue         所属队列（配额维度）
 * @param dependencies  前置任务 id 集合
 * @param cost          资源占用单位数
 * @param maxRetries    失败/超时后最大重试次数（不含首次执行）
 * @param timeoutMillis 单次执行超时时间（毫秒），<=0 表示不限制
 * @param handler       执行体
 */
public record TaskDefinition(
        String taskId,
        String queue,
        Set<String> dependencies,
        int cost,
        int maxRetries,
        long timeoutMillis,
        TaskHandler handler) {

    public TaskDefinition {
        dependencies = dependencies == null ? Set.of() : Set.copyOf(dependencies);
        if (cost <= 0) {
            throw new IllegalArgumentException("cost must be positive");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
    }

    public static TaskDefinition of(String taskId, String queue, Set<String> dependencies,
                                    int cost, int maxRetries, long timeoutMillis, TaskHandler handler) {
        return new TaskDefinition(taskId, queue, dependencies, cost, maxRetries, timeoutMillis, handler);
    }
}
