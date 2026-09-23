package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务生命周期状态。
 */
public enum TaskStatus {
    /** 已受理，等待上游依赖完成 */
    PENDING,
    /** 依赖已就绪，等待队列配额 */
    QUEUED,
    /** 已占用配额，正在执行 */
    RUNNING,
    /** 执行成功（终态） */
    SUCCEEDED,
    /** 执行失败且重试耗尽 / 超时（终态） */
    FAILED,
    /** 被用户取消（终态） */
    CANCELLED,
    /** 上游失败或被取消，永不调度（终态） */
    BLOCKED
}
