package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务被拒绝或进入失败终态的原因，取值之间互斥且可区分。
 */
public enum FailureReason {
    /** 无（任务正常或未结束） */
    NONE,
    /** 提交被拒绝：同一批次或系统内重复定义任务 */
    DUPLICATE_TASK,
    /** 提交被拒绝：依赖了不存在的任务 */
    UNKNOWN_DEPENDENCY,
    /** 提交被拒绝：依赖关系成环 */
    DEPENDENCY_CYCLE,
    /** 提交被拒绝：所属队列配额策略为拒绝且配额不足 */
    QUOTA_REJECTED,
    /** 终态：上游任务失败 */
    DEPENDENCY_FAILED,
    /** 终态：上游任务被取消 */
    DEPENDENCY_CANCELLED,
    /** 终态：执行异常且重试耗尽 */
    RETRY_EXHAUSTED,
    /** 终态：执行超时且重试耗尽 */
    TIMEOUT,
    /** 终态：用户主动取消 */
    CANCELLED_BY_USER,
    /** 提交被拒绝：调度器已停止接收新任务（优雅停止中或已停止） */
    SCHEDULER_CLOSED,
    /** 终态：优雅停止排空时被取消（未运行完的排队/等待任务） */
    SHUTDOWN_DRAINED
}
