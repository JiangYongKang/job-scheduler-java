package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务终态/被拒绝时的可区分原因。
 * 用于区分依赖失败、配额拒绝、重试耗尽与超时等不同调度结论。
 */
public enum FailureReason {

    /** 无原因（任务未进入失败类终态）。 */
    NONE,

    /** 上游任务失败/超时/被取消/被拒绝，导致本任务永远不可能满足依赖。 */
    DEPENDENCY_FAILED,

    /** 队列 REJECT 策略下，无法获得并发或资源配额，且不允许排队。 */
    QUOTA_REJECTED,

    /** 执行重试次数耗尽（最后一次执行业务返回失败）。 */
    RETRIES_EXHAUSTED,

    /** 执行超过任务配置的超时时间，且重试已耗尽（或不允许重试）。 */
    TIMEOUT,

    /** 用户主动取消。 */
    CANCELLED
}
