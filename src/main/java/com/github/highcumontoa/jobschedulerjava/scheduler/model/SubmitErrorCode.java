package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 提交期校验失败的错误码（任务尚未进入调度状态机）。
 */
public enum SubmitErrorCode {

    /** 任务定义缺少 id。 */
    MISSING_ID,

    /** 同一提交批次或已存在任务中出现重复 id。 */
    DUPLICATE_ID,

    /** 依赖了不存在的前置任务。 */
    UNKNOWN_DEPENDENCY,

    /** 依赖关系成环。 */
    CYCLIC_DEPENDENCY,

    /** 目标队列未注册配额配置。 */
    UNKNOWN_QUEUE
}
