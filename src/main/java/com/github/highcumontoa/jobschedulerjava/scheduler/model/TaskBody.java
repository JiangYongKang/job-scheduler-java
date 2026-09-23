package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务执行体。抛出任意异常即视为本次执行失败；正常返回即视为成功。
 * 实现应通过 {@link TaskContext#remainingMillis()} 或响应中断来配合超时控制。
 */
@FunctionalInterface
public interface TaskBody {

    void execute(TaskContext context) throws Exception;
}
