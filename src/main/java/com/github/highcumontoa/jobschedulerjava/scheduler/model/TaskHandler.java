package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务执行体，进程内模拟任务的真实工作。
 * 抛出异常即视为本次尝试失败；未在超时时间内返回将被判为超时。
 */
@FunctionalInterface
public interface TaskHandler {
    void run() throws Exception;
}
