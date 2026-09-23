package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 任务在其生命周期内可能处于的状态。
 */
public enum TaskStatus {

    /** 已提交，等待前置依赖全部成功。 */
    PENDING(false),

    /** 依赖已满足，等待队列资源/并发配额。 */
    QUEUED(false),

    /** 任务正在执行（重试由 attempt 计数体现）。 */
    RUNNING(false),

    /** 终态：执行成功。 */
    SUCCEEDED(true),

    /** 终态：执行失败且重试已耗尽。 */
    FAILED(true),

    /** 终态：本次执行超时且重试已耗尽。 */
    TIMED_OUT(true),

    /** 终态：队列策略为 REJECT 且配额不足/排队队列已满。 */
    REJECTED(true),

    /** 终态：用户主动取消。 */
    CANCELLED(true),

    /** 终态：上游任务未成功，链路级联跳过，自身从未执行。 */
    SKIPPED(true);

    private final boolean terminal;

    TaskStatus(boolean terminal) {
        this.terminal = terminal;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
