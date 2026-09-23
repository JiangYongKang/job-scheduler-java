package com.github.highcumontoa.jobschedulerjava.scheduler.core;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;

/**
 * 提交结果：被受理或被拒绝（含可区分原因）。
 */
public record SubmitResult(String taskId, boolean accepted, FailureReason reason, String detail) {

    public static SubmitResult accepted(String taskId) {
        return new SubmitResult(taskId, true, FailureReason.NONE, "");
    }

    public static SubmitResult rejected(String taskId, FailureReason reason, String detail) {
        return new SubmitResult(taskId, false, reason, detail);
    }
}
