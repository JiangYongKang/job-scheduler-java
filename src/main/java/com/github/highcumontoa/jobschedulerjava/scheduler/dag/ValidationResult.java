package com.github.highcumontoa.jobschedulerjava.scheduler.dag;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;

/**
 * 依赖校验结果。
 */
public record ValidationResult(boolean ok, FailureReason reason, String detail) {

    public static ValidationResult passed() {
        return new ValidationResult(true, FailureReason.NONE, "");
    }

    public static ValidationResult reject(FailureReason reason, String detail) {
        return new ValidationResult(false, reason, detail);
    }
}
