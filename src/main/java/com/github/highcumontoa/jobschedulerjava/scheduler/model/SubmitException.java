package com.github.highcumontoa.jobschedulerjava.scheduler.model;

/**
 * 提交期异常：任务未进入状态机，调用方可通过 {@link #getErrorCode()} 区分拒绝原因。
 */
public class SubmitException extends RuntimeException {

    private final SubmitErrorCode errorCode;

    public SubmitException(SubmitErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public SubmitErrorCode getErrorCode() {
        return errorCode;
    }
}
