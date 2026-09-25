package com.github.highcumontoa.saga.error;

/** 对外暴露的归一化异常：仅携带类别与安全信息，内部细节仅写日志。 */
public class SagaException extends RuntimeException {

    private final ErrorCategory category;

    public SagaException(ErrorCategory category, String message) {
        super(message);
        this.category = category;
    }

    public ErrorCategory getCategory() {
        return category;
    }
}
