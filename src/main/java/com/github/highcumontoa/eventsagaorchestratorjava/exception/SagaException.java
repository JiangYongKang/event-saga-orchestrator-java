package com.github.highcumontoa.eventsagaorchestratorjava.exception;

/**
 * 对外可区分的领域异常，只携带错误码与安全消息，不泄露内部堆栈。
 */
public class SagaException extends RuntimeException {

    private final ErrorCode errorCode;

    public SagaException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public SagaException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
