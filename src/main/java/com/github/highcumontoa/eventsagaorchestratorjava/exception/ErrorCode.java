package com.github.highcumontoa.eventsagaorchestratorjava.exception;

import org.springframework.http.HttpStatus;

/**
 * 归一化错误码：超时、重试耗尽、死信、并发冲突等统一归类。
 */
public enum ErrorCode {

    VALIDATION_ERROR(HttpStatus.BAD_REQUEST),
    NOT_FOUND(HttpStatus.NOT_FOUND),
    DUPLICATE_EVENT(HttpStatus.CONFLICT),
    STALE_EVENT(HttpStatus.CONFLICT),
    CONCURRENT_CONFLICT(HttpStatus.CONFLICT),
    STEP_TIMEOUT(HttpStatus.GATEWAY_TIMEOUT),
    RETRY_EXHAUSTED(HttpStatus.UNPROCESSABLE_ENTITY),
    COMPENSATION_FAILED(HttpStatus.UNPROCESSABLE_ENTITY),
    DEAD_LETTER(HttpStatus.UNPROCESSABLE_ENTITY),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR);

    private final HttpStatus httpStatus;

    ErrorCode(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }
}
