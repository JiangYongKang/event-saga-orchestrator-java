package com.github.highcumontoa.eventsagaorchestratorjava.exception;

import java.time.Instant;

/** 对外统一错误响应体。 */
public record ErrorResponse(
        Instant timestamp,
        String errorCode,
        String message,
        String businessKey) {
}
