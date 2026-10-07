package com.github.highcumontoa.saga.api;

import com.github.highcumontoa.saga.error.ErrorCategory;
import com.github.highcumontoa.saga.error.SagaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一错误出口：把内部异常归一化为 (category, message)，
 * 不向客户端泄露堆栈与内部实现细节；内部细节仅记录日志。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    public record ErrorResponse(String category, String message) {
    }

    @ExceptionHandler(SagaException.class)
    public ResponseEntity<ErrorResponse> handleSagaException(SagaException e) {
        HttpStatus status = switch (e.getCategory()) {
            case VALIDATION -> HttpStatus.BAD_REQUEST;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case CONFLICT -> HttpStatus.CONFLICT;
            case TIMEOUT -> HttpStatus.GATEWAY_TIMEOUT;
            case BUSINESS_FAILURE, RETRY_EXHAUSTED, DEAD_LETTER -> HttpStatus.UNPROCESSABLE_ENTITY;
            case INTERNAL -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
        return ResponseEntity.status(status)
                .body(new ErrorResponse(e.getCategory().name(), e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception e) {
        // 内部异常细节只进日志，对外仅暴露归一化类别
        log.error("[api] 未预期异常：{}", e.toString());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(ErrorCategory.INTERNAL.name(), "内部错误"));
    }
}
