package com.github.highcumontoa.eventsagaorchestratorjava.api;

import com.github.highcumontoa.eventsagaorchestratorjava.exception.ErrorCode;
import com.github.highcumontoa.eventsagaorchestratorjava.exception.ErrorResponse;
import com.github.highcumontoa.eventsagaorchestratorjava.exception.SagaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

/**
 * 将各类异常归一化为对外错误响应：
 * 只暴露错误码枚举与安全消息，内部堆栈仅记录在服务端日志。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(SagaException.class)
    public ResponseEntity<ErrorResponse> handleSaga(SagaException ex) {
        ErrorCode code = ex.getErrorCode();
        // 4xx 用 info，便于观察被拒绝的重复/乱序/冲突请求
        log.info("[API-ERROR] code={} message={}", code, ex.getMessage());
        return ResponseEntity.status(code.getHttpStatus())
                .body(new ErrorResponse(Instant.now(), code.name(), ex.getMessage(), null));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class,
            IllegalArgumentException.class})
    public ResponseEntity<ErrorResponse> handleValidation(Exception ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse(Instant.now(), ErrorCode.VALIDATION_ERROR.name(),
                        "request validation failed", null));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {
        // 兜底：内部细节不外泄，堆栈仅在服务端日志
        log.error("[API-ERROR] unexpected exception", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(Instant.now(), ErrorCode.INTERNAL_ERROR.name(),
                        "internal server error", null));
    }
}
