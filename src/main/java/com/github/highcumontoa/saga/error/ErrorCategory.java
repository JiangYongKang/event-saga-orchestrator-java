package com.github.highcumontoa.saga.error;

/** 对外归一化的错误类别，不泄露内部异常细节。 */
public enum ErrorCategory {
    VALIDATION,
    NOT_FOUND,
    CONFLICT,
    TIMEOUT,
    RETRY_EXHAUSTED,
    DEAD_LETTER,
    INTERNAL
}
