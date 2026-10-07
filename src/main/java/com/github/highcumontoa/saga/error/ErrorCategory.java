package com.github.highcumontoa.saga.error;

/** 对外归一化的错误类别，不泄露内部异常细节。 */
public enum ErrorCategory {
    VALIDATION,
    NOT_FOUND,
    CONFLICT,
    /** 步骤执行超出时间预算。 */
    TIMEOUT,
    /** 步骤自身业务失败（与超时区分）。 */
    BUSINESS_FAILURE,
    /** 补偿重试次数耗尽。 */
    RETRY_EXHAUSTED,
    /** 进入人工介入清单（死信）。 */
    DEAD_LETTER,
    INTERNAL
}
