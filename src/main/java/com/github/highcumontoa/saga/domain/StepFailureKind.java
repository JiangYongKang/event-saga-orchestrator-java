package com.github.highcumontoa.saga.domain;

/** 步骤失败的可区分种类：超时与业务失败对外可区分。 */
public enum StepFailureKind {
    /** 步骤执行超过时间预算。 */
    TIMEOUT,
    /** 步骤动作自身抛出异常（业务失败）。 */
    BUSINESS_FAILURE
}
