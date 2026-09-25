package com.github.highcumontoa.saga.domain;

/** 单个步骤的执行状态。 */
public enum StepStatus {
    PENDING,
    EXECUTED,
    COMPENSATED
}
