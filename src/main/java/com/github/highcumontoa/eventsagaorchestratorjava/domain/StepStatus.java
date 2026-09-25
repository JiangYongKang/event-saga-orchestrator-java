package com.github.highcumontoa.eventsagaorchestratorjava.domain;

/** 单个步骤的执行状态。 */
public enum StepStatus {
    PENDING,
    COMPLETED,
    COMPENSATING,
    COMPENSATED,
    FAILED
}
