package com.github.highcumontoa.eventsagaorchestratorjava.domain;

/**
 * Saga 流程状态机：
 * CREATED -> RUNNING -> COMPLETED
 * RUNNING -> COMPENSATING -> COMPENSATED | COMPENSATION_FAILED
 */
public enum SagaState {
    CREATED,
    RUNNING,
    COMPENSATING,
    COMPLETED,
    COMPENSATED,
    COMPENSATION_FAILED,
    FAILED
}
