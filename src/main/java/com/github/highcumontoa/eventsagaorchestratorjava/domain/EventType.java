package com.github.highcumontoa.eventsagaorchestratorjava.domain;

/** 事件类型契约：创建、前进步进、补偿驱动。 */
public enum EventType {
    SAGA_CREATED,
    STEP_ADVANCE,
    COMPENSATION_TICK
}
