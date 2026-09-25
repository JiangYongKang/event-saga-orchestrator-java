package com.github.highcumontoa.eventsagaorchestratorjava.deadletter;

import com.github.highcumontoa.eventsagaorchestratorjava.event.SagaEvent;

import java.time.Instant;

/** 死信条目：原始事件、归一化原因、发生时间。 */
public record DeadLetterEntry(
        SagaEvent event,
        String reasonCode,
        String detail,
        Instant deadLetteredAt) {
}
