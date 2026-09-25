package com.github.highcumontoa.eventsagaorchestratorjava.api;

public record StepStateView(
        int index,
        String name,
        String status,
        int forwardAttempts,
        int compensationAttempts) {
}
