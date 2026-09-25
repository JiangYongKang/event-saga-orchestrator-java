package com.github.highcumontoa.eventsagaorchestratorjava.api;

public record EventResultView(
        String status,
        String reasonCode,
        String sagaState,
        long version) {
}
