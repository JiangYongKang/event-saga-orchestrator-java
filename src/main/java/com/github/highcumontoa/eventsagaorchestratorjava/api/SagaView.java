package com.github.highcumontoa.eventsagaorchestratorjava.api;

import java.util.List;

public record SagaView(
        String businessKey,
        String flowType,
        String state,
        int currentStepIndex,
        long version,
        List<StepStateView> steps) {
}
