package com.github.highcumontoa.eventsagaorchestratorjava.engine;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.StepDefinition;

import java.util.List;

/** 一种业务流程的步骤编排定义。 */
public interface SagaFlow {

    String flowType();

    List<StepDefinition> steps();
}
