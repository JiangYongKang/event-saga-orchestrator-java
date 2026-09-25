package com.github.highcumontoa.eventsagaorchestratorjava.domain;

/**
 * 步骤动作：正向执行 doStep 与补偿 compensate。
 * 实现必须保证副作用可被计数/可在测试中注入故障。
 */
public interface StepAction {

    StepResult doStep(SagaContext context);

    StepResult compensate(SagaContext context);
}
