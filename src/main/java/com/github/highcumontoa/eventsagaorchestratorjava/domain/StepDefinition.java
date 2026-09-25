package com.github.highcumontoa.eventsagaorchestratorjava.domain;

/**
 * 步骤定义：序号、名称、正向动作与补偿动作（最小骨架）。
 */
public record StepDefinition(int index, String name, StepAction action) {
}
