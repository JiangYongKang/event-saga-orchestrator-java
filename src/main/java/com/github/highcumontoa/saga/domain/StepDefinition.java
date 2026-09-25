package com.github.highcumontoa.saga.domain;

/** 步骤定义：名称、正向动作与补偿动作。 */
public record StepDefinition(String name, StepAction action, StepAction compensation) {
}
