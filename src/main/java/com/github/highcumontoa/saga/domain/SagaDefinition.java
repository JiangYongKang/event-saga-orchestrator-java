package com.github.highcumontoa.saga.domain;

import java.util.List;

/** 流程定义：名称 + 有序步骤列表。 */
public record SagaDefinition(String name, List<StepDefinition> steps) {
}
