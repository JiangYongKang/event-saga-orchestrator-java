package com.github.highcumontoa.saga.core;

import com.github.highcumontoa.saga.domain.SagaDefinition;
import java.util.Optional;

/** 流程定义注册表。 */
public interface SagaDefinitionRegistry {

    void register(SagaDefinition definition);

    Optional<SagaDefinition> find(String name);
}
