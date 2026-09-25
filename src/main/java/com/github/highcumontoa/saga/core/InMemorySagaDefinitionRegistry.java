package com.github.highcumontoa.saga.core;

import com.github.highcumontoa.saga.domain.SagaDefinition;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 进程内流程定义注册表。 */
@Component
public class InMemorySagaDefinitionRegistry implements SagaDefinitionRegistry {

    private final Map<String, SagaDefinition> definitions = new ConcurrentHashMap<>();

    @Override
    public void register(SagaDefinition definition) {
        definitions.put(definition.name(), definition);
    }

    @Override
    public Optional<SagaDefinition> find(String name) {
        return Optional.ofNullable(definitions.get(name));
    }
}
