package com.github.highcumontoa.eventsagaorchestratorjava.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 流程定义注册表：按 flowType 查找编排步骤。 */
@Component
public class SagaFlowRegistry {

    private static final Logger log = LoggerFactory.getLogger(SagaFlowRegistry.class);

    private final Map<String, SagaFlow> flows = new ConcurrentHashMap<>();

    public void register(SagaFlow flow) {
        SagaFlow previous = flows.put(flow.flowType(), flow);
        if (previous == null) {
            log.info("[FLOW-REGISTER] flowType={} steps={}", flow.flowType(), flow.steps().size());
        }
    }

    public Optional<SagaFlow> get(String flowType) {
        return Optional.ofNullable(flows.get(flowType));
    }
}
