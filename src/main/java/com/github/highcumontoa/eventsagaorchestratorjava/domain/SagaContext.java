package com.github.highcumontoa.eventsagaorchestratorjava.domain;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 流程上下文：在步骤间传递数据，单流程内由编排器按 key 串行访问。
 */
public class SagaContext {

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public void put(String key, Object value) {
        attributes.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) attributes.get(key);
    }

    public Map<String, Object> snapshot() {
        return Map.copyOf(attributes);
    }
}
