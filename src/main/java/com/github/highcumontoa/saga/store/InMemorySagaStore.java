package com.github.highcumontoa.saga.store;

import com.github.highcumontoa.saga.domain.SagaInstance;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于 ConcurrentHashMap 的单进程存储。
 * 已处理事件 ID 集合用于至少一次投递语义下的幂等判定。
 */
@Component
public class InMemorySagaStore implements SagaStore {

    private final Map<String, SagaInstance> byId = new ConcurrentHashMap<>();
    private final Map<String, String> sagaIdByBusinessKey = new ConcurrentHashMap<>();
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    @Override
    public void save(SagaInstance instance) {
        byId.put(instance.getSagaId(), instance);
        sagaIdByBusinessKey.put(instance.getBusinessKey(), instance.getSagaId());
    }

    @Override
    public Optional<SagaInstance> findById(String sagaId) {
        return Optional.ofNullable(byId.get(sagaId));
    }

    @Override
    public Optional<SagaInstance> findByBusinessKey(String businessKey) {
        return Optional.ofNullable(sagaIdByBusinessKey.get(businessKey)).map(byId::get);
    }

    @Override
    public boolean isEventProcessed(String eventId) {
        return processedEventIds.contains(eventId);
    }

    @Override
    public void markEventProcessed(String eventId) {
        processedEventIds.add(eventId);
    }
}
