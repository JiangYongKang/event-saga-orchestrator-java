package com.github.highcumontoa.eventsagaorchestratorjava.store;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaInstance;
import com.github.highcumontoa.eventsagaorchestratorjava.event.SagaEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 单进程内存存储：
 * - 事件按 businessKey 分组、按追加顺序保存（可回放）。
 * - 全局 eventId 索引保证同一事件在至少一次投递下被精确识别。
 * 说明：调用方（编排器）按 businessKey 串行化写入，这里仍用并发集合兜底。
 */
@Component
public class InMemorySagaStore implements EventStore, SagaRepository {

    private final ConcurrentHashMap<String, List<SagaEvent>> events = new ConcurrentHashMap<>();
    private final Set<String> globalEventIds = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, SagaInstance> instances = new ConcurrentHashMap<>();

    @Override
    public boolean exists(String eventId) {
        return eventId != null && globalEventIds.contains(eventId);
    }

    @Override
    public void append(SagaEvent event) {
        if (!globalEventIds.add(event.eventId())) {
            // 防御性检查：编排器应在调用前已判定重复
            throw new IllegalStateException("event already appended: " + event.eventId());
        }
        events.computeIfAbsent(event.businessKey(), k -> new ArrayList<>()).add(event);
    }

    @Override
    public List<SagaEvent> findByBusinessKey(String businessKey) {
        return List.copyOf(events.getOrDefault(businessKey, List.of()));
    }

    @Override
    public SagaInstance save(SagaInstance instance) {
        instances.put(instance.getBusinessKey(), instance);
        return instance;
    }

    @Override
    public Optional<SagaInstance> findById(String businessKey) {
        return Optional.ofNullable(instances.get(businessKey));
    }

    @Override
    public List<SagaInstance> findAll() {
        return List.copyOf(instances.values());
    }
}
