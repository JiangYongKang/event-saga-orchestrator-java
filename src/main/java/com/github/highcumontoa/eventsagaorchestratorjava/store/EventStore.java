package com.github.highcumontoa.eventsagaorchestratorjava.store;

import com.github.highcumontoa.eventsagaorchestratorjava.event.SagaEvent;

import java.util.List;

/** 只追加事件存储，支持按 eventId 幂等查询与按业务键回放。 */
public interface EventStore {

    boolean exists(String eventId);

    void append(SagaEvent event);

    List<SagaEvent> findByBusinessKey(String businessKey);
}
