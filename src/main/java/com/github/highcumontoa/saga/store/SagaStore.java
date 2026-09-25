package com.github.highcumontoa.saga.store;

import com.github.highcumontoa.saga.domain.SagaInstance;
import java.util.Optional;

/** 流程实例的进程内存储。骨架阶段仅定义接口形状。 */
public interface SagaStore {

    void save(SagaInstance instance);

    Optional<SagaInstance> findById(String sagaId);

    Optional<SagaInstance> findByBusinessKey(String businessKey);

    boolean isEventProcessed(String eventId);

    void markEventProcessed(String eventId);
}
