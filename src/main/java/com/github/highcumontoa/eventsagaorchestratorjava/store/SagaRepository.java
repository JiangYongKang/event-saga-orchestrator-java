package com.github.highcumontoa.eventsagaorchestratorjava.store;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaInstance;

import java.util.List;
import java.util.Optional;

/** 流程实例仓储。 */
public interface SagaRepository {

    SagaInstance save(SagaInstance instance);

    Optional<SagaInstance> findById(String businessKey);

    List<SagaInstance> findAll();
}
