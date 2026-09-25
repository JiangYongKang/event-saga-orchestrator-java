package com.github.highcumontoa.saga.core;

import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.event.EventAck;
import com.github.highcumontoa.saga.event.SagaEvent;

/** 编排器：流程创建、事件驱动推进与状态查询。骨架阶段仅定义接口形状。 */
public interface SagaOrchestrator {

    /** 创建流程实例，返回 sagaId。同一 businessKey 重复创建返回已有实例。 */
    String createSaga(String definitionName, String businessKey);

    /** 处理事件：幂等、串行化、驱动状态迁移与补偿。 */
    EventAck handleEvent(SagaEvent event);

    /** 查询流程当前状态快照。 */
    SagaInstance getSaga(String sagaId);
}
