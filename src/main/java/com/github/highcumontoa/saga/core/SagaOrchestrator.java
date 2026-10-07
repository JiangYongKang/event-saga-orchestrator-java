package com.github.highcumontoa.saga.core;

import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.event.EventAck;
import com.github.highcumontoa.saga.event.SagaEvent;
import com.github.highcumontoa.saga.manual.ManualInterventionEntry;
import com.github.highcumontoa.saga.manual.ManualInterventionStatus;

import java.util.List;

/** 编排器：流程创建、事件驱动推进、状态查询与人工介入处理。 */
public interface SagaOrchestrator {

    /** 创建流程实例，返回 sagaId。同一 businessKey 重复创建返回已有实例。 */
    String createSaga(String definitionName, String businessKey);

    /** 处理事件：幂等、串行化、驱动状态迁移与补偿。 */
    EventAck handleEvent(SagaEvent event);

    /** 查询流程当前状态快照。 */
    SagaInstance getSaga(String sagaId);

    /** 查询人工介入清单；参数为 null 表示不过滤。 */
    List<ManualInterventionEntry> listManualInterventions(String businessKey,
                                                          ManualInterventionStatus status);

    /**
     * 重新处理人工介入条目：从卡住位置继续按相反顺序补偿，
     * 已成功补偿的步骤不会重复补偿；仍失败则保持 MANUAL_INTERVENTION 终态。
     */
    ManualInterventionEntry reprocessManualIntervention(String entryId);

    /** 清理已处理完成（RESOLVED）的清单条目；未完成条目拒绝清理。 */
    void removeResolvedManualIntervention(String entryId);
}
