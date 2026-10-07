package com.github.highcumontoa.saga.core;

import com.github.highcumontoa.saga.domain.ManualInterventionEntry;
import com.github.highcumontoa.saga.domain.ManualInterventionStatus;
import com.github.highcumontoa.saga.error.ErrorCategory;
import com.github.highcumontoa.saga.error.SagaException;
import com.github.highcumontoa.saga.store.ManualInterventionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/** 人工介入清单服务默认实现：查询、重投与清理。 */
@Service
public class DefaultManualInterventionService implements ManualInterventionService {

    private static final Logger log = LoggerFactory.getLogger(DefaultManualInterventionService.class);

    private final ManualInterventionStore interventionStore;
    private final SagaOrchestrator orchestrator;

    public DefaultManualInterventionService(ManualInterventionStore interventionStore,
                                            SagaOrchestrator orchestrator) {
        this.interventionStore = interventionStore;
        this.orchestrator = orchestrator;
    }

    @Override
    public List<ManualInterventionEntry> list(String businessKey, ManualInterventionStatus status) {
        List<ManualInterventionEntry> entries = interventionStore.find(businessKey, status);
        log.info("[intervention] 查询清单 businessKey={} status={} 命中 {} 条",
                businessKey, status, entries.size());
        return entries;
    }

    @Override
    public ManualInterventionEntry reprocess(String sagaId) {
        ManualInterventionEntry entry = interventionStore.findBySagaId(sagaId)
                .orElseThrow(() -> new SagaException(ErrorCategory.NOT_FOUND, "人工介入清单中不存在该流程"));
        if (entry.getStatus() != ManualInterventionStatus.PENDING) {
            throw new SagaException(ErrorCategory.CONFLICT, "该条目已处理完成，不能重复处理");
        }
        // 状态机校验与补偿推进由编排器负责（非人工介入终态会抛 CONFLICT）
        orchestrator.reprocess(sagaId);
        return interventionStore.findBySagaId(sagaId)
                .orElseThrow(() -> new SagaException(ErrorCategory.INTERNAL, "清单条目缺失"));
    }

    @Override
    public int purgeResolved() {
        int removed = interventionStore.purgeResolved();
        log.info("[intervention] 清理已处理完成的条目 {} 条", removed);
        return removed;
    }
}
