package com.github.highcumontoa.saga.store;

import com.github.highcumontoa.saga.domain.ManualInterventionEntry;
import com.github.highcumontoa.saga.domain.ManualInterventionStatus;

import java.util.List;
import java.util.Optional;

/** 人工介入清单的进程内存储。 */
public interface ManualInterventionStore {

    /** 按 sagaId 去重登记或更新，同一流程不会叠加多条。 */
    void upsert(ManualInterventionEntry entry);

    Optional<ManualInterventionEntry> findBySagaId(String sagaId);

    /** 按业务键与状态过滤；传 null 表示不过滤。 */
    List<ManualInterventionEntry> find(String businessKey, ManualInterventionStatus status);

    /** 清理已处理完成的条目，返回清理条数。 */
    int purgeResolved();
}
