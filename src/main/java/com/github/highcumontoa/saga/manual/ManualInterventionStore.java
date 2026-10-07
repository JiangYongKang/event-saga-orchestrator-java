package com.github.highcumontoa.saga.manual;

import java.util.List;
import java.util.Optional;

/** 人工介入清单的进程内存储。 */
public interface ManualInterventionStore {

    /** 按 sagaId 去重保存：同一流程重复进入人工介入只保留一条记录。 */
    void upsert(ManualInterventionEntry entry);

    Optional<ManualInterventionEntry> findById(String entryId);

    Optional<ManualInterventionEntry> findBySagaId(String sagaId);

    /** 按业务键与状态过滤；参数为 null 表示不过滤。 */
    List<ManualInterventionEntry> query(String businessKey, ManualInterventionStatus status);

    /** 删除指定条目（仅允许清理已处理完成的条目，由调用方保证）。 */
    boolean remove(String entryId);
}
