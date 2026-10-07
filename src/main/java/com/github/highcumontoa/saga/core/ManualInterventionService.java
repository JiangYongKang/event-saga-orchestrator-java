package com.github.highcumontoa.saga.core;

import com.github.highcumontoa.saga.domain.ManualInterventionEntry;
import com.github.highcumontoa.saga.domain.ManualInterventionStatus;

import java.util.List;

/** 人工介入清单：查询、重新处理与清理。 */
public interface ManualInterventionService {

    /** 按业务键与状态查询清单；参数为 null 表示不过滤。 */
    List<ManualInterventionEntry> list(String businessKey, ManualInterventionStatus status);

    /**
     * 对处于人工介入终态的流程重新发起补偿处理。
     * 已成功回滚的步骤不会重复补偿，回滚顺序保持与原执行顺序相反。
     */
    ManualInterventionEntry reprocess(String sagaId);

    /** 清理已处理完成的条目，返回清理条数。 */
    int purgeResolved();
}
