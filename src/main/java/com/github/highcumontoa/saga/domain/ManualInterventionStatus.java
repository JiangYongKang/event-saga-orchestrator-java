package com.github.highcumontoa.saga.domain;

/** 人工介入清单条目的处理状态。 */
public enum ManualInterventionStatus {
    /** 等待人工处理。 */
    PENDING,
    /** 已处理完成（补偿最终成功），可被清理。 */
    RESOLVED
}
