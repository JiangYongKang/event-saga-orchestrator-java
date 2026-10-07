package com.github.highcumontoa.saga.manual;

/** 人工介入清单条目的处理状态。 */
public enum ManualInterventionStatus {
    /** 待人工处理。 */
    PENDING,
    /** 已处理完成（重新处理成功，流程已补偿完毕）。 */
    RESOLVED
}
