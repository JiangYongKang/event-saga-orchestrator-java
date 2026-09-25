package com.github.highcumontoa.saga.domain;

/** 流程（Saga）实例的生命周期状态。 */
public enum SagaStatus {
    /** 已创建，尚未推进任何步骤。 */
    NEW,
    /** 正在按步骤推进。 */
    RUNNING,
    /** 全部步骤完成，终态。 */
    COMPLETED,
    /** 正在按相反顺序执行补偿。 */
    COMPENSATING,
    /** 补偿全部完成，终态。 */
    COMPENSATED,
    /** 补偿重试耗尽，等待人工介入，终态。 */
    MANUAL_INTERVENTION
}
