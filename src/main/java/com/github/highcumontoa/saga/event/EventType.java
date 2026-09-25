package com.github.highcumontoa.saga.event;

/** 驱动流程推进的事件类型。 */
public enum EventType {
    /** 某一步骤在下游执行成功，请求推进。 */
    STEP_SUCCEEDED,
    /** 某一步骤执行失败，触发补偿。 */
    STEP_FAILED
}
