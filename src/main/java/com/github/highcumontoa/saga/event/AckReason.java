package com.github.highcumontoa.saga.event;

/** 事件处理结果的可区分原因。 */
public enum AckReason {
    /** 事件被接受并产生了状态迁移。 */
    APPLIED,
    /** 相同 eventId 已被处理过，未重复产生副作用。 */
    DUPLICATE_EVENT,
    /** 事件对应的步骤不是当前待推进步骤（乱序/过期），未产生迁移。 */
    OUT_OF_ORDER,
    /** 流程已处于终态，事件被忽略。 */
    IGNORED_TERMINAL_STATE,
    /** 事件与流程当前状态不匹配（如终态前收到未知步骤）。 */
    REJECTED_UNKNOWN_STEP
}
