package com.github.highcumontoa.eventsagaorchestratorjava.event;

/**
 * 事件被处理后的归类结果：
 * ACCEPTED 正常产生副作用；
 * DUPLICATE 至少一次投递下的重复事件（无副作用）；
 * STALE 过期/乱序事件（无副作用）；
 * CONFLICT 并发版本冲突（无副作用）；
 * COMPENSATED 前向失败后已完成补偿回滚；
 * DEAD_LETTER 补偿重试耗尽，进入死信等待人工介入。
 */
public enum EventAcceptStatus {
    ACCEPTED,
    DUPLICATE,
    STALE,
    CONFLICT,
    COMPENSATED,
    DEAD_LETTER
}
