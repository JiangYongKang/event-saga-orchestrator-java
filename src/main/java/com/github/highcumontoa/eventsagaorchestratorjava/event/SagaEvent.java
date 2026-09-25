package com.github.highcumontoa.eventsagaorchestratorjava.event;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.EventType;

import java.time.Instant;
import java.util.Map;

/**
 * 不可变事件契约：
 * eventId 全局唯一用于幂等去重；businessKey 标识流程；
 * stepIndex 表示目标步骤；expectedVersion 用于并发冲突检测。
 */
public record SagaEvent(
        String eventId,
        String businessKey,
        EventType type,
        int stepIndex,
        Long expectedVersion,
        Map<String, Object> payload,
        Instant occurredAt) {

    public SagaEvent {
        // skeleton
    }
}
