package com.github.highcumontoa.saga.event;

import java.time.Instant;

/**
 * 事件契约：eventId 全局唯一（幂等键），businessKey 定位流程实例，
 * stepName 标识事件对应的步骤，occurredAt 用于流水审计。
 */
public record SagaEvent(String eventId, String businessKey, String stepName,
                        EventType type, String detail, Instant occurredAt) {
}
