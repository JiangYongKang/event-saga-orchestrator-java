package com.github.highcumontoa.eventsagaorchestratorjava.api;

import java.util.Map;

/**
 * 推进请求：eventId 由调用方提供用于幂等；
 * stepIndex 为目标步骤；expectedVersion 可选，用于乐观并发控制。
 */
public record AdvanceRequest(
        String eventId,
        Integer stepIndex,
        Long expectedVersion,
        Map<String, Object> payload) {
}
