package com.github.highcumontoa.eventsagaorchestratorjava.event;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaState;

/**
 * 事件处理结果（最小骨架）：归类原因 + 处理后状态/版本。
 */
public record EventProcessingResult(
        EventAcceptStatus status,
        String reasonCode,
        SagaState sagaState,
        long version) {
}
