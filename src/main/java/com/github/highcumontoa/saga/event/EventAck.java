package com.github.highcumontoa.saga.event;

import com.github.highcumontoa.saga.domain.SagaStatus;

/** 事件处理回执：是否应用 + 可区分原因 + 处理后流程状态。 */
public record EventAck(boolean applied, AckReason reason, SagaStatus statusAfter) {
}
