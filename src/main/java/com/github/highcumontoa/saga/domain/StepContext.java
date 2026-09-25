package com.github.highcumontoa.saga.domain;

import java.util.Map;

/** 步骤执行上下文：业务键、步骤名与共享数据。 */
public record StepContext(String sagaId, String businessKey, String stepName, Map<String, Object> data) {
}
