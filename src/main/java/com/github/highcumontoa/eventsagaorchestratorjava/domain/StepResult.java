package com.github.highcumontoa.eventsagaorchestratorjava.domain;

/**
 * 步骤动作执行结果（最小骨架）。
 */
public record StepResult(boolean success, String reasonCode, String message) {

    public static StepResult ok() {
        return new StepResult(true, null, null);
    }

    public static StepResult fail(String reasonCode, String message) {
        return new StepResult(false, reasonCode, message);
    }
}
