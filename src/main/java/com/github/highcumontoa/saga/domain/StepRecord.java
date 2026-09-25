package com.github.highcumontoa.saga.domain;

/** 单个步骤的运行时记录（可变，随状态机迁移更新）。 */
public class StepRecord {

    private final String stepName;
    private StepStatus status;
    private int compensationAttempts;

    public StepRecord(String stepName) {
        this.stepName = stepName;
        this.status = StepStatus.PENDING;
        this.compensationAttempts = 0;
    }

    public String getStepName() {
        return stepName;
    }

    public StepStatus getStatus() {
        return status;
    }

    public void setStatus(StepStatus status) {
        this.status = status;
    }

    public int getCompensationAttempts() {
        return compensationAttempts;
    }

    public void incrementCompensationAttempts() {
        this.compensationAttempts++;
    }
}
