package com.github.highcumontoa.saga.domain;

/** 单个步骤的运行时记录（可变，随状态机迁移更新）。 */
public class StepRecord {

    private final String stepName;
    private StepStatus status;
    private int compensationAttempts;
    /** 最近一次正向执行失败的种类（超时/业务失败），未失败过为 null。 */
    private StepFailureKind lastFailureKind;

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

    public StepFailureKind getLastFailureKind() {
        return lastFailureKind;
    }

    public void setLastFailureKind(StepFailureKind lastFailureKind) {
        this.lastFailureKind = lastFailureKind;
    }
}
