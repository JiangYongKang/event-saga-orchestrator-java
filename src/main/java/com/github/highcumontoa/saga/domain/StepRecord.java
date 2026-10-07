package com.github.highcumontoa.saga.domain;

import com.github.highcumontoa.saga.error.ErrorCategory;

/** 单个步骤的运行时记录（可变，随状态机迁移更新）。 */
public class StepRecord {

    private final String stepName;
    private StepStatus status;
    private int compensationAttempts;
    /** 失败类别：TIMEOUT 表示超时，BUSINESS_FAILURE 表示业务失败；未失败为 null。 */
    private ErrorCategory failureCategory;

    public StepRecord(String stepName) {
        this.stepName = stepName;
        this.status = StepStatus.PENDING;
        this.compensationAttempts = 0;
        this.failureCategory = null;
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

    public void resetCompensationAttempts() {
        this.compensationAttempts = 0;
    }

    public ErrorCategory getFailureCategory() {
        return failureCategory;
    }

    public void setFailureCategory(ErrorCategory failureCategory) {
        this.failureCategory = failureCategory;
    }
}
