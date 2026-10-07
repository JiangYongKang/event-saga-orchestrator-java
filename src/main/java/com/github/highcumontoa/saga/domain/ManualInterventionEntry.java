package com.github.highcumontoa.saga.domain;

import com.github.highcumontoa.saga.error.ErrorCategory;

import java.time.Instant;

/**
 * 人工介入清单条目：补偿重试耗尽后登记，一条流程至多一条（按 sagaId 去重），
 * 重复重投只更新本条目，不叠加新记录。
 */
public class ManualInterventionEntry {

    private final String sagaId;
    private final String businessKey;
    private final String definitionName;
    private final ErrorCategory failureCategory;
    private final Instant createdAt;
    private String stuckStepName;
    private String failureReason;
    private int compensationAttempts;
    private int reprocessAttempts;
    private ManualInterventionStatus status;
    private Instant updatedAt;

    public ManualInterventionEntry(String sagaId, String businessKey, String definitionName,
                                   String stuckStepName, ErrorCategory failureCategory,
                                   String failureReason, int compensationAttempts, Instant createdAt) {
        this.sagaId = sagaId;
        this.businessKey = businessKey;
        this.definitionName = definitionName;
        this.stuckStepName = stuckStepName;
        this.failureCategory = failureCategory;
        this.failureReason = failureReason;
        this.compensationAttempts = compensationAttempts;
        this.createdAt = createdAt;
        this.reprocessAttempts = 0;
        this.status = ManualInterventionStatus.PENDING;
        this.updatedAt = createdAt;
    }

    public String getSagaId() { return sagaId; }
    public String getBusinessKey() { return businessKey; }
    public String getDefinitionName() { return definitionName; }
    public String getStuckStepName() { return stuckStepName; }
    public ErrorCategory getFailureCategory() { return failureCategory; }
    public String getFailureReason() { return failureReason; }
    public Instant getCreatedAt() { return createdAt; }
    public int getCompensationAttempts() { return compensationAttempts; }
    public int getReprocessAttempts() { return reprocessAttempts; }
    public ManualInterventionStatus getStatus() { return status; }
    public Instant getUpdatedAt() { return updatedAt; }

    public void setCompensationAttempts(int compensationAttempts) {
        this.compensationAttempts = compensationAttempts;
    }

    public void setStuckStepName(String stuckStepName) {
        this.stuckStepName = stuckStepName;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public void incrementReprocessAttempts() {
        this.reprocessAttempts++;
    }

    public void setStatus(ManualInterventionStatus status) {
        this.status = status;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
