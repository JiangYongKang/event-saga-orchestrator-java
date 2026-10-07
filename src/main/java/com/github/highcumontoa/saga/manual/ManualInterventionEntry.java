package com.github.highcumontoa.saga.manual;

import com.github.highcumontoa.saga.error.ErrorCategory;

import java.time.Instant;

/**
 * 人工介入清单条目：补偿重试耗尽后卡住的流程快照。
 * 只携带对外安全的信息，不包含内部异常堆栈。
 */
public class ManualInterventionEntry {

    private final String entryId;
    private final String sagaId;
    private final String businessKey;
    private final String definitionName;
    private final String stuckStepName;
    private final ErrorCategory failureCategory;
    private final String failureReason;
    private final Instant createdAt;
    private ManualInterventionStatus status;
    private int compensationAttempts;
    private int reprocessCount;
    private Instant updatedAt;

    public ManualInterventionEntry(String entryId, String sagaId, String businessKey,
                                   String definitionName, String stuckStepName,
                                   ErrorCategory failureCategory, String failureReason,
                                   int compensationAttempts, Instant createdAt) {
        this.entryId = entryId;
        this.sagaId = sagaId;
        this.businessKey = businessKey;
        this.definitionName = definitionName;
        this.stuckStepName = stuckStepName;
        this.failureCategory = failureCategory;
        this.failureReason = failureReason;
        this.compensationAttempts = compensationAttempts;
        this.createdAt = createdAt;
        this.status = ManualInterventionStatus.PENDING;
        this.reprocessCount = 0;
        this.updatedAt = createdAt;
    }

    public String getEntryId() { return entryId; }
    public String getSagaId() { return sagaId; }
    public String getBusinessKey() { return businessKey; }
    public String getDefinitionName() { return definitionName; }
    public String getStuckStepName() { return stuckStepName; }
    public ErrorCategory getFailureCategory() { return failureCategory; }
    public String getFailureReason() { return failureReason; }
    public Instant getCreatedAt() { return createdAt; }

    public ManualInterventionStatus getStatus() { return status; }
    public void setStatus(ManualInterventionStatus status) { this.status = status; }

    public int getCompensationAttempts() { return compensationAttempts; }
    public void setCompensationAttempts(int compensationAttempts) {
        this.compensationAttempts = compensationAttempts;
    }

    public int getReprocessCount() { return reprocessCount; }
    public void incrementReprocessCount() { this.reprocessCount++; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
