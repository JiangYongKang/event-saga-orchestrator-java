package com.github.highcumontoa.eventsagaorchestratorjava.domain;

import java.time.Instant;

/**
 * 步骤运行记录：状态、重试次数、补偿次数（最小骨架）。
 */
public class SagaStepRecord {

    private final int index;
    private final String name;
    private StepStatus status = StepStatus.PENDING;
    private int forwardAttempts;
    private int compensationAttempts;
    private boolean compensationPoison;
    private Instant updatedAt;

    public SagaStepRecord(int index, String name) {
        this.index = index;
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public StepStatus getStatus() {
        return status;
    }

    public void setStatus(StepStatus status) {
        this.status = status;
    }

    public int getForwardAttempts() {
        return forwardAttempts;
    }

    public int getCompensationAttempts() {
        return compensationAttempts;
    }

    public boolean isCompensationPoison() {
        return compensationPoison;
    }

    public void setCompensationPoison(boolean compensationPoison) {
        this.compensationPoison = compensationPoison;
    }

    public void incrementForwardAttempts() {
        this.forwardAttempts++;
    }

    public void incrementCompensationAttempts() {
        this.compensationAttempts++;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void touch() {
        this.updatedAt = Instant.now();
    }
}
