package com.github.highcumontoa.eventsagaorchestratorjava.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一个业务键对应的流程实例聚合根（最小骨架）。
 */
public class SagaInstance {

    private String businessKey;
    private String flowType;
    private SagaState state = SagaState.CREATED;
    private int currentStepIndex = -1;
    private long version = 0;
    private final Map<Integer, SagaStepRecord> steps = new LinkedHashMap<>();
    private final List<String> eventIds = new ArrayList<>();
    private transient SagaContext context;
    private String failureReason;
    private Instant createdAt;
    private Instant updatedAt;

    public String getBusinessKey() {
        return businessKey;
    }

    public void setBusinessKey(String businessKey) {
        this.businessKey = businessKey;
    }

    public String getFlowType() {
        return flowType;
    }

    public void setFlowType(String flowType) {
        this.flowType = flowType;
    }

    public SagaState getState() {
        return state;
    }

    public void setState(SagaState state) {
        this.state = state;
    }

    public int getCurrentStepIndex() {
        return currentStepIndex;
    }

    public void setCurrentStepIndex(int currentStepIndex) {
        this.currentStepIndex = currentStepIndex;
    }

    public long getVersion() {
        return version;
    }

    public long nextVersion() {
        return ++version;
    }

    public Map<Integer, SagaStepRecord> getSteps() {
        return steps;
    }

    public List<String> getEventIds() {
        return eventIds;
    }

    public SagaContext getContext() {
        return context;
    }

    public void setContext(SagaContext context) {
        this.context = context;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public void touch() {
        this.updatedAt = Instant.now();
    }
}
