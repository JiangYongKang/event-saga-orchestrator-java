package com.github.highcumontoa.saga.domain;

import java.util.List;
import java.util.Map;

/** 流程实例：当前状态机快照。骨架阶段仅定义结构。 */
public class SagaInstance {

    private final String sagaId;
    private final String businessKey;
    private final String definitionName;
    private final List<StepRecord> steps;
    private final Map<String, Object> data;
    private SagaStatus status;
    private int currentStepIndex;

    public SagaInstance(String sagaId, String businessKey, String definitionName,
                        List<StepRecord> steps, Map<String, Object> data) {
        this.sagaId = sagaId;
        this.businessKey = businessKey;
        this.definitionName = definitionName;
        this.steps = steps;
        this.data = data;
        this.status = SagaStatus.NEW;
        this.currentStepIndex = 0;
    }

    public String getSagaId() { return sagaId; }
    public String getBusinessKey() { return businessKey; }
    public String getDefinitionName() { return definitionName; }
    public List<StepRecord> getSteps() { return steps; }
    public Map<String, Object> getData() { return data; }
    public SagaStatus getStatus() { return status; }
    public void setStatus(SagaStatus status) { this.status = status; }
    public int getCurrentStepIndex() { return currentStepIndex; }
    public void setCurrentStepIndex(int currentStepIndex) { this.currentStepIndex = currentStepIndex; }
}
