package com.github.highcumontoa.eventsagaorchestratorjava.api;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.EventType;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaInstance;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaStepRecord;
import com.github.highcumontoa.eventsagaorchestratorjava.engine.SagaOrchestrator;
import com.github.highcumontoa.eventsagaorchestratorjava.event.EventProcessingResult;
import com.github.highcumontoa.eventsagaorchestratorjava.event.SagaEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 流程编排 REST 入口。
 *
 *  - POST /api/sagas                       创建流程
 *  - POST /api/sagas/{key}/events          事件驱动推进（携带 eventId/stepIndex/expectedVersion）
 *  - POST /api/sagas/{key}/compensate      人工触发补偿 / 补偿失败终态下重试
 *  - GET  /api/sagas/{key}                 查询状态（含各步骤状态与尝试次数）
 */
@RestController
@RequestMapping("/api/sagas")
public class SagaController {

    private final SagaOrchestrator orchestrator;

    public SagaController(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping
    public ResponseEntity<EventResultView> create(@RequestBody CreateSagaRequest request) {
        return present(orchestrator.createSaga(request.businessKey(), request.flowType()));
    }

    @PostMapping("/{businessKey}/events")
    public ResponseEntity<EventResultView> advance(@PathVariable String businessKey,
                                                   @RequestBody AdvanceRequest request) {
        Map<String, Object> payload = request.payload() == null ? Map.of() : request.payload();
        SagaEvent event = new SagaEvent(
                request.eventId() != null ? request.eventId() : UUID.randomUUID().toString(),
                businessKey,
                EventType.STEP_ADVANCE,
                request.stepIndex() == null ? -1 : request.stepIndex(),
                request.expectedVersion(),
                payload,
                Instant.now());
        return present(orchestrator.process(event));
    }

    @PostMapping("/{businessKey}/compensate")
    public ResponseEntity<EventResultView> compensate(@PathVariable String businessKey,
                                      @RequestBody(required = false) CompensateRequest request) {
        String reason = request == null || request.reasonCode() == null
                ? "MANUAL" : request.reasonCode();
        return present(orchestrator.triggerCompensation(businessKey, reason));
    }

    @GetMapping("/{businessKey}")
    public SagaView get(@PathVariable String businessKey) {
        SagaInstance saga = orchestrator.getSaga(businessKey);
        List<StepStateView> steps = saga.getSteps().values().stream()
                .map(SagaController::toStepView)
                .toList();
        return new SagaView(
                saga.getBusinessKey(),
                saga.getFlowType(),
                saga.getState().name(),
                saga.getCurrentStepIndex(),
                saga.getVersion(),
                steps);
    }

    private static StepStateView toStepView(SagaStepRecord record) {
        return new StepStateView(
                record.getIndex(),
                record.getName(),
                record.getStatus().name(),
                record.getForwardAttempts(),
                record.getCompensationAttempts());
    }

    private static EventResultView toView(EventProcessingResult result) {
        return new EventResultView(
                result.status().name(),
                result.reasonCode(),
                result.sagaState() == null ? null : result.sagaState().name(),
                result.version());
    }

    /**
     * 将处理归类映射为可区分的 HTTP 状态：
     * DUPLICATE/STALE/CONFLICT -> 409；DEAD_LETTER -> 422；其余正常 -> 200。
     * 注意：重复/乱序是“被识别的正常拒绝”，仍返回结构化结果体而非错误堆栈。
     */
    private static ResponseEntity<EventResultView> present(EventProcessingResult result) {
        HttpStatus http = switch (result.status()) {
            case DUPLICATE, STALE, CONFLICT -> HttpStatus.CONFLICT;
            case DEAD_LETTER -> HttpStatus.UNPROCESSABLE_ENTITY;
            case ACCEPTED, COMPENSATED -> HttpStatus.OK;
        };
        return ResponseEntity.status(http).body(toView(result));
    }
}
