package com.github.highcumontoa.saga.api;

import com.github.highcumontoa.saga.core.SagaOrchestrator;
import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.domain.SagaStatus;
import com.github.highcumontoa.saga.domain.StepRecord;
import com.github.highcumontoa.saga.event.EventAck;
import com.github.highcumontoa.saga.event.EventType;
import com.github.highcumontoa.saga.event.SagaEvent;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/** REST 入口：创建流程、投递事件、查询状态。 */
@RestController
@RequestMapping("/api/sagas")
public class SagaController {

    private final SagaOrchestrator orchestrator;

    public SagaController(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    public record CreateSagaRequest(String definitionName, String businessKey) {
    }

    public record CreateSagaResponse(String sagaId) {
    }

    public record EventRequest(String eventId, String businessKey, String stepName,
                               EventType type, String detail) {
    }

    public record StepView(String stepName, String status, int compensationAttempts) {
    }

    public record SagaView(String sagaId, String businessKey, String definitionName,
                           SagaStatus status, int currentStepIndex, List<StepView> steps) {
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreateSagaResponse create(@RequestBody CreateSagaRequest request) {
        return new CreateSagaResponse(orchestrator.createSaga(request.definitionName(), request.businessKey()));
    }

    @PostMapping("/events")
    public EventAck publish(@RequestBody EventRequest request) {
        SagaEvent event = new SagaEvent(request.eventId(), request.businessKey(),
                request.stepName(), request.type(), request.detail(), Instant.now());
        return orchestrator.handleEvent(event);
    }

    @GetMapping("/{sagaId}")
    public SagaView get(@PathVariable String sagaId) {
        SagaInstance instance = orchestrator.getSaga(sagaId);
        List<StepView> steps = instance.getSteps().stream()
                .map(this::toView)
                .toList();
        return new SagaView(instance.getSagaId(), instance.getBusinessKey(),
                instance.getDefinitionName(), instance.getStatus(),
                instance.getCurrentStepIndex(), steps);
    }

    private StepView toView(StepRecord record) {
        return new StepView(record.getStepName(), record.getStatus().name(),
                record.getCompensationAttempts());
    }
}
