package com.github.highcumontoa.saga.api;

import com.github.highcumontoa.saga.core.ManualInterventionService;
import com.github.highcumontoa.saga.core.SagaOrchestrator;
import com.github.highcumontoa.saga.domain.ManualInterventionEntry;
import com.github.highcumontoa.saga.domain.ManualInterventionStatus;
import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.domain.SagaStatus;
import com.github.highcumontoa.saga.domain.StepRecord;
import com.github.highcumontoa.saga.event.EventAck;
import com.github.highcumontoa.saga.event.EventType;
import com.github.highcumontoa.saga.event.SagaEvent;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/** REST 入口：创建流程、投递事件、查询状态、人工介入清单。 */
@RestController
@RequestMapping("/api/sagas")
public class SagaController {

    private final SagaOrchestrator orchestrator;
    private final ManualInterventionService interventionService;

    public SagaController(SagaOrchestrator orchestrator, ManualInterventionService interventionService) {
        this.orchestrator = orchestrator;
        this.interventionService = interventionService;
    }

    public record CreateSagaRequest(String definitionName, String businessKey) {
    }

    public record CreateSagaResponse(String sagaId) {
    }

    public record EventRequest(String eventId, String businessKey, String stepName,
                               EventType type, String detail) {
    }

    public record StepView(String stepName, String status, int compensationAttempts,
                           String failureCategory) {
    }

    public record SagaView(String sagaId, String businessKey, String definitionName,
                           SagaStatus status, int currentStepIndex, List<StepView> steps) {
    }

    public record InterventionView(String sagaId, String businessKey, String definitionName,
                                   String stuckStepName, String failureCategory, String failureReason,
                                   int compensationAttempts, int reprocessAttempts,
                                   ManualInterventionStatus status,
                                   Instant createdAt, Instant updatedAt) {
    }

    public record PurgeResponse(int purged) {
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

    /** 人工介入清单查询：支持按业务键与状态过滤。 */
    @GetMapping("/interventions")
    public List<InterventionView> listInterventions(
            @RequestParam(required = false) String businessKey,
            @RequestParam(required = false) ManualInterventionStatus status) {
        return interventionService.list(businessKey, status).stream()
                .map(this::toView)
                .toList();
    }

    /** 人工确认后重新发起补偿处理。 */
    @PostMapping("/interventions/{sagaId}/reprocess")
    public InterventionView reprocess(@PathVariable String sagaId) {
        return toView(interventionService.reprocess(sagaId));
    }

    /** 清理已处理完成的清单条目。 */
    @DeleteMapping("/interventions/resolved")
    public PurgeResponse purgeResolved() {
        return new PurgeResponse(interventionService.purgeResolved());
    }

    private StepView toView(StepRecord record) {
        return new StepView(record.getStepName(), record.getStatus().name(),
                record.getCompensationAttempts(),
                record.getFailureCategory() == null ? null : record.getFailureCategory().name());
    }

    private InterventionView toView(ManualInterventionEntry entry) {
        return new InterventionView(entry.getSagaId(), entry.getBusinessKey(),
                entry.getDefinitionName(), entry.getStuckStepName(),
                entry.getFailureCategory().name(), entry.getFailureReason(),
                entry.getCompensationAttempts(), entry.getReprocessAttempts(),
                entry.getStatus(), entry.getCreatedAt(), entry.getUpdatedAt());
    }
}
