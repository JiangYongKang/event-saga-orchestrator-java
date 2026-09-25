package com.github.highcumontoa.eventsagaorchestratorjava.engine;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.EventType;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaContext;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaInstance;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaState;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaStepRecord;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.StepDefinition;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.StepResult;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.StepStatus;
import com.github.highcumontoa.eventsagaorchestratorjava.deadletter.DeadLetterQueue;
import com.github.highcumontoa.eventsagaorchestratorjava.event.EventAcceptStatus;
import com.github.highcumontoa.eventsagaorchestratorjava.event.EventProcessingResult;
import com.github.highcumontoa.eventsagaorchestratorjava.event.SagaEvent;
import com.github.highcumontoa.eventsagaorchestratorjava.exception.ErrorCode;
import com.github.highcumontoa.eventsagaorchestratorjava.exception.SagaException;
import com.github.highcumontoa.eventsagaorchestratorjava.store.EventStore;
import com.github.highcumontoa.eventsagaorchestratorjava.store.SagaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事件驱动流程编排核心。
 *
 * 关键不变式（均在 businessKey 分段锁内成立）：
 * 1. 幂等：同一 eventId 全局只产生一次副作用，重复投递返回 DUPLICATE + 可区分原因。
 * 2. 顺序：STEP_ADVANCE 必须严格等于“下一未完成步骤”，乱序返回 STALE/OUT_OF_ORDER。
 * 3. 并发：按 businessKey 串行 + expectedVersion 乐观版本检测，冲突返回 CONFLICT。
 * 4. 补偿：前向失败后，已完成步骤按执行相反顺序回滚；补偿按步重试，
 *    单步重试耗尽后进入 COMPENSATION_FAILED 终态并落死信，绝不静默丢弃。
 * 5. 归一化：超时/重试耗尽/死信等统一映射到 ErrorCode，对外消息不含内部堆栈。
 */
@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    static final String REASON_TIMEOUT = "STEP_TIMEOUT";
    static final String REASON_RETRY_EXHAUSTED = "FORWARD_RETRY_EXHAUSTED";
    static final String REASON_COMPENSATION_EXHAUSTED = "COMPENSATION_RETRY_EXHAUSTED";
    static final String REASON_OUT_OF_ORDER = "OUT_OF_ORDER";
    static final String REASON_STALE = "STALE_EVENT";
    static final String REASON_VERSION_CONFLICT = "VERSION_CONFLICT";
    static final String REASON_DUPLICATE = "DUPLICATE_EVENT";
    static final String REASON_UNKNOWN_SAGA = "UNKNOWN_SAGA";
    static final String REASON_UNKNOWN_FLOW = "UNKNOWN_FLOW";
    static final String REASON_TERMINAL = "TERMINAL_STATE";

    private final EventStore eventStore;
    private final SagaRepository repository;
    private final SagaFlowRegistry flowRegistry;
    private final KeyLockManager lockManager;
    private final DeadLetterQueue deadLetterQueue;
    private final RetryPolicy forwardPolicy;
    private final RetryPolicy compensationPolicy;

    @org.springframework.beans.factory.annotation.Autowired
    public SagaOrchestrator(EventStore eventStore,
                            SagaRepository repository,
                            SagaFlowRegistry flowRegistry,
                            KeyLockManager lockManager,
                            DeadLetterQueue deadLetterQueue) {
        this(eventStore, repository, flowRegistry, lockManager, deadLetterQueue,
                RetryPolicy.forward(), RetryPolicy.compensation());
    }

    /** 可注入重试策略的构造器，主要供测试使用。 */
    public SagaOrchestrator(EventStore eventStore,
                     SagaRepository repository,
                     SagaFlowRegistry flowRegistry,
                     KeyLockManager lockManager,
                     DeadLetterQueue deadLetterQueue,
                     RetryPolicy forwardPolicy,
                     RetryPolicy compensationPolicy) {
        this.eventStore = eventStore;
        this.repository = repository;
        this.flowRegistry = flowRegistry;
        this.lockManager = lockManager;
        this.deadLetterQueue = deadLetterQueue;
        this.forwardPolicy = forwardPolicy;
        this.compensationPolicy = compensationPolicy;
    }

    // ------------------------------------------------------------------
    // 对外能力
    // ------------------------------------------------------------------

    /** 创建流程（幂等：同一 businessKey 重复创建返回既有实例状态）。 */
    public EventProcessingResult createSaga(String businessKey, String flowType) {
        validateText(businessKey, "businessKey");
        validateText(flowType, "flowType");
        SagaFlow flow = flowRegistry.get(flowType)
                .orElseThrow(() -> new SagaException(ErrorCode.VALIDATION_ERROR,
                        "unknown flowType: " + flowType));
        ReentrantLock lock = lockManager.lockFor(businessKey);
        lock.lock();
        try {
            Optional<SagaInstance> existing = repository.findById(businessKey);
            if (existing.isPresent()) {
                SagaInstance s = existing.get();
                log.info("[STATE-MOVE] businessKey={} {} (create ignored, already exists) version={}",
                        businessKey, s.getState(), s.getVersion());
                return new EventProcessingResult(EventAcceptStatus.DUPLICATE,
                        REASON_DUPLICATE + ":SAGA_ALREADY_EXISTS", s.getState(), s.getVersion());
            }
            SagaInstance instance = newSagaInstance(businessKey, flowType, flow);
            SagaEvent event = new SagaEvent(
                    UUID.randomUUID().toString(), businessKey, EventType.SAGA_CREATED,
                    -1, null, Map.of(), Instant.now());
            eventStore.append(event);
            instance.getEventIds().add(event.eventId());
            transitionTo(instance, SagaState.RUNNING, "SAGA_CREATED");
            log.info("[STATE-MOVE] businessKey={} CREATED->RUNNING reason=SAGA_CREATED version={}",
                    businessKey, instance.getVersion());
            return new EventProcessingResult(EventAcceptStatus.ACCEPTED,
                    "SAGA_CREATED", instance.getState(), instance.getVersion());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 处理一个事件。重复 / 乱序 / 冲突都会被识别并以可区分原因返回，不产生副作用。
     */
    public EventProcessingResult process(SagaEvent event) {
        if (event == null) {
            throw new SagaException(ErrorCode.VALIDATION_ERROR, "event must not be null");
        }
        validateText(event.eventId(), "eventId");
        validateText(event.businessKey(), "businessKey");
        ReentrantLock lock = lockManager.lockFor(event.businessKey());
        lock.lock();
        try {
            // 1) 幂等：eventId 去重优先，任何重复事件都不会二次执行副作用
            if (eventStore.exists(event.eventId())) {
                SagaInstance current = repository.findById(event.businessKey()).orElse(null);
                SagaState state = current == null ? null : current.getState();
                long version = current == null ? -1 : current.getVersion();
                log.info("[EVENT-RECV] businessKey={} eventId={} type={} step={} => DUPLICATE version={}",
                        event.businessKey(), event.eventId(), event.type(), event.stepIndex(), version);
                return new EventProcessingResult(EventAcceptStatus.DUPLICATE,
                        REASON_DUPLICATE, state, version);
            }

            SagaInstance saga = repository.findById(event.businessKey()).orElse(null);
            if (saga == null) {
                // 无法路由到任何流程：落死信，避免静默丢弃
                deadLetterQueue.publish(event, REASON_UNKNOWN_SAGA,
                        "no saga exists for businessKey");
                throw new SagaException(ErrorCode.NOT_FOUND,
                        "saga not found for businessKey");
            }

            // 2) 记录收到事件（尚未产生副作用）
            log.info("[EVENT-RECV] businessKey={} eventId={} type={} step={} expectedVersion={} currentVersion={}",
                    event.businessKey(), event.eventId(), event.type(),
                    event.stepIndex(), event.expectedVersion(), saga.getVersion());

            return switch (event.type()) {
                case STEP_ADVANCE -> handleAdvance(event, saga);
                case COMPENSATION_TICK -> handleCompensationTick(event, saga);
                case SAGA_CREATED -> rejectTerminalOrDuplicate(event, saga, REASON_DUPLICATE);
            };
        } finally {
            lock.unlock();
        }
    }

    /** 人工/外部触发补偿；COMPENSATION_FAILED 终态下允许再次调用以重试。 */
    public EventProcessingResult triggerCompensation(String businessKey, String reasonCode) {
        validateText(businessKey, "businessKey");
        SagaEvent tick = new SagaEvent(
                UUID.randomUUID().toString(), businessKey, EventType.COMPENSATION_TICK,
                -1, null, Map.of("reason", safe(reasonCode)), Instant.now());
        return process(tick);
    }

    public SagaInstance getSaga(String businessKey) {
        return repository.findById(businessKey)
                .orElseThrow(() -> new SagaException(ErrorCode.NOT_FOUND,
                        "saga not found for businessKey"));
    }

    public List<SagaEvent> getEventStream(String businessKey) {
        return eventStore.findByBusinessKey(businessKey);
    }

    // ------------------------------------------------------------------
    // 前向推进
    // ------------------------------------------------------------------

    private EventProcessingResult handleAdvance(SagaEvent event, SagaInstance saga) {
        SagaFlow flow = flowRegistry.get(saga.getFlowType())
                .orElseThrow(() -> {
                    deadLetterQueue.publish(event, REASON_UNKNOWN_FLOW,
                            "flow definition missing: " + saga.getFlowType());
                    return new SagaException(ErrorCode.INTERNAL_ERROR, "flow definition missing");
                });
        List<StepDefinition> steps = flow.steps();

        // 终态保护：COMPLETED/COMPENSATED 后到达的步骤事件属于过期事件
        SagaState state = saga.getState();
        if (state == SagaState.COMPLETED) {
            return reject(event, saga, EventAcceptStatus.STALE,
                    REASON_STALE + ":SAGA_ALREADY_COMPLETED");
        }
        if (state == SagaState.COMPENSATED) {
            return reject(event, saga, EventAcceptStatus.STALE,
                    REASON_STALE + ":SAGA_ALREADY_COMPENSATED");
        }
        if (state == SagaState.COMPENSATION_FAILED) {
            return reject(event, saga, EventAcceptStatus.STALE,
                    REASON_STALE + ":COMPENSATION_FAILED_AWAITING_MANUAL_INTERVENTION");
        }

        int expectedStep = saga.getCurrentStepIndex() + 1;
        if (event.stepIndex() != expectedStep) {
            // 乱序：既不是重复，也不是当前应执行步骤
            String reason = event.stepIndex() < expectedStep
                    ? REASON_STALE + ":STEP_ALREADY_DONE"
                    : REASON_OUT_OF_ORDER + ":STEP_AHEAD";
            return reject(event, saga, EventAcceptStatus.STALE, reason);
        }
        if (event.stepIndex() < 0 || event.stepIndex() >= steps.size()) {
            return reject(event, saga, EventAcceptStatus.STALE, REASON_OUT_OF_ORDER + ":STEP_OUT_OF_RANGE");
        }

        // 乐观并发：携带 expectedVersion 且不匹配 => 冲突，拒绝推进
        if (event.expectedVersion() != null && event.expectedVersion() != saga.getVersion()) {
            log.info("[CONFLICT] businessKey={} eventId={} expectedVersion={} actualVersion={}",
                    saga.getBusinessKey(), event.eventId(),
                    event.expectedVersion(), saga.getVersion());
            return reject(event, saga, EventAcceptStatus.CONFLICT, REASON_VERSION_CONFLICT);
        }

        StepDefinition step = steps.get(event.stepIndex());
        SagaStepRecord record = recordOf(saga, step);
        SagaContext context = ensureContext(saga);
        context.put("businessKey", saga.getBusinessKey());
        context.put("flowType", saga.getFlowType());
        mergePayload(context, event.payload());

        // 带退避的有界重试；内部异常被归一化为可区分原因，不外泄堆栈
        StepResult result = executeWithRetry(step, record, context, forwardPolicy, true);
        if (!result.success()) {
            return onForwardFailure(event, saga, flow, result);
        }

        // 副作用已成功 => 记录事件并推进状态机（二者均在锁内）
        record.setStatus(StepStatus.COMPLETED);
        record.touch();
        saga.setCurrentStepIndex(event.stepIndex());
        eventStore.append(event);
        saga.getEventIds().add(event.eventId());

        SagaState target;
        String moveReason;
        if (event.stepIndex() == steps.size() - 1) {
            target = SagaState.COMPLETED;
            moveReason = "ALL_STEPS_DONE";
        } else {
            target = SagaState.RUNNING;
            moveReason = "STEP_DONE";
        }
        transitionTo(saga, target, moveReason);
        log.info("[STATE-MOVE] businessKey={} {}->{} step={} reason={} version={}",
                saga.getBusinessKey(), state, target, event.stepIndex(), moveReason,
                saga.getVersion());
        return new EventProcessingResult(EventAcceptStatus.ACCEPTED,
                moveReason, target, saga.getVersion());
    }

    private EventProcessingResult onForwardFailure(SagaEvent event, SagaInstance saga,
                                                   SagaFlow flow, StepResult failed) {
        SagaStepRecord record = recordOf(saga, flow.steps().get(event.stepIndex()));
        record.setStatus(StepStatus.FAILED);
        record.touch();
        saga.setFailureReason(failed.reasonCode());
        eventStore.append(event);
        saga.getEventIds().add(event.eventId());
        transitionTo(saga, SagaState.COMPENSATING, "FORWARD_FAILED:" + failed.reasonCode());
        log.warn("[STEP-FAIL] businessKey={} step={} reason={} => begin compensation version={}",
                saga.getBusinessKey(), event.stepIndex(), failed.reasonCode(), saga.getVersion());

        // 立即反向补偿已完成步骤
        boolean healthy = runCompensation(saga, flow, failed.reasonCode());
        if (healthy) {
            return new EventProcessingResult(EventAcceptStatus.COMPENSATED,
                    "COMPENSATED_AFTER_FAILURE:" + failed.reasonCode(),
                    saga.getState(), saga.getVersion());
        }
        // 补偿失败的失败事件本身已落死信（在 runCompensation 内）
        return new EventProcessingResult(EventAcceptStatus.DEAD_LETTER,
                failed.reasonCode(), saga.getState(), saga.getVersion());
    }

    // ------------------------------------------------------------------
    // 补偿
    // ------------------------------------------------------------------

    private EventProcessingResult handleCompensationTick(SagaEvent event, SagaInstance saga) {
        SagaFlow flow = flowRegistry.get(saga.getFlowType())
                .orElseThrow(() -> {
                    deadLetterQueue.publish(event, REASON_UNKNOWN_FLOW,
                            "flow definition missing: " + saga.getFlowType());
                    return new SagaException(ErrorCode.INTERNAL_ERROR, "flow definition missing");
                });
        SagaState state = saga.getState();
        if (state == SagaState.COMPLETED) {
            return reject(event, saga, EventAcceptStatus.STALE, REASON_STALE + ":SAGA_ALREADY_COMPLETED");
        }
        if (state == SagaState.COMPENSATED) {
            return reject(event, saga, EventAcceptStatus.DUPLICATE, REASON_DUPLICATE + ":ALREADY_COMPENSATED");
        }
        // RUNNING（人工显式补偿）与 COMPENSATION_FAILED（人工介入重试）允许进入
        eventStore.append(event);
        saga.getEventIds().add(event.eventId());
        if (state == SagaState.RUNNING || state == SagaState.CREATED) {
            transitionTo(saga, SagaState.COMPENSATING, "MANUAL_COMPENSATION");
        }
        // 已处于 COMPENSATING / COMPENSATION_FAILED 时保持版本不变（同态不产生版本迁移）
        boolean healthy = runCompensation(saga, flow,
                String.valueOf(event.payload().getOrDefault("reason", "MANUAL")));
        if (healthy) {
            return new EventProcessingResult(EventAcceptStatus.COMPENSATED,
                    "COMPENSATED", saga.getState(), saga.getVersion());
        }
        return new EventProcessingResult(EventAcceptStatus.DEAD_LETTER,
                REASON_COMPENSATION_EXHAUSTED, saga.getState(), saga.getVersion());
    }

    /**
     * 按与执行相反的顺序补偿所有 COMPLETED 步骤。
     * 每个补偿动作有独立的有界重试；任一步骤耗尽 => COMPENSATION_FAILED 终态 + 死信。
     * @return true 表示全部补偿完成进入 COMPENSATED；false 表示进入 COMPENSATION_FAILED
     */
    private boolean runCompensation(SagaInstance saga, SagaFlow flow, String triggerReason) {
        List<StepDefinition> steps = flow.steps();
        SagaContext context = ensureContext(saga);

        for (int i = steps.size() - 1; i >= 0; i--) {
            StepDefinition step = steps.get(i);
            SagaStepRecord record = recordOf(saga, step);
            // COMPLETED：本轮待补偿；
            // FAILED + compensationPoison：上一轮补偿耗尽、人工介入后的重试起点。
            boolean poisonRetry = record.getStatus() == StepStatus.FAILED && record.isCompensationPoison();
            if (record.getStatus() != StepStatus.COMPLETED && !poisonRetry) {
                continue;
            }
            record.setStatus(StepStatus.COMPENSATING);
            record.touch();

            StepResult result = executeWithRetry(step, record, context, compensationPolicy, false);
            if (!result.success()) {
                record.setStatus(StepStatus.FAILED);
                record.setCompensationPoison(true);
                record.touch();
                saga.setFailureReason(result.reasonCode());
                transitionTo(saga, SagaState.COMPENSATION_FAILED, "COMPENSATION_RETRY_EXHAUSTED");
                // 构造一条可追溯的补偿失败事件并落死信，等待人工介入
                SagaEvent poison = new SagaEvent(
                        UUID.randomUUID().toString(), saga.getBusinessKey(),
                        EventType.COMPENSATION_TICK, i, null,
                        Map.of("failedStep", step.name(),
                                "reason", safe(result.reasonCode()),
                                "attempts", record.getCompensationAttempts()),
                        Instant.now());
                deadLetterQueue.publish(poison, REASON_COMPENSATION_EXHAUSTED,
                        "step=" + step.name() + " cause=" + safe(result.reasonCode())
                                + " trigger=" + triggerReason);
                log.error("[STATE-MOVE] businessKey={} ->COMPENSATION_FAILED step={} attempts={} "
                                + "compensationCount={} MANUAL_INTERVENTION_REQUIRED",
                        saga.getBusinessKey(), step.name(),
                        record.getCompensationAttempts(), totalCompensationAttempts(saga));
                return false;
            }
            record.setStatus(StepStatus.COMPENSATED);
            record.setCompensationPoison(false);
            record.touch();
            log.info("[COMPENSATE-OK] businessKey={} step={} attempts={} compensationCount={}",
                    saga.getBusinessKey(), step.name(),
                    record.getCompensationAttempts(), totalCompensationAttempts(saga));
        }

        transitionTo(saga, SagaState.COMPENSATED, triggerReason);
        log.info("[STATE-MOVE] businessKey={} ->COMPENSATED reason={} totalCompensationCount={}",
                saga.getBusinessKey(), triggerReason,
                totalCompensationAttempts(saga));
        return true;
    }

    // ------------------------------------------------------------------
    // 步骤执行（有界重试 + 异常归一化）
    // ------------------------------------------------------------------

    private StepResult executeWithRetry(StepDefinition step, SagaStepRecord record,
                                        SagaContext context, RetryPolicy policy, boolean forward) {
        String kind = forward ? "FORWARD" : "COMPENSATE";
        StepResult last = StepResult.fail(kind + "_NO_ATTEMPT", "no attempt was made");
        for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
            if (forward) {
                record.incrementForwardAttempts();
            } else {
                record.incrementCompensationAttempts();
            }
            try {
                StepResult result = forward
                        ? step.action().doStep(context)
                        : step.action().compensate(context);
                if (result != null && result.success()) {
                    log.info("[STEP-ATTEMPT] step={} kind={} attempt={} => SUCCESS",
                            step.name(), kind, attempt);
                    return StepResult.ok();
                }
                last = result == null
                        ? StepResult.fail(kind + "_NULL_RESULT", "action returned null")
                        : result;
                log.warn("[RETRY] step={} kind={} attempt={}/{} reason={}",
                        step.name(), kind, attempt, policy.maxAttempts(), last.reasonCode());
            } catch (RuntimeException ex) {
                // 内部异常细节不外泄，仅在服务端日志归类记录
                last = StepResult.fail(kind + "_EXCEPTION",
                        ex.getClass().getSimpleName() + ": " + ex.getMessage());
                log.warn("[RETRY] step={} kind={} attempt={}/{} exception={} message={}",
                        step.name(), kind, attempt, policy.maxAttempts(),
                        ex.getClass().getSimpleName(), ex.getMessage());
            }
            if (attempt < policy.maxAttempts()) {
                sleep(policy.initialBackoff().toMillis() * attempt);
            }
        }
        // 重试耗尽：超时等原因保留在 last.reasonCode 中，对外统一归一化为 *_RETRY_EXHAUSTED
        String exhausted = forward ? REASON_RETRY_EXHAUSTED : REASON_COMPENSATION_EXHAUSTED;
        return StepResult.fail(exhausted,
                kind + " failed after " + policy.maxAttempts() + " attempts; last="
                        + safe(last.reasonCode()));
    }

    // ------------------------------------------------------------------
    // 辅助方法
    // ------------------------------------------------------------------

    private EventProcessingResult reject(SagaEvent event, SagaInstance saga,
                                         EventAcceptStatus status, String reason) {
        log.info("[EVENT-REJECT] businessKey={} eventId={} type={} step={} status={} reason={} version={}",
                saga.getBusinessKey(), event.eventId(), event.type(),
                event.stepIndex(), status, reason, saga.getVersion());
        return new EventProcessingResult(status, reason, saga.getState(), saga.getVersion());
    }

    private EventProcessingResult rejectTerminalOrDuplicate(SagaEvent event, SagaInstance saga, String reason) {
        return reject(event, saga, EventAcceptStatus.DUPLICATE, reason);
    }

    private SagaInstance newSagaInstance(String businessKey, String flowType, SagaFlow flow) {
        SagaInstance instance = new SagaInstance();
        instance.setBusinessKey(businessKey);
        instance.setFlowType(flowType);
        instance.setState(SagaState.CREATED);
        instance.setCreatedAt(Instant.now());
        instance.setContext(new SagaContext());
        for (StepDefinition def : flow.steps()) {
            instance.getSteps().put(def.index(), new SagaStepRecord(def.index(), def.name()));
        }
        return repository.save(instance);
    }

    private SagaContext ensureContext(SagaInstance saga) {
        if (saga.getContext() == null) {
            saga.setContext(new SagaContext());
        }
        return saga.getContext();
    }

    private SagaStepRecord recordOf(SagaInstance saga, StepDefinition step) {
        return saga.getSteps().computeIfAbsent(step.index(),
                idx -> new SagaStepRecord(idx, step.name()));
    }

    private void mergePayload(SagaContext context, Map<String, Object> payload) {
        if (payload != null) {
            payload.forEach((k, v) -> {
                if (k != null) {
                    context.put(k, v);
                }
            });
        }
    }

    private void persist(SagaInstance saga, SagaState expectedFrom, SagaState to) {
        saga.setState(to);
        saga.nextVersion();
        saga.touch();
        repository.save(saga);
        log.debug("[PERSIST] businessKey={} {}->{} version={}",
                saga.getBusinessKey(), expectedFrom, to, saga.getVersion());
    }

    /** 真正的状态迁移：升版本并落库；状态相同则仅落库不升版本。 */
    private void transitionTo(SagaInstance saga, SagaState to, String reason) {
        SagaState from = saga.getState();
        saga.setState(to);
        if (from != to) {
            saga.nextVersion();
        }
        saga.touch();
        repository.save(saga);
        log.info("[STATE-MOVE] businessKey={} {}->{} reason={} version={}",
                saga.getBusinessKey(), from, to, reason, saga.getVersion());
    }

    private int totalCompensationAttempts(SagaInstance saga) {
        return saga.getSteps().values().stream()
                .mapToInt(SagaStepRecord::getCompensationAttempts)
                .sum();
    }

    private void validateText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new SagaException(ErrorCode.VALIDATION_ERROR, field + " must not be blank");
        }
    }

    private String safe(String s) {
        return s == null ? "" : s;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
