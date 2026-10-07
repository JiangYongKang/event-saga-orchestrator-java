package com.github.highcumontoa.saga.core;

import com.github.highcumontoa.saga.domain.ManualInterventionEntry;
import com.github.highcumontoa.saga.domain.ManualInterventionStatus;
import com.github.highcumontoa.saga.domain.SagaDefinition;
import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.domain.SagaStatus;
import com.github.highcumontoa.saga.domain.StepContext;
import com.github.highcumontoa.saga.domain.StepDefinition;
import com.github.highcumontoa.saga.domain.StepRecord;
import com.github.highcumontoa.saga.domain.StepStatus;
import com.github.highcumontoa.saga.error.ErrorCategory;
import com.github.highcumontoa.saga.error.SagaException;
import com.github.highcumontoa.saga.event.AckReason;
import com.github.highcumontoa.saga.event.EventAck;
import com.github.highcumontoa.saga.event.SagaEvent;
import com.github.highcumontoa.saga.store.ManualInterventionStore;
import com.github.highcumontoa.saga.store.SagaStore;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 默认编排器实现。
 *
 * <p>核心保证：
 * <ul>
 *   <li>幂等：已应用的 eventId 再次到达时不重复产生副作用，返回 DUPLICATE_EVENT。</li>
 *   <li>串行化：同一业务键的所有操作在同一监视器锁下执行，并发不会重复推进或状态回退。</li>
 *   <li>乱序：事件步骤名必须等于当前待推进步骤，否则拒绝且不改变状态。</li>
 *   <li>补偿：失败时按相反顺序回滚已完成步骤；补偿重试耗尽后进入 MANUAL_INTERVENTION 终态。</li>
 * </ul>
 */
@Component
public class DefaultSagaOrchestrator implements SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(DefaultSagaOrchestrator.class);

    /** 补偿失败时的最大尝试次数（含首次）。 */
    static final int MAX_COMPENSATION_ATTEMPTS = 3;

    private final SagaStore store;
    private final SagaDefinitionRegistry registry;
    private final ManualInterventionStore interventionStore;
    /** 按业务键/流程 ID 的锁表，将同一流程的操作串行化。 */
    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    /** 带时间预算的步骤动作执行线程池（守护线程，不阻塞进程退出）。 */
    private final ExecutorService stepExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "saga-step-timeout");
        t.setDaemon(true);
        return t;
    });

    public DefaultSagaOrchestrator(SagaStore store, SagaDefinitionRegistry registry,
                                   ManualInterventionStore interventionStore) {
        this.store = store;
        this.registry = registry;
        this.interventionStore = interventionStore;
    }

    @Override
    public String createSaga(String definitionName, String businessKey) {
        if (definitionName == null || definitionName.isBlank()
                || businessKey == null || businessKey.isBlank()) {
            throw new SagaException(ErrorCategory.VALIDATION, "definitionName 与 businessKey 不能为空");
        }
        SagaDefinition definition = registry.find(definitionName)
                .orElseThrow(() -> new SagaException(ErrorCategory.VALIDATION, "未知的流程定义"));
        synchronized (lockFor("biz:" + businessKey)) {
            var existing = store.findByBusinessKey(businessKey);
            if (existing.isPresent()) {
                log.info("[saga] create 幂等命中 businessKey={} -> 已有 sagaId={}",
                        businessKey, existing.get().getSagaId());
                return existing.get().getSagaId();
            }
            String sagaId = UUID.randomUUID().toString();
            List<StepRecord> steps = new ArrayList<>();
            for (StepDefinition def : definition.steps()) {
                steps.add(new StepRecord(def.name()));
            }
            SagaInstance instance = new SagaInstance(
                    sagaId, businessKey, definitionName, steps, new ConcurrentHashMap<>());
            store.save(instance);
            log.info("[saga] 创建 sagaId={} businessKey={} definition={} 状态={}",
                    sagaId, businessKey, definitionName, instance.getStatus());
            return sagaId;
        }
    }

    @Override
    public EventAck handleEvent(SagaEvent event) {
        validate(event);
        SagaInstance instance = store.findByBusinessKey(event.businessKey())
                .orElseThrow(() -> new SagaException(ErrorCategory.NOT_FOUND, "流程不存在"));
        synchronized (lockFor("saga:" + instance.getSagaId())) {
            log.info("[event] 收到 eventId={} type={} step={} businessKey={} 当前状态={} 当前步骤下标={}",
                    event.eventId(), event.type(), event.stepName(),
                    event.businessKey(), instance.getStatus(), instance.getCurrentStepIndex());

            // 1) 幂等：同一 eventId 不重复产生副作用
            if (store.isEventProcessed(event.eventId())) {
                log.info("[event] eventId={} 判定为重复投递，忽略副作用", event.eventId());
                return new EventAck(false, AckReason.DUPLICATE_EVENT, instance.getStatus());
            }
            // 2) 终态不再接受推进
            if (isTerminal(instance.getStatus())) {
                log.info("[event] eventId={} 流程已处于终态 {}，忽略", event.eventId(), instance.getStatus());
                return new EventAck(false, AckReason.IGNORED_TERMINAL_STATE, instance.getStatus());
            }

            SagaDefinition definition = registry.find(instance.getDefinitionName())
                    .orElseThrow(() -> new SagaException(ErrorCategory.INTERNAL, "流程定义缺失"));
            int stepIndex = indexOfStep(definition, event.stepName());
            if (stepIndex < 0) {
                log.warn("[event] eventId={} 未知步骤 {}，拒绝", event.eventId(), event.stepName());
                return new EventAck(false, AckReason.REJECTED_UNKNOWN_STEP, instance.getStatus());
            }
            // 3) 乱序/过期：必须是当前待推进步骤
            if (stepIndex != instance.getCurrentStepIndex()) {
                log.info("[event] eventId={} 乱序：期望步骤下标={} 实际={}，不迁移状态",
                        event.eventId(), instance.getCurrentStepIndex(), stepIndex);
                return new EventAck(false, AckReason.OUT_OF_ORDER, instance.getStatus());
            }

            // 4) 应用事件，驱动状态机迁移
            EventAck ack = switch (event.type()) {
                case STEP_SUCCEEDED -> applyStepSucceeded(instance, definition, stepIndex);
                case STEP_FAILED -> applyStepFailed(instance, definition, stepIndex);
            };
            store.markEventProcessed(event.eventId());
            log.info("[event] eventId={} 已应用，迁移后状态={}", event.eventId(), instance.getStatus());
            return ack;
        }
    }

    @Override
    public SagaInstance getSaga(String sagaId) {
        return store.findById(sagaId)
                .orElseThrow(() -> new SagaException(ErrorCategory.NOT_FOUND, "流程不存在"));
    }

    @Override
    public SagaInstance reprocess(String sagaId) {
        if (sagaId == null || sagaId.isBlank()) {
            throw new SagaException(ErrorCategory.VALIDATION, "sagaId 不能为空");
        }
        SagaInstance instance = getSaga(sagaId);
        synchronized (lockFor("saga:" + instance.getSagaId())) {
            if (instance.getStatus() != SagaStatus.MANUAL_INTERVENTION) {
                throw new SagaException(ErrorCategory.CONFLICT, "流程未处于人工介入状态，不能重新处理");
            }
            SagaDefinition definition = registry.find(instance.getDefinitionName())
                    .orElseThrow(() -> new SagaException(ErrorCategory.INTERNAL, "流程定义缺失"));
            log.info("[reprocess] sagaId={} 人工确认后重新发起补偿，当前步骤下标={}",
                    sagaId, instance.getCurrentStepIndex());
            // 给尚未回滚的步骤一轮新的重试预算；已 COMPENSATED 的步骤不动
            for (StepRecord record : instance.getSteps()) {
                if (record.getStatus() == StepStatus.EXECUTED) {
                    record.resetCompensationAttempts();
                }
            }
            interventionStore.findBySagaId(sagaId).ifPresent(entry -> {
                entry.incrementReprocessAttempts();
                entry.setUpdatedAt(Instant.now());
                interventionStore.upsert(entry);
            });
            // 复用反向补偿循环：自动跳过已成功回滚的步骤，保持相反顺序
            compensate(instance, definition);
            if (instance.getStatus() == SagaStatus.COMPENSATED) {
                interventionStore.findBySagaId(sagaId).ifPresent(entry -> {
                    entry.setStatus(ManualInterventionStatus.RESOLVED);
                    entry.setUpdatedAt(Instant.now());
                    interventionStore.upsert(entry);
                    log.info("[reprocess] sagaId={} 补偿完成，清单条目标记为已处理", sagaId);
                });
            } else {
                log.warn("[reprocess] sagaId={} 重投后仍未完成补偿，保持人工介入终态", sagaId);
            }
            return instance;
        }
    }

    // ---- 状态机迁移 ----

    private EventAck applyStepSucceeded(SagaInstance instance, SagaDefinition definition, int stepIndex) {
        StepDefinition stepDef = definition.steps().get(stepIndex);
        StepRecord record = instance.getSteps().get(stepIndex);
        transition(instance, SagaStatus.RUNNING, "步骤 " + stepDef.name() + " 成功事件被接受");
        try {
            runStepAction(stepDef, new StepContext(instance.getSagaId(), instance.getBusinessKey(),
                    stepDef.name(), instance.getData()));
        } catch (StepTimeoutException e) {
            // 超时与业务失败区分记录；内部细节仅写日志，对外只暴露类别
            record.setFailureCategory(ErrorCategory.TIMEOUT);
            log.error("[step] 步骤 {} 超出时间预算 {}，按失败处理转入补偿（TIMEOUT）",
                    stepDef.name(), stepDef.timeout());
            return compensate(instance, definition);
        } catch (Exception e) {
            // 正向动作业务失败：内部细节仅写日志，对外不泄露；按失败处理进入补偿，避免停留在中间态
            record.setFailureCategory(ErrorCategory.BUSINESS_FAILURE);
            log.error("[step] 步骤 {} 正向动作业务失败，转入补偿（BUSINESS_FAILURE）。原因={}",
                    stepDef.name(), e.toString());
            return compensate(instance, definition);
        }
        record.setStatus(StepStatus.EXECUTED);
        instance.setCurrentStepIndex(stepIndex + 1);
        if (stepIndex + 1 == definition.steps().size()) {
            transition(instance, SagaStatus.COMPLETED, "全部步骤完成");
        }
        return new EventAck(true, AckReason.APPLIED, instance.getStatus());
    }

    /**
     * 执行步骤正向动作；设置了时间预算时在独立线程执行并限时等待，
     * 超时抛出 {@link StepTimeoutException} 与业务异常区分。
     */
    private void runStepAction(StepDefinition stepDef, StepContext context) throws Exception {
        if (stepDef.timeout() == null) {
            stepDef.action().run(context);
            return;
        }
        Future<?> future = stepExecutor.submit(() -> {
            try {
                stepDef.action().run(context);
            } catch (Exception e) {
                throw new ActionFailedException(e);
            }
        });
        try {
            future.get(stepDef.timeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new StepTimeoutException("步骤 " + stepDef.name() + " 执行超时");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ActionFailedException wrapper) {
                throw wrapper.cause;
            }
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /** 包装受检异常以便跨线程传递（内部使用）。 */
    private static final class ActionFailedException extends RuntimeException {
        private final Exception cause;

        ActionFailedException(Exception cause) {
            super(cause);
            this.cause = cause;
        }
    }

    private EventAck applyStepFailed(SagaInstance instance, SagaDefinition definition, int stepIndex) {
        StepDefinition stepDef = definition.steps().get(stepIndex);
        // 下游回报的业务失败，与超时区分记录
        instance.getSteps().get(stepIndex).setFailureCategory(ErrorCategory.BUSINESS_FAILURE);
        log.info("[step] 步骤 {} 失败事件被接受（BUSINESS_FAILURE），开始补偿", stepDef.name());
        return compensate(instance, definition);
    }

    /** 按与执行相反的顺序补偿已完成步骤；重试耗尽进入人工介入终态。 */
    private EventAck compensate(SagaInstance instance, SagaDefinition definition) {
        transition(instance, SagaStatus.COMPENSATING, "触发补偿，按相反顺序回滚");
        for (int i = instance.getCurrentStepIndex() - 1; i >= 0; i--) {
            StepRecord record = instance.getSteps().get(i);
            if (record.getStatus() != StepStatus.EXECUTED) {
                continue;
            }
            StepDefinition stepDef = definition.steps().get(i);
            if (!compensateStepWithRetry(instance, stepDef, record)) {
                transition(instance, SagaStatus.MANUAL_INTERVENTION,
                        "步骤 " + stepDef.name() + " 补偿重试 " + MAX_COMPENSATION_ATTEMPTS
                                + " 次仍失败（RETRY_EXHAUSTED），进入人工介入终态");
                recordIntervention(instance, stepDef, record);
                return new EventAck(true, AckReason.APPLIED, instance.getStatus());
            }
        }
        transition(instance, SagaStatus.COMPENSATED, "全部已完成步骤补偿成功");
        return new EventAck(true, AckReason.APPLIED, instance.getStatus());
    }

    /**
     * 登记/更新人工介入清单条目：同一流程按 sagaId 去重，
     * 重复进入只更新卡住的步骤、尝试次数与时间，不叠加新记录。
     */
    private void recordIntervention(SagaInstance instance, StepDefinition stepDef, StepRecord record) {
        String reason = "步骤 " + stepDef.name() + " 补偿重试 " + MAX_COMPENSATION_ATTEMPTS + " 次仍失败";
        ManualInterventionEntry entry = interventionStore.findBySagaId(instance.getSagaId())
                .orElseGet(() -> new ManualInterventionEntry(instance.getSagaId(),
                        instance.getBusinessKey(), instance.getDefinitionName(), stepDef.name(),
                        ErrorCategory.RETRY_EXHAUSTED, reason, record.getCompensationAttempts(),
                        Instant.now()));
        entry.setStuckStepName(stepDef.name());
        entry.setFailureReason(reason);
        entry.setCompensationAttempts(record.getCompensationAttempts());
        entry.setStatus(ManualInterventionStatus.PENDING);
        entry.setUpdatedAt(Instant.now());
        interventionStore.upsert(entry);
        log.info("[intervention] sagaId={} 登记人工介入清单：卡住步骤={} 已补偿尝试={} 次",
                instance.getSagaId(), stepDef.name(), record.getCompensationAttempts());
    }

    private boolean compensateStepWithRetry(SagaInstance instance, StepDefinition stepDef, StepRecord record) {
        while (record.getCompensationAttempts() < MAX_COMPENSATION_ATTEMPTS) {
            record.incrementCompensationAttempts();
            try {
                stepDef.compensation().run(new StepContext(instance.getSagaId(),
                        instance.getBusinessKey(), stepDef.name(), instance.getData()));
                record.setStatus(StepStatus.COMPENSATED);
                log.info("[compensate] 步骤 {} 第 {} 次补偿成功", stepDef.name(), record.getCompensationAttempts());
                return true;
            } catch (Exception e) {
                log.warn("[compensate] 步骤 {} 第 {} 次补偿失败：{}", stepDef.name(),
                        record.getCompensationAttempts(), e.toString());
            }
        }
        return false;
    }

    // ---- 工具 ----

    private void transition(SagaInstance instance, SagaStatus target, String reason) {
        SagaStatus from = instance.getStatus();
        if (from == target) {
            return;
        }
        instance.setStatus(target);
        log.info("[state] sagaId={} 状态迁移 {} -> {}，依据：{}", instance.getSagaId(), from, target, reason);
    }

    private boolean isTerminal(SagaStatus status) {
        return status == SagaStatus.COMPLETED || status == SagaStatus.COMPENSATED
                || status == SagaStatus.MANUAL_INTERVENTION;
    }

    private int indexOfStep(SagaDefinition definition, String stepName) {
        for (int i = 0; i < definition.steps().size(); i++) {
            if (definition.steps().get(i).name().equals(stepName)) {
                return i;
            }
        }
        return -1;
    }

    private Object lockFor(String key) {
        return locks.computeIfAbsent(key, k -> new Object());
    }

    @PreDestroy
    void shutdown() {
        stepExecutor.shutdownNow();
    }

    private void validate(SagaEvent event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()
                || event.businessKey() == null || event.businessKey().isBlank()
                || event.stepName() == null || event.stepName().isBlank()
                || event.type() == null) {
            throw new SagaException(ErrorCategory.VALIDATION, "事件缺少必填字段");
        }
    }
}
