package com.github.highcumontoa.saga.core;

import com.github.highcumontoa.saga.domain.SagaDefinition;
import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.domain.SagaStatus;
import com.github.highcumontoa.saga.domain.StepContext;
import com.github.highcumontoa.saga.domain.StepDefinition;
import com.github.highcumontoa.saga.domain.StepFailureKind;
import com.github.highcumontoa.saga.domain.StepRecord;
import com.github.highcumontoa.saga.domain.StepStatus;
import com.github.highcumontoa.saga.error.ErrorCategory;
import com.github.highcumontoa.saga.error.SagaException;
import com.github.highcumontoa.saga.event.AckReason;
import com.github.highcumontoa.saga.event.EventAck;
import com.github.highcumontoa.saga.event.SagaEvent;
import com.github.highcumontoa.saga.manual.InMemoryManualInterventionStore;
import com.github.highcumontoa.saga.manual.ManualInterventionEntry;
import com.github.highcumontoa.saga.manual.ManualInterventionStatus;
import com.github.highcumontoa.saga.manual.ManualInterventionStore;
import com.github.highcumontoa.saga.store.SagaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
    private final ManualInterventionStore manualInterventionStore;
    /** 按业务键/流程 ID 的锁表，将同一流程的操作串行化。 */
    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    /** 执行带时间预算的步骤动作的线程池（守护线程，不阻止进程退出）。 */
    private final ExecutorService stepExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "saga-step-worker");
        t.setDaemon(true);
        return t;
    });

    @Autowired
    public DefaultSagaOrchestrator(SagaStore store, SagaDefinitionRegistry registry,
                                   ManualInterventionStore manualInterventionStore) {
        this.store = store;
        this.registry = registry;
        this.manualInterventionStore = manualInterventionStore;
    }

    /** 便于测试的构造：使用独立的进程内人工介入清单。 */
    public DefaultSagaOrchestrator(SagaStore store, SagaDefinitionRegistry registry) {
        this(store, registry, new InMemoryManualInterventionStore());
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
    public List<ManualInterventionEntry> listManualInterventions(String businessKey,
                                                                 ManualInterventionStatus status) {
        return manualInterventionStore.query(businessKey, status);
    }

    @Override
    public ManualInterventionEntry reprocessManualIntervention(String entryId) {
        ManualInterventionEntry entry = manualInterventionStore.findById(entryId)
                .orElseThrow(() -> new SagaException(ErrorCategory.NOT_FOUND, "人工介入条目不存在"));
        SagaInstance instance = store.findById(entry.getSagaId())
                .orElseThrow(() -> new SagaException(ErrorCategory.INTERNAL, "流程实例缺失"));
        synchronized (lockFor("saga:" + instance.getSagaId())) {
            if (entry.getStatus() == ManualInterventionStatus.RESOLVED) {
                throw new SagaException(ErrorCategory.CONFLICT, "条目已处理完成，无需重新处理");
            }
            if (instance.getStatus() != SagaStatus.MANUAL_INTERVENTION) {
                throw new SagaException(ErrorCategory.CONFLICT, "流程不在人工介入状态，无法重新处理");
            }
            SagaDefinition definition = registry.find(instance.getDefinitionName())
                    .orElseThrow(() -> new SagaException(ErrorCategory.INTERNAL, "流程定义缺失"));
            entry.incrementReprocessCount();
            entry.setUpdatedAt(java.time.Instant.now());
            log.info("[manual] 重新处理 entryId={} sagaId={} 第 {} 次重投，状态保持 {} 不回退中间态",
                    entryId, instance.getSagaId(), entry.getReprocessCount(), instance.getStatus());

            // 只补偿仍处于 EXECUTED 的步骤，保持相反顺序；已 COMPENSATED 的步骤跳过不重复补偿
            for (int i = instance.getCurrentStepIndex() - 1; i >= 0; i--) {
                StepRecord record = instance.getSteps().get(i);
                if (record.getStatus() != StepStatus.EXECUTED) {
                    continue;
                }
                StepDefinition stepDef = definition.steps().get(i);
                if (!compensateStepWithRetry(instance, stepDef, record, MAX_COMPENSATION_ATTEMPTS)) {
                    // 仍失败：保持 MANUAL_INTERVENTION 终态稳定，不重复建条目，仅更新快照
                    entry.setCompensationAttempts(record.getCompensationAttempts());
                    entry.setUpdatedAt(java.time.Instant.now());
                    log.warn("[manual] 重新处理仍失败 entryId={} 卡住步骤={} 累计补偿尝试={}，保持终态 {}",
                            entryId, stepDef.name(), record.getCompensationAttempts(),
                            instance.getStatus());
                    return entry;
                }
            }
            transition(instance, SagaStatus.COMPENSATED, "人工重新处理后全部补偿完成");
            entry.setStatus(ManualInterventionStatus.RESOLVED);
            entry.setUpdatedAt(java.time.Instant.now());
            log.info("[manual] 重新处理成功 entryId={} sagaId={} 状态={}",
                    entryId, instance.getSagaId(), instance.getStatus());
            return entry;
        }
    }

    @Override
    public void removeResolvedManualIntervention(String entryId) {
        ManualInterventionEntry entry = manualInterventionStore.findById(entryId)
                .orElseThrow(() -> new SagaException(ErrorCategory.NOT_FOUND, "人工介入条目不存在"));
        if (entry.getStatus() != ManualInterventionStatus.RESOLVED) {
            throw new SagaException(ErrorCategory.CONFLICT, "仅允许清理已处理完成的条目");
        }
        manualInterventionStore.remove(entryId);
        log.info("[manual] 清理已处理条目 entryId={} sagaId={}", entryId, entry.getSagaId());
    }

    // ---- 状态机迁移 ----

    private EventAck applyStepSucceeded(SagaInstance instance, SagaDefinition definition, int stepIndex) {
        StepDefinition stepDef = definition.steps().get(stepIndex);
        StepRecord record = instance.getSteps().get(stepIndex);
        transition(instance, SagaStatus.RUNNING, "步骤 " + stepDef.name() + " 成功事件被接受");
        StepContext ctx = new StepContext(instance.getSagaId(), instance.getBusinessKey(),
                stepDef.name(), instance.getData());
        try {
            runActionWithBudget(stepDef, ctx);
        } catch (StepTimedOutException e) {
            // 超时与业务失败可区分：记录失败种类，对外暴露 TIMEOUT 类别，随后按失败走补偿
            record.setLastFailureKind(StepFailureKind.TIMEOUT);
            instance.setFailureCategory(ErrorCategory.TIMEOUT);
            instance.setFailureStep(stepDef.name());
            log.error("[step] 步骤 {} 执行超过时间预算 {}ms，按失败处理转入补偿",
                    stepDef.name(), stepDef.timeout().toMillis());
            return compensate(instance, definition);
        } catch (Exception e) {
            // 正向动作失败：内部细节仅写日志，对外不泄露；按失败处理进入补偿，避免停留在中间态
            record.setLastFailureKind(StepFailureKind.BUSINESS_FAILURE);
            log.error("[step] 步骤 {} 正向动作失败，转入补偿。原因={}", stepDef.name(), e.toString());
            return compensate(instance, definition);
        }
        record.setStatus(StepStatus.EXECUTED);
        instance.setCurrentStepIndex(stepIndex + 1);
        if (stepIndex + 1 == definition.steps().size()) {
            transition(instance, SagaStatus.COMPLETED, "全部步骤完成");
        }
        return new EventAck(true, AckReason.APPLIED, instance.getStatus());
    }

    /** 内部信号：步骤执行超出时间预算。不对外暴露。 */
    private static final class StepTimedOutException extends Exception {
    }

    /** 在步骤时间预算内执行正向动作；未设置预算则直接执行。 */
    private void runActionWithBudget(StepDefinition stepDef, StepContext ctx) throws Exception {
        if (stepDef.timeout() == null) {
            stepDef.action().run(ctx);
            return;
        }
        Future<?> future = stepExecutor.submit(() -> {
            try {
                stepDef.action().run(ctx);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            future.get(stepDef.timeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new StepTimedOutException();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException re && re.getCause() instanceof Exception orig) {
                throw orig;
            }
            throw new RuntimeException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("步骤执行被中断", e);
        }
    }

    private EventAck applyStepFailed(SagaInstance instance, SagaDefinition definition, int stepIndex) {
        log.info("[step] 步骤 {} 失败事件被接受，开始补偿", definition.steps().get(stepIndex).name());
        return compensate(instance, definition);
    }

    /** 按与执行相反的顺序补偿已完成步骤；重试耗尽进入人工介入终态并登记清单。 */
    private EventAck compensate(SagaInstance instance, SagaDefinition definition) {
        transition(instance, SagaStatus.COMPENSATING, "触发补偿，按相反顺序回滚");
        for (int i = instance.getCurrentStepIndex() - 1; i >= 0; i--) {
            StepRecord record = instance.getSteps().get(i);
            if (record.getStatus() != StepStatus.EXECUTED) {
                continue;
            }
            StepDefinition stepDef = definition.steps().get(i);
            if (!compensateStepWithRetry(instance, stepDef, record, MAX_COMPENSATION_ATTEMPTS)) {
                instance.setFailureCategory(ErrorCategory.RETRY_EXHAUSTED);
                instance.setFailureStep(stepDef.name());
                transition(instance, SagaStatus.MANUAL_INTERVENTION,
                        "步骤 " + stepDef.name() + " 补偿重试 " + MAX_COMPENSATION_ATTEMPTS
                                + " 次仍失败（RETRY_EXHAUSTED），进入人工介入终态");
                registerManualIntervention(instance, stepDef, record);
                return new EventAck(true, AckReason.APPLIED, instance.getStatus());
            }
        }
        transition(instance, SagaStatus.COMPENSATED, "全部已完成步骤补偿成功");
        return new EventAck(true, AckReason.APPLIED, instance.getStatus());
    }

    /** 登记人工介入清单：同一流程只保留一条，重复进入仅更新快照，不叠加记录。 */
    private void registerManualIntervention(SagaInstance instance, StepDefinition stepDef, StepRecord record) {
        var existing = manualInterventionStore.findBySagaId(instance.getSagaId());
        if (existing.isPresent()) {
            ManualInterventionEntry entry = existing.get();
            entry.setCompensationAttempts(record.getCompensationAttempts());
            entry.setUpdatedAt(java.time.Instant.now());
            log.info("[manual] 流程 {} 已在人工介入清单中（entryId={}），仅更新快照",
                    instance.getSagaId(), entry.getEntryId());
            return;
        }
        ManualInterventionEntry entry = new ManualInterventionEntry(
                UUID.randomUUID().toString(), instance.getSagaId(), instance.getBusinessKey(),
                instance.getDefinitionName(), stepDef.name(), ErrorCategory.RETRY_EXHAUSTED,
                "步骤 " + stepDef.name() + " 补偿重试 " + MAX_COMPENSATION_ATTEMPTS + " 次仍失败",
                record.getCompensationAttempts(), java.time.Instant.now());
        manualInterventionStore.upsert(entry);
        log.info("[manual] 登记人工介入 entryId={} sagaId={} 卡住步骤={} 已补偿尝试={}",
                entry.getEntryId(), instance.getSagaId(), stepDef.name(),
                record.getCompensationAttempts());
    }

    /**
     * 在预算内重试补偿单个步骤；budget 为本次调用允许的最大尝试次数，
     * 尝试次数在步骤记录上累计（跨重投可见）。
     */
    private boolean compensateStepWithRetry(SagaInstance instance, StepDefinition stepDef,
                                            StepRecord record, int budget) {
        for (int attempt = 0; attempt < budget; attempt++) {
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

    private void validate(SagaEvent event) {
        if (event == null || event.eventId() == null || event.eventId().isBlank()
                || event.businessKey() == null || event.businessKey().isBlank()
                || event.stepName() == null || event.stepName().isBlank()
                || event.type() == null) {
            throw new SagaException(ErrorCategory.VALIDATION, "事件缺少必填字段");
        }
    }
}
