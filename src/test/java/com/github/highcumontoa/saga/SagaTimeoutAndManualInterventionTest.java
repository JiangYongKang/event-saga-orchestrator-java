package com.github.highcumontoa.saga;

import com.github.highcumontoa.saga.core.DefaultSagaOrchestrator;
import com.github.highcumontoa.saga.core.InMemorySagaDefinitionRegistry;
import com.github.highcumontoa.saga.domain.SagaDefinition;
import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.domain.SagaStatus;
import com.github.highcumontoa.saga.domain.StepDefinition;
import com.github.highcumontoa.saga.domain.StepFailureKind;
import com.github.highcumontoa.saga.error.ErrorCategory;
import com.github.highcumontoa.saga.error.SagaException;
import com.github.highcumontoa.saga.event.EventType;
import com.github.highcumontoa.saga.event.SagaEvent;
import com.github.highcumontoa.saga.manual.ManualInterventionEntry;
import com.github.highcumontoa.saga.manual.ManualInterventionStatus;
import com.github.highcumontoa.saga.store.InMemorySagaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 覆盖：步骤超时、人工介入清单查询/重投/清理、重投幂等与终态稳定。 */
class SagaTimeoutAndManualInterventionTest {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutAndManualInterventionTest.class);

    private InMemorySagaStore store;
    private InMemorySagaDefinitionRegistry registry;
    private DefaultSagaOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        store = new InMemorySagaStore();
        registry = new InMemorySagaDefinitionRegistry();
        orchestrator = new DefaultSagaOrchestrator(store, registry);
    }

    private SagaEvent event(String businessKey, String stepName, EventType type) {
        return new SagaEvent(UUID.randomUUID().toString(), businessKey, stepName,
                type, null, Instant.now());
    }

    @Test
    void stepTimeoutTriggersCompensationAndIsDistinguishable() {
        log.info("[test-scenario] 步骤超时进入补偿，且超时与业务失败对外可区分");
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger());
        registry.register(new SagaDefinition("timeout-saga", List.of(
                new StepDefinition("step-1",
                        ctx -> { },
                        ctx -> comps.get(0).incrementAndGet()),
                new StepDefinition("step-2",
                        ctx -> Thread.sleep(5_000), // 超过 200ms 预算
                        ctx -> comps.get(1).incrementAndGet(),
                        Duration.ofMillis(200)))));

        String sagaId = orchestrator.createSaga("timeout-saga", "to-1");
        orchestrator.handleEvent(event("to-1", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("to-1", "step-2", EventType.STEP_SUCCEEDED));

        SagaInstance instance = orchestrator.getSaga(sagaId);
        assertEquals(SagaStatus.COMPENSATED, instance.getStatus(), "超时按失败处理并走完补偿");
        assertEquals(ErrorCategory.TIMEOUT, instance.getFailureCategory(),
                "对外失败类别应为 TIMEOUT，与业务失败区分");
        assertEquals("step-2", instance.getFailureStep());
        assertEquals(StepFailureKind.TIMEOUT, instance.getSteps().get(1).getLastFailureKind());
        assertEquals(1, comps.get(0).get(), "已完成的 step-1 应被反向补偿一次");
        assertEquals(0, comps.get(1).get(), "step-2 未执行成功，不应补偿");
    }

    @Test
    void businessFailureIsDistinguishedFromTimeout() {
        log.info("[test-scenario] 业务失败不标记为超时");
        registry.register(new SagaDefinition("biz-fail-saga", List.of(
                new StepDefinition("step-1",
                        ctx -> { throw new IllegalStateException("业务校验不通过"); },
                        ctx -> { },
                        Duration.ofSeconds(30)))));

        String sagaId = orchestrator.createSaga("biz-fail-saga", "bf-1");
        orchestrator.handleEvent(event("bf-1", "step-1", EventType.STEP_SUCCEEDED));

        SagaInstance instance = orchestrator.getSaga(sagaId);
        assertEquals(SagaStatus.COMPENSATED, instance.getStatus());
        assertEquals(StepFailureKind.BUSINESS_FAILURE,
                instance.getSteps().get(0).getLastFailureKind(), "业务失败种类可区分");
        assertNull(instance.getFailureCategory(), "业务失败不应标记为 TIMEOUT");
    }

    @Test
    void manualInterventionListIsQueryableByBusinessKeyAndStatus() {
        log.info("[test-scenario] 补偿重试耗尽进入人工介入，清单可按业务键与状态查询");
        AtomicBoolean alwaysFail = new AtomicBoolean(true);
        registerStuckSaga("stuck-saga", alwaysFail, new AtomicInteger(), new AtomicInteger());

        String sagaId = orchestrator.createSaga("stuck-saga", "mi-1");
        orchestrator.handleEvent(event("mi-1", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("mi-1", "step-2", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("mi-1", "step-3", EventType.STEP_FAILED));
        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus());

        List<ManualInterventionEntry> all = orchestrator.listManualInterventions(null, null);
        assertEquals(1, all.size(), "同一流程只应登记一条人工介入记录");
        ManualInterventionEntry entry = all.get(0);
        assertEquals("mi-1", entry.getBusinessKey());
        assertEquals(ManualInterventionStatus.PENDING, entry.getStatus());
        assertEquals("step-1", entry.getStuckStepName(), "应能看到卡在哪一步");
        assertEquals(ErrorCategory.RETRY_EXHAUSTED, entry.getFailureCategory());
        assertEquals(3, entry.getCompensationAttempts(), "应能看到已补偿尝试次数");

        assertEquals(1, orchestrator
                .listManualInterventions("mi-1", ManualInterventionStatus.PENDING).size());
        assertTrue(orchestrator
                .listManualInterventions("mi-1", ManualInterventionStatus.RESOLVED).isEmpty());
        assertTrue(orchestrator.listManualInterventions("other-key", null).isEmpty());
    }

    @Test
    void reprocessDoesNotRecompensateAlreadyCompensatedSteps() {
        log.info("[test-scenario] 重投不重复补偿已回滚步骤，保持相反顺序，成功后条目可清理");
        AtomicBoolean failStep1Compensation = new AtomicBoolean(true);
        AtomicInteger comp1 = new AtomicInteger();
        AtomicInteger comp2 = new AtomicInteger();
        registerStuckSaga("reprocess-saga", failStep1Compensation, comp1, comp2);

        String sagaId = orchestrator.createSaga("reprocess-saga", "rp-1");
        orchestrator.handleEvent(event("rp-1", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("rp-1", "step-2", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("rp-1", "step-3", EventType.STEP_FAILED));
        // 首次补偿：step-2 已成功回滚，step-1 重试耗尽
        assertEquals(1, comp2.get(), "step-2 应已补偿成功一次");
        assertEquals(3, comp1.get(), "step-1 补偿重试耗尽");
        ManualInterventionEntry entry =
                orchestrator.listManualInterventions("rp-1", null).get(0);

        // 人工修复后重投
        failStep1Compensation.set(false);
        ManualInterventionEntry updated = orchestrator.reprocessManualIntervention(entry.getEntryId());

        assertEquals(ManualInterventionStatus.RESOLVED, updated.getStatus());
        assertEquals(1, updated.getReprocessCount());
        assertEquals(SagaStatus.COMPENSATED, orchestrator.getSaga(sagaId).getStatus());
        assertEquals(1, comp2.get(), "已成功回滚的 step-2 不得被重复补偿");
        assertEquals(4, comp1.get(), "step-1 仅补做卡住的那一步补偿");

        // 已处理条目再次重投应被拒绝，不产生叠加效果
        SagaException conflict = assertThrows(SagaException.class,
                () -> orchestrator.reprocessManualIntervention(entry.getEntryId()));
        assertEquals(ErrorCategory.CONFLICT, conflict.getCategory());
        assertEquals(1, orchestrator.listManualInterventions(null, null).size(),
                "重投不得叠加清单记录");

        // 清理：已处理条目可删除，清单变空
        orchestrator.removeResolvedManualIntervention(entry.getEntryId());
        assertTrue(orchestrator.listManualInterventions(null, null).isEmpty());
    }

    @Test
    void failedReprocessKeepsTerminalStateStable() {
        log.info("[test-scenario] 重投后仍失败保持终态稳定，不叠加记录、不回退中间态");
        AtomicBoolean alwaysFail = new AtomicBoolean(true);
        AtomicInteger comp1 = new AtomicInteger();
        registerStuckSaga("stable-saga", alwaysFail, comp1, new AtomicInteger());

        String sagaId = orchestrator.createSaga("stable-saga", "st-1");
        orchestrator.handleEvent(event("st-1", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("st-1", "step-2", EventType.STEP_FAILED));
        ManualInterventionEntry entry =
                orchestrator.listManualInterventions("st-1", null).get(0);

        // 连续两次重投都仍失败
        orchestrator.reprocessManualIntervention(entry.getEntryId());
        ManualInterventionEntry afterSecond =
                orchestrator.reprocessManualIntervention(entry.getEntryId());

        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus(),
                "重投失败必须保持 MANUAL_INTERVENTION 终态，不得回退中间态");
        assertEquals(ManualInterventionStatus.PENDING, afterSecond.getStatus());
        assertEquals(2, afterSecond.getReprocessCount());
        assertEquals(9, comp1.get(), "每次重投追加一轮重试预算，累计 3+3+3 次");
        assertEquals(1, orchestrator.listManualInterventions(null, null).size(),
                "多次重投不得叠加清单记录");

        // 未完成条目不允许清理
        SagaException conflict = assertThrows(SagaException.class,
                () -> orchestrator.removeResolvedManualIntervention(entry.getEntryId()));
        assertEquals(ErrorCategory.CONFLICT, conflict.getCategory());
        assertEquals(1, orchestrator.listManualInterventions(null, null).size());
    }

    /**
     * 注册三步流程：step-3 失败事件触发补偿；step-2 补偿总是成功；
     * step-1 的补偿由 failStep1Compensation 开关控制（模拟人工修复前/后）。
     */
    private void registerStuckSaga(String name, AtomicBoolean failStep1Compensation,
                                   AtomicInteger comp1, AtomicInteger comp2) {
        registry.register(new SagaDefinition(name, List.of(
                new StepDefinition("step-1",
                        ctx -> { },
                        ctx -> {
                            comp1.incrementAndGet();
                            if (failStep1Compensation.get()) {
                                throw new IllegalStateException("注入的补偿失败");
                            }
                        }),
                new StepDefinition("step-2",
                        ctx -> { },
                        ctx -> comp2.incrementAndGet()),
                new StepDefinition("step-3",
                        ctx -> { },
                        ctx -> { }))));
    }
}
