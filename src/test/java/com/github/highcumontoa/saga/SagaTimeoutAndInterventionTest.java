package com.github.highcumontoa.saga;

import com.github.highcumontoa.saga.core.DefaultManualInterventionService;
import com.github.highcumontoa.saga.core.DefaultSagaOrchestrator;
import com.github.highcumontoa.saga.core.InMemorySagaDefinitionRegistry;
import com.github.highcumontoa.saga.domain.ManualInterventionEntry;
import com.github.highcumontoa.saga.domain.ManualInterventionStatus;
import com.github.highcumontoa.saga.domain.SagaDefinition;
import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.domain.SagaStatus;
import com.github.highcumontoa.saga.domain.StepDefinition;
import com.github.highcumontoa.saga.error.ErrorCategory;
import com.github.highcumontoa.saga.error.SagaException;
import com.github.highcumontoa.saga.event.EventAck;
import com.github.highcumontoa.saga.event.EventType;
import com.github.highcumontoa.saga.event.SagaEvent;
import com.github.highcumontoa.saga.store.InMemoryManualInterventionStore;
import com.github.highcumontoa.saga.store.InMemorySagaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 覆盖本轮新增行为：步骤超时、人工介入清单查询/重投/清理。 */
class SagaTimeoutAndInterventionTest {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutAndInterventionTest.class);

    private InMemorySagaStore store;
    private InMemorySagaDefinitionRegistry registry;
    private InMemoryManualInterventionStore interventionStore;
    private DefaultSagaOrchestrator orchestrator;
    private DefaultManualInterventionService interventionService;

    @BeforeEach
    void setUp() {
        store = new InMemorySagaStore();
        registry = new InMemorySagaDefinitionRegistry();
        interventionStore = new InMemoryManualInterventionStore();
        orchestrator = new DefaultSagaOrchestrator(store, registry, interventionStore);
        interventionService = new DefaultManualInterventionService(interventionStore, orchestrator);
    }

    private SagaEvent event(String businessKey, String stepName, EventType type) {
        return new SagaEvent(UUID.randomUUID().toString(), businessKey, stepName,
                type, null, Instant.now());
    }

    @Test
    void stepTimeoutTriggersCompensationAndIsDistinguishableFromBusinessFailure() {
        log.info("[test] 场景：步骤超时 -> 按失败走反向补偿，类别 TIMEOUT，与业务失败 BUSINESS_FAILURE 可区分");
        AtomicInteger comp1 = new AtomicInteger();
        registry.register(new SagaDefinition("timeout-saga", List.of(
                new StepDefinition("step-1", ctx -> { }, ctx -> comp1.incrementAndGet()),
                new StepDefinition("step-2",
                        ctx -> Thread.sleep(5_000),
                        ctx -> { },
                        Duration.ofMillis(100))
        )));
        String sagaId = orchestrator.createSaga("timeout-saga", "to-1");
        orchestrator.handleEvent(event("to-1", "step-1", EventType.STEP_SUCCEEDED));

        EventAck ack = orchestrator.handleEvent(event("to-1", "step-2", EventType.STEP_SUCCEEDED));
        assertTrue(ack.applied());
        SagaInstance instance = orchestrator.getSaga(sagaId);
        assertEquals(SagaStatus.COMPENSATED, instance.getStatus(), "超时应按失败处理并完成反向补偿");
        assertEquals(ErrorCategory.TIMEOUT, instance.getSteps().get(1).getFailureCategory(),
                "超时步骤的失败类别必须是 TIMEOUT");
        assertEquals(1, comp1.get(), "已完成步骤应被补偿一次");

        // 对照组：业务失败（非超时）
        registry.register(new SagaDefinition("bizfail-saga", List.of(
                new StepDefinition("step-1", ctx -> { }, ctx -> { }),
                new StepDefinition("step-2",
                        ctx -> {
                            throw new IllegalStateException("业务校验不通过");
                        },
                        ctx -> { },
                        Duration.ofSeconds(30))
        )));
        String sagaId2 = orchestrator.createSaga("bizfail-saga", "bf-1");
        orchestrator.handleEvent(event("bf-1", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("bf-1", "step-2", EventType.STEP_SUCCEEDED));
        assertEquals(ErrorCategory.BUSINESS_FAILURE,
                orchestrator.getSaga(sagaId2).getSteps().get(1).getFailureCategory(),
                "业务失败的类别必须是 BUSINESS_FAILURE，与 TIMEOUT 区分");
    }

    @Test
    void timeoutFollowedByCompensationExhaustionCreatesInterventionEntry() {
        log.info("[test] 场景：步骤超时 + 补偿重试耗尽 -> 进入人工介入，TIMEOUT 与 RETRY_EXHAUSTED 类别可区分");
        registry.register(new SagaDefinition("timeout-stuck-saga", List.of(
                new StepDefinition("step-1", ctx -> { }, ctx -> {
                    throw new IllegalStateException("补偿永久失败");
                }),
                new StepDefinition("step-2",
                        ctx -> Thread.sleep(5_000),
                        ctx -> { },
                        Duration.ofMillis(100))
        )));
        String sagaId = orchestrator.createSaga("timeout-stuck-saga", "to-2");
        orchestrator.handleEvent(event("to-2", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("to-2", "step-2", EventType.STEP_SUCCEEDED));

        SagaInstance instance = orchestrator.getSaga(sagaId);
        assertEquals(SagaStatus.MANUAL_INTERVENTION, instance.getStatus());
        assertEquals(ErrorCategory.TIMEOUT, instance.getSteps().get(1).getFailureCategory(),
                "触发补偿的根因是超时，步骤上应记录 TIMEOUT");

        List<ManualInterventionEntry> entries = interventionService.list(null, null);
        assertEquals(1, entries.size(), "重试耗尽应登记一条人工介入记录");
        assertEquals(ErrorCategory.RETRY_EXHAUSTED, entries.get(0).getFailureCategory(),
                "进入人工介入的直接原因是补偿重试耗尽");
    }

    @Test
    void manualInterventionEntriesAreQueryableByBusinessKeyAndStatus() {
        log.info("[test] 场景：人工介入清单支持按业务键与状态查询，含卡住步骤/失败原因/补偿次数");
        registry.register(new SagaDefinition("stuck-saga", List.of(
                new StepDefinition("step-1", ctx -> { }, ctx -> {
                    throw new IllegalStateException("补偿永久失败");
                }),
                new StepDefinition("step-2", ctx -> { }, ctx -> { })
        )));
        String sagaId = orchestrator.createSaga("stuck-saga", "mi-1");
        orchestrator.handleEvent(event("mi-1", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("mi-1", "step-2", EventType.STEP_FAILED));
        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus());

        List<ManualInterventionEntry> byKey = interventionService.list("mi-1", ManualInterventionStatus.PENDING);
        assertEquals(1, byKey.size(), "按业务键 + PENDING 应查到卡住条目");
        ManualInterventionEntry entry = byKey.get(0);
        assertEquals(sagaId, entry.getSagaId());
        assertEquals("step-1", entry.getStuckStepName(), "应能看到卡在哪一步");
        assertEquals(3, entry.getCompensationAttempts(), "应能看到已补偿次数");
        assertEquals(ErrorCategory.RETRY_EXHAUSTED, entry.getFailureCategory());
        assertTrue(entry.getFailureReason().contains("step-1"), "失败原因应指向卡住步骤");
        assertEquals(0, entry.getReprocessAttempts());

        assertEquals(0, interventionService.list("other-key", null).size(), "其他业务键查不到");
        assertEquals(0, interventionService.list(null, ManualInterventionStatus.RESOLVED).size(),
                "尚无已处理条目");
    }

    @Test
    void reprocessSkipsCompensatedStepsAndKeepsReverseOrder() {
        log.info("[test] 场景：重投不重复补偿已回滚步骤，且保持相反顺序");
        List<String> compOrder = Collections.synchronizedList(new ArrayList<>());
        List<AtomicInteger> compCounters = List.of(new AtomicInteger(), new AtomicInteger(),
                new AtomicInteger(), new AtomicInteger());
        AtomicBoolean step2CompFails = new AtomicBoolean(true);

        List<StepDefinition> defs = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            String name = "step-" + i;
            int idx = i - 1;
            defs.add(new StepDefinition(name,
                    ctx -> { },
                    ctx -> {
                        compCounters.get(idx).incrementAndGet();
                        compOrder.add(name);
                        if (idx == 1 && step2CompFails.get()) {
                            throw new IllegalStateException("step-2 补偿暂时失败");
                        }
                    }));
        }
        registry.register(new SagaDefinition("reprocess-saga", defs));

        String sagaId = orchestrator.createSaga("reprocess-saga", "rp-1");
        for (int i = 1; i <= 3; i++) {
            orchestrator.handleEvent(event("rp-1", "step-" + i, EventType.STEP_SUCCEEDED));
        }
        orchestrator.handleEvent(event("rp-1", "step-4", EventType.STEP_FAILED));
        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus());
        assertEquals(List.of("step-3", "step-2", "step-2", "step-2"), compOrder,
                "第一轮：step-3 补偿成功，step-2 补偿重试 3 次失败");
        assertEquals(1, compCounters.get(2).get(), "step-3 已成功回滚");

        // 人工确认并重投（此时 step-2 补偿已恢复可用）
        step2CompFails.set(false);
        ManualInterventionEntry entry = interventionService.reprocess(sagaId);

        assertEquals(SagaStatus.COMPENSATED, orchestrator.getSaga(sagaId).getStatus());
        assertEquals(List.of("step-3", "step-2", "step-2", "step-2", "step-2", "step-1"), compOrder,
                "重投只补偿未回滚的步骤，且顺序仍与执行顺序相反");
        assertEquals(1, compCounters.get(2).get(), "已成功回滚的 step-3 不得被重复补偿");
        assertEquals(4, compCounters.get(1).get(), "step-2 第一轮 3 次 + 重投 1 次");
        assertEquals(1, compCounters.get(0).get(), "step-1 在重投中补偿一次");
        assertEquals(ManualInterventionStatus.RESOLVED, entry.getStatus());
        assertEquals(1, entry.getReprocessAttempts());
    }

    @Test
    void repeatedReprocessFailureKeepsTerminalStateAndSingleEntry() {
        log.info("[test] 场景：多次重投仍失败 -> 保持终态稳定，清单不叠加记录");
        registry.register(new SagaDefinition("perma-stuck-saga", List.of(
                new StepDefinition("step-1", ctx -> { }, ctx -> {
                    throw new IllegalStateException("补偿永久失败");
                }),
                new StepDefinition("step-2", ctx -> { }, ctx -> { })
        )));
        String sagaId = orchestrator.createSaga("perma-stuck-saga", "rp-2");
        orchestrator.handleEvent(event("rp-2", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("rp-2", "step-2", EventType.STEP_FAILED));
        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus());

        interventionService.reprocess(sagaId);
        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus(),
                "重投失败后必须回到人工介入终态，不得停留在中间态");
        interventionService.reprocess(sagaId);

        List<ManualInterventionEntry> entries = interventionService.list(null, null);
        assertEquals(1, entries.size(), "多次重投不得叠加清单记录");
        ManualInterventionEntry entry = entries.get(0);
        assertEquals(2, entry.getReprocessAttempts(), "重投次数应累计");
        assertEquals(ManualInterventionStatus.PENDING, entry.getStatus(), "仍未处理好应保持待处理");
        assertEquals(3, entry.getCompensationAttempts(), "每轮重投的补偿尝试次数应被记录");
        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus());
    }

    @Test
    void resolvedEntriesCanBePurgedAndReprocessRulesAreEnforced() {
        log.info("[test] 场景：重投成功后条目可清理；非法重投被拒绝且类别可区分");
        AtomicInteger compCalls = new AtomicInteger();
        registry.register(new SagaDefinition("recoverable-saga", List.of(
                new StepDefinition("step-1", ctx -> { }, ctx -> {
                    if (compCalls.incrementAndGet() <= 3) {
                        throw new IllegalStateException("第一轮补偿失败");
                    }
                }),
                new StepDefinition("step-2", ctx -> { }, ctx -> { })
        )));
        String sagaId = orchestrator.createSaga("recoverable-saga", "rp-3");
        orchestrator.handleEvent(event("rp-3", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("rp-3", "step-2", EventType.STEP_FAILED));
        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus());

        ManualInterventionEntry entry = interventionService.reprocess(sagaId);
        assertEquals(SagaStatus.COMPENSATED, orchestrator.getSaga(sagaId).getStatus());
        assertEquals(ManualInterventionStatus.RESOLVED, entry.getStatus());

        // 已处理条目不可重复重投
        SagaException conflict = assertThrows(SagaException.class,
                () -> interventionService.reprocess(sagaId));
        assertEquals(ErrorCategory.CONFLICT, conflict.getCategory());

        // 清理已处理条目
        assertEquals(1, interventionService.purgeResolved());
        assertEquals(0, interventionService.list(null, null).size(), "清理后清单应为空");
        assertEquals(0, interventionService.purgeResolved(), "重复清理不应出错");

        // 不存在的条目 / 非人工介入状态的流程
        SagaException notFound = assertThrows(SagaException.class,
                () -> interventionService.reprocess(sagaId));
        assertEquals(ErrorCategory.NOT_FOUND, notFound.getCategory());

        String runningId = orchestrator.createSaga("recoverable-saga", "rp-4");
        SagaException conflict2 = assertThrows(SagaException.class,
                () -> orchestrator.reprocess(runningId));
        assertEquals(ErrorCategory.CONFLICT, conflict2.getCategory(),
                "未处于人工介入状态的流程不能重投");
    }
}
