package com.github.highcumontoa.eventsagaorchestratorjava.tests;

import com.github.highcumontoa.eventsagaorchestratorjava.deadletter.DeadLetterEntry;
import com.github.highcumontoa.eventsagaorchestratorjava.deadletter.DeadLetterQueue;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.EventType;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaInstance;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaState;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.StepDefinition;
import com.github.highcumontoa.eventsagaorchestratorjava.engine.KeyLockManager;
import com.github.highcumontoa.eventsagaorchestratorjava.engine.RetryPolicy;
import com.github.highcumontoa.eventsagaorchestratorjava.engine.SagaFlow;
import com.github.highcumontoa.eventsagaorchestratorjava.engine.SagaFlowRegistry;
import com.github.highcumontoa.eventsagaorchestratorjava.engine.SagaOrchestrator;
import com.github.highcumontoa.eventsagaorchestratorjava.event.EventAcceptStatus;
import com.github.highcumontoa.eventsagaorchestratorjava.event.EventProcessingResult;
import com.github.highcumontoa.eventsagaorchestratorjava.event.SagaEvent;
import com.github.highcumontoa.eventsagaorchestratorjava.flow.ControllableStepAction;
import com.github.highcumontoa.eventsagaorchestratorjava.store.InMemorySagaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 补偿语义测试：
 * - 前向失败后已完成步骤按相反顺序回滚；
 * - 补偿瞬时失败在有界重试内恢复；
 * - 补偿持续失败重试耗尽 => COMPENSATION_FAILED 终态 + 死信（不静默丢弃）；
 * - 人工介入修复后再次触发补偿可恢复到 COMPENSATED 终态。
 */
class SagaCompensationTest {

    private InMemorySagaStore store;
    private DeadLetterQueue dlq;
    private SagaOrchestrator orchestrator;
    private ControllableStepAction reserve;
    private ControllableStepAction debit;
    private ControllableStepAction createOrder;
    private ControllableStepAction notify;

    @BeforeEach
    void setUp() {
        store = new InMemorySagaStore();
        dlq = new DeadLetterQueue();
        SagaFlowRegistry registry = new SagaFlowRegistry();
        reserve = new ControllableStepAction("reserveInventory");
        debit = new ControllableStepAction("debitPayment");
        createOrder = new ControllableStepAction("createOrder");
        notify = new ControllableStepAction("notifyShipping");
        registry.register(new SagaFlow() {
            @Override
            public String flowType() {
                return "ORDER";
            }

            @Override
            public List<StepDefinition> steps() {
                return List.of(
                        new StepDefinition(0, "reserveInventory", reserve),
                        new StepDefinition(1, "debitPayment", debit),
                        new StepDefinition(2, "createOrder", createOrder),
                        new StepDefinition(3, "notifyShipping", notify));
            }
        });
        orchestrator = new SagaOrchestrator(store, store, registry, new KeyLockManager(), dlq,
                new RetryPolicy(3, Duration.ZERO), new RetryPolicy(3, Duration.ZERO));
    }

    private SagaEvent advance(String key, int step) {
        return new SagaEvent(UUID.randomUUID().toString(), key, EventType.STEP_ADVANCE,
                step, null, Map.of(), Instant.now());
    }

    private void advanceTo(String key, int inclusiveStep) {
        for (int i = 0; i <= inclusiveStep; i++) {
            orchestrator.process(advance(key, i));
        }
    }

    @Test
    @DisplayName("前向失败：第3步失败时按 debit -> reserve 反向补偿，最终 COMPENSATED 且补偿次数符合预期")
    void forwardFailureTriggersReverseCompensation() {
        String key = "order-comp-1";
        orchestrator.createSaga(key, "ORDER");
        advanceTo(key, 1); // reserve, debit 成功
        // createOrder(step2) 永久失败（3 次正向重试后仍失败）
        createOrder.failForward(100);

        EventProcessingResult result = orchestrator.process(advance(key, 2));
        assertEquals(EventAcceptStatus.COMPENSATED, result.status());
        assertTrue(result.reasonCode().startsWith("COMPENSATED_AFTER_FAILURE"));

        SagaInstance saga = orchestrator.getSaga(key);
        assertEquals(SagaState.COMPENSATED, saga.getState());

        // 只有完成过副作用的步骤被补偿，且顺序相反：debit 先，reserve 后
        assertEquals(1, debit.getCompensateSuccessCount());
        assertEquals(1, reserve.getCompensateSuccessCount());
        assertEquals(0, createOrder.getCompensateSuccessCount(), "失败步骤本身未完成副作用，不应补偿");
        assertEquals(0, notify.getCompensateSuccessCount(), "未执行到的步骤不应补偿");

        // createOrder 的正向尝试次数恰好是重试上限
        assertEquals(3, saga.getSteps().get(2).getForwardAttempts());
        // 各被补偿步骤补偿一次成功
        assertEquals(1, saga.getSteps().get(0).getCompensationAttempts());
        assertEquals(1, saga.getSteps().get(1).getCompensationAttempts());
        // 终态稳定：再来推进事件应被拒绝
        EventProcessingResult late = orchestrator.process(advance(key, 3));
        assertEquals(EventAcceptStatus.STALE, late.status());
        // 无死信
        assertEquals(0, dlq.findByBusinessKey(key).size());
    }

    @Test
    @DisplayName("补偿瞬时失败：补偿前两次失败第三次成功，最终 COMPENSATED，补偿尝试次数为3")
    void compensationRetryThenSuccess() {
        String key = "order-comp-2";
        orchestrator.createSaga(key, "ORDER");
        advanceTo(key, 0);
        // debit 前向永久失败，reserve 补偿前 2 次失败、第 3 次成功
        debit.failForward(100);
        reserve.failCompensation(2);

        EventProcessingResult result = orchestrator.process(advance(key, 1));
        assertEquals(EventAcceptStatus.COMPENSATED, result.status());

        SagaInstance saga = orchestrator.getSaga(key);
        assertEquals(SagaState.COMPENSATED, saga.getState());
        assertEquals(3, saga.getSteps().get(0).getCompensationAttempts(),
                "reserve 补偿应在第3次尝试成功");
        assertEquals(1, reserve.getCompensateSuccessCount());
        assertEquals(0, dlq.findByBusinessKey(key).size());
    }

    @Test
    @DisplayName("补偿持续失败：重试耗尽后进入 COMPENSATION_FAILED 终态并落死信，不静默丢弃")
    void compensationExhaustionEntersManualTerminalState() {
        String key = "order-comp-3";
        orchestrator.createSaga(key, "ORDER");
        advanceTo(key, 1);
        createOrder.failForward(100);
        // debit（最先被补偿的步骤）补偿永久失败
        debit.failCompensation(100);

        EventProcessingResult result = orchestrator.process(advance(key, 2));
        assertEquals(EventAcceptStatus.DEAD_LETTER, result.status());

        SagaInstance saga = orchestrator.getSaga(key);
        assertEquals(SagaState.COMPENSATION_FAILED, saga.getState(),
                "必须进入明确的人工介入终态，不能停留在中间状态");
        assertEquals(3, saga.getSteps().get(1).getCompensationAttempts(),
                "补偿必须重试到上限");
        assertEquals(0, debit.getCompensateSuccessCount());
        assertEquals(0, reserve.getCompensateSuccessCount(),
                "补偿在 debit 处中断，不得继续回滚更早的步骤");

        List<DeadLetterEntry> entries = dlq.findByBusinessKey(key);
        assertEquals(1, entries.size(), "补偿失败必须落死信");
        assertEquals("COMPENSATION_RETRY_EXHAUSTED", entries.get(0).reasonCode());

        // 终态下重复/新的推进事件必须被拒绝且不产生副作用
        EventProcessingResult late = orchestrator.process(advance(key, 2));
        assertEquals(EventAcceptStatus.STALE, late.status());
        assertEquals(3, saga.getSteps().get(1).getCompensationAttempts(),
                "终态拒绝事件不得再次触发补偿重试");
    }

    @Test
    @DisplayName("人工介入恢复：修复补偿动作后再次触发补偿，从 COMPENSATION_FAILED 恢复到 COMPENSATED")
    void manualInterventionRecoversCompensation() {
        String key = "order-comp-4";
        orchestrator.createSaga(key, "ORDER");
        advanceTo(key, 1);
        createOrder.failForward(100);
        // 第一轮补偿：debit 失败 3 次耗尽
        debit.failCompensation(100);
        assertEquals(EventAcceptStatus.DEAD_LETTER,
                orchestrator.process(advance(key, 2)).status());
        assertEquals(SagaState.COMPENSATION_FAILED, orchestrator.getSaga(key).getState());

        // 人工修复下游：后续补偿调用恢复正常（前 3 次已计入失败，从第 4 次开始成功）
        debit.failCompensation(3);

        EventProcessingResult retry = orchestrator.triggerCompensation(key, "MANUAL_FIX_APPLIED");
        assertEquals(EventAcceptStatus.COMPENSATED, retry.status());
        SagaInstance saga = orchestrator.getSaga(key);
        assertEquals(SagaState.COMPENSATED, saga.getState());

        // 补偿跨多次人工介入累计计数：debit 总计 3（失败轮）+ 1（恢复轮）
        assertEquals(4, saga.getSteps().get(1).getCompensationAttempts());
        // 恢复后继续反向补偿 reserve
        assertEquals(1, reserve.getCompensateSuccessCount());
        assertEquals(1, debit.getCompensateSuccessCount());
        // 历史死信仍保留，可审计
        assertEquals(1, dlq.findByBusinessKey(key).size());

        // 幂等：已 COMPENSATED 再触发补偿返回 DUPLICATE，不产生额外补偿副作用
        EventProcessingResult again = orchestrator.triggerCompensation(key, "DUP");
        assertEquals(EventAcceptStatus.DUPLICATE, again.status());
        assertEquals(1, reserve.getCompensateSuccessCount());
    }

    @Test
    @DisplayName("异常归一化：步骤抛 RuntimeException 时不泄露内部细节；无已完成步骤时直接进入 COMPENSATED")
    void runtimeExceptionIsNormalized() {
        String key = "order-comp-5";
        orchestrator.createSaga(key, "ORDER");
        // reserve 前向永久抛异常；此时尚无任何已完成步骤
        reserve.failForward(100).asException();

        EventProcessingResult result = orchestrator.process(advance(key, 0));
        // 补偿集合为空 => 无需回滚，直接进入 COMPENSATED 终态
        assertEquals(EventAcceptStatus.COMPENSATED, result.status());
        assertTrue(result.reasonCode().startsWith("COMPENSATED_AFTER_FAILURE"));
        SagaInstance saga = orchestrator.getSaga(key);
        assertEquals(SagaState.COMPENSATED, saga.getState(),
                "即使首步失败也必须进入明确终态，不得停留在 RUNNING/COMPENSATING");
        assertEquals(3, saga.getSteps().get(0).getForwardAttempts(),
                "正向必须重试到上限");
        assertEquals(0, dlq.findByBusinessKey(key).size(),
                "补偿未失败，不应产生死信");
    }
}
