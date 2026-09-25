package com.github.highcumontoa.eventsagaorchestratorjava.tests;

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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 编排器核心单测：重复投递、乱序到达、并发推进。
 * 不启动 Spring，手工装配以使用零退避快速重试策略。
 */
class SagaOrchestratorTest {

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

        SagaFlow flow = new SagaFlow() {
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
        };
        registry.register(flow);

        orchestrator = new SagaOrchestrator(store, store, registry, new KeyLockManager(), dlq,
                new RetryPolicy(3, Duration.ZERO), new RetryPolicy(3, Duration.ZERO));
    }

    private SagaEvent advance(String key, int step) {
        return new SagaEvent(UUID.randomUUID().toString(), key, EventType.STEP_ADVANCE,
                step, null, Map.of(), Instant.now());
    }

    @Test
    @DisplayName("正常路径：4 步全部推进后进入 COMPLETED")
    void happyPathCompletes() {
        String key = "order-happy";
        assertEquals(EventAcceptStatus.ACCEPTED, orchestrator.createSaga(key, "ORDER").status());
        for (int i = 0; i < 4; i++) {
            assertEquals(EventAcceptStatus.ACCEPTED, orchestrator.process(advance(key, i)).status());
        }
        SagaInstance saga = orchestrator.getSaga(key);
        assertEquals(SagaState.COMPLETED, saga.getState());
        assertEquals(3, saga.getCurrentStepIndex());
        // 每个正向副作用只发生一次
        assertEquals(1, reserve.getDoSuccessCount());
        assertEquals(1, debit.getDoSuccessCount());
        assertEquals(1, createOrder.getDoSuccessCount());
        assertEquals(1, notify.getDoSuccessCount());
        // 事件流水可回放
        assertEquals(5, orchestrator.getEventStream(key).size());
    }

    @Test
    @DisplayName("重复投递：相同 eventId 第二次被识别为 DUPLICATE 且不重复产生副作用")
    void duplicateEventHasNoSideEffect() {
        String key = "order-dup";
        orchestrator.createSaga(key, "ORDER");
        SagaEvent step0 = advance(key, 0);
        EventProcessingResult first = orchestrator.process(step0);
        assertEquals(EventAcceptStatus.ACCEPTED, first.status());

        EventProcessingResult second = orchestrator.process(step0);
        assertEquals(EventAcceptStatus.DUPLICATE, second.status());
        assertTrue(second.reasonCode().contains("DUPLICATE_EVENT"),
                "重复原因必须可区分: " + second.reasonCode());
        assertEquals(first.version(), second.version(), "重复事件不得推进版本");

        // 关键断言：正向副作用只有一次
        assertEquals(1, reserve.getDoSuccessCount());
        assertEquals(1, reserve.getDoCalls());

        // 流程仍可继续推进到完成
        for (int i = 1; i < 4; i++) {
            assertEquals(EventAcceptStatus.ACCEPTED, orchestrator.process(advance(key, i)).status());
        }
        assertEquals(SagaState.COMPLETED, orchestrator.getSaga(key).getState());
    }

    @Test
    @DisplayName("乱序到达：跳步事件被识别为 STALE/OUT_OF_ORDER，随后正确步骤仍可推进")
    void outOfOrderEventRejected() {
        String key = "order-ooo";
        orchestrator.createSaga(key, "ORDER");

        // 先投递 step 2（领先），应拒绝
        EventProcessingResult ahead = orchestrator.process(advance(key, 2));
        assertEquals(EventAcceptStatus.STALE, ahead.status());
        assertTrue(ahead.reasonCode().contains("OUT_OF_ORDER"));
        assertEquals(0, createOrder.getDoCalls(), "乱序事件不得产生副作用");

        // 补投 step 0
        assertEquals(EventAcceptStatus.ACCEPTED, orchestrator.process(advance(key, 0)).status());

        // 再投递 step 0（过期），应识别为已完成的过期事件
        EventProcessingResult behind = orchestrator.process(advance(key, 0));
        assertEquals(EventAcceptStatus.STALE, behind.status());
        assertTrue(behind.reasonCode().contains("STEP_ALREADY_DONE"));
        assertEquals(1, reserve.getDoSuccessCount());

        // 正确顺序继续
        for (int i = 1; i < 4; i++) {
            assertEquals(EventAcceptStatus.ACCEPTED, orchestrator.process(advance(key, i)).status());
        }
        assertEquals(SagaState.COMPLETED, orchestrator.getSaga(key).getState());
    }

    @Test
    @DisplayName("并发冲突：expectedVersion 不匹配时返回 CONFLICT，不重复推进也不回退状态")
    void optimisticVersionConflictDetected() throws Exception {
        String key = "order-conflict";
        orchestrator.createSaga(key, "ORDER");
        orchestrator.process(advance(key, 0));
        long versionAfterStep0 = orchestrator.getSaga(key).getVersion();

        SagaEvent staleVersionEvent = new SagaEvent(
                UUID.randomUUID().toString(), key, EventType.STEP_ADVANCE,
                1, versionAfterStep0 - 1, Map.of(), Instant.now());
        EventProcessingResult conflict = orchestrator.process(staleVersionEvent);
        assertEquals(EventAcceptStatus.CONFLICT, conflict.status());
        assertTrue(conflict.reasonCode().contains("VERSION_CONFLICT"));
        assertEquals(0, debit.getDoCalls(), "冲突事件不得产生副作用");
        assertEquals(versionAfterStep0, orchestrator.getSaga(key).getVersion(), "状态版本不得变化");

        // 携带正确版本可以继续
        SagaEvent correct = new SagaEvent(
                UUID.randomUUID().toString(), key, EventType.STEP_ADVANCE,
                1, versionAfterStep0, Map.of(), Instant.now());
        assertEquals(EventAcceptStatus.ACCEPTED, orchestrator.process(correct).status());
    }

    @Test
    @DisplayName("并发推进：同一业务键、同一目标步骤的两个事件并发提交，恰好一个生效且副作用只发生一次")
    void concurrentAdvanceIsSerialized() throws Exception {
        String key = "order-concurrent";
        orchestrator.createSaga(key, "ORDER");

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            // 两个不同 eventId 同时推进 step0（至少一次投递下的并发重复推进）
            Future<EventProcessingResult> f1 = pool.submit(() -> orchestrator.process(advance(key, 0)));
            Future<EventProcessingResult> f2 = pool.submit(() -> orchestrator.process(advance(key, 0)));
            EventProcessingResult r1 = f1.get(5, TimeUnit.SECONDS);
            EventProcessingResult r2 = f2.get(5, TimeUnit.SECONDS);

            long accepted = List.of(r1, r2).stream()
                    .filter(r -> r.status() == EventAcceptStatus.ACCEPTED).count();
            long rejected = List.of(r1, r2).stream()
                    .filter(r -> r.status() == EventAcceptStatus.STALE
                            || r.status() == EventAcceptStatus.DUPLICATE
                            || r.status() == EventAcceptStatus.CONFLICT).count();
            assertEquals(1, accepted, "并发下同一目标步骤只能被推进一次");
            assertEquals(1, rejected, "另一个必须被识别为重复/过期/冲突");

            // 关键断言：正向副作用恰好一次，状态无回退
            assertEquals(1, reserve.getDoSuccessCount());
            assertEquals(0, orchestrator.getSaga(key).getCurrentStepIndex());
            assertEquals(SagaState.RUNNING, orchestrator.getSaga(key).getState());

            // 流程仍可继续推进到完成
            for (int i = 1; i < 4; i++) {
                assertEquals(EventAcceptStatus.ACCEPTED, orchestrator.process(advance(key, i)).status());
            }
            assertEquals(SagaState.COMPLETED, orchestrator.getSaga(key).getState());
        } finally {
            pool.shutdownNow();
        }
    }
}
