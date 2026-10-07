package com.github.highcumontoa.saga;

import com.github.highcumontoa.saga.core.DefaultSagaOrchestrator;
import com.github.highcumontoa.saga.core.InMemorySagaDefinitionRegistry;
import com.github.highcumontoa.saga.domain.SagaDefinition;
import com.github.highcumontoa.saga.domain.SagaInstance;
import com.github.highcumontoa.saga.domain.SagaStatus;
import com.github.highcumontoa.saga.domain.StepAction;
import com.github.highcumontoa.saga.domain.StepDefinition;
import com.github.highcumontoa.saga.event.AckReason;
import com.github.highcumontoa.saga.event.EventAck;
import com.github.highcumontoa.saga.event.EventType;
import com.github.highcumontoa.saga.event.SagaEvent;
import com.github.highcumontoa.saga.store.InMemoryManualInterventionStore;
import com.github.highcumontoa.saga.store.InMemorySagaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaOrchestratorTest {

    private InMemorySagaStore store;
    private InMemorySagaDefinitionRegistry registry;
    private InMemoryManualInterventionStore interventionStore;
    private DefaultSagaOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        store = new InMemorySagaStore();
        registry = new InMemorySagaDefinitionRegistry();
        interventionStore = new InMemoryManualInterventionStore();
        orchestrator = new DefaultSagaOrchestrator(store, registry, interventionStore);
    }

    private void registerThreeStepSaga(String name, List<AtomicInteger> actionCounters,
                                       List<AtomicInteger> compensationCounters) {
        registerThreeStepSaga(name, actionCounters, compensationCounters, null);
    }

    /** 注册三步流程；compensationHook 可在补偿动作前注入自定义行为（如失败注入）。 */
    private void registerThreeStepSaga(String name, List<AtomicInteger> actionCounters,
                                       List<AtomicInteger> compensationCounters,
                                       StepAction compensationHook) {
        List<StepDefinition> steps = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            AtomicInteger actionCounter = actionCounters.get(i);
            AtomicInteger compensationCounter = compensationCounters.get(i);
            steps.add(new StepDefinition("step-" + (i + 1),
                    ctx -> actionCounter.incrementAndGet(),
                    ctx -> {
                        compensationCounter.incrementAndGet();
                        if (compensationHook != null) {
                            compensationHook.run(ctx);
                        }
                    }));
        }
        registry.register(new SagaDefinition(name, steps));
    }

    private SagaEvent event(String businessKey, String stepName, EventType type) {
        return new SagaEvent(UUID.randomUUID().toString(), businessKey, stepName,
                type, null, Instant.now());
    }

    @Test
    void happyPathReachesCompleted() {
        List<AtomicInteger> actions = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        registerThreeStepSaga("happy", actions, comps);

        String sagaId = orchestrator.createSaga("happy", "order-1");
        assertEquals(SagaStatus.NEW, orchestrator.getSaga(sagaId).getStatus());

        for (int i = 1; i <= 3; i++) {
            EventAck ack = orchestrator.handleEvent(event("order-1", "step-" + i, EventType.STEP_SUCCEEDED));
            assertTrue(ack.applied());
            assertEquals(AckReason.APPLIED, ack.reason());
        }
        assertEquals(SagaStatus.COMPLETED, orchestrator.getSaga(sagaId).getStatus());
        actions.forEach(c -> assertEquals(1, c.get(), "每个步骤动作应恰好执行一次"));
        comps.forEach(c -> assertEquals(0, c.get(), "成功路径不应触发补偿"));
    }

    @Test
    void duplicateDeliveryDoesNotRepeatSideEffects() {
        List<AtomicInteger> actions = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        registerThreeStepSaga("dup", actions, comps);

        orchestrator.createSaga("dup", "order-2");
        SagaEvent first = event("order-2", "step-1", EventType.STEP_SUCCEEDED);

        EventAck ack1 = orchestrator.handleEvent(first);
        EventAck ack2 = orchestrator.handleEvent(first); // 同一 eventId 重复投递
        EventAck ack3 = orchestrator.handleEvent(first);

        assertTrue(ack1.applied());
        assertFalse(ack2.applied());
        assertEquals(AckReason.DUPLICATE_EVENT, ack2.reason());
        assertEquals(AckReason.DUPLICATE_EVENT, ack3.reason());
        assertEquals(1, actions.get(0).get(), "重复投递不得重复执行步骤动作");
    }

    @Test
    void outOfOrderEventIsRejectedWithoutStateChange() {
        List<AtomicInteger> actions = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        registerThreeStepSaga("ooo", actions, comps);

        String sagaId = orchestrator.createSaga("ooo", "order-3");

        // 乱序：step-2 的事件先于 step-1 到达
        EventAck early = orchestrator.handleEvent(event("order-3", "step-2", EventType.STEP_SUCCEEDED));
        assertFalse(early.applied());
        assertEquals(AckReason.OUT_OF_ORDER, early.reason());
        assertEquals(SagaStatus.NEW, orchestrator.getSaga(sagaId).getStatus());
        assertEquals(0, actions.get(1).get(), "乱序事件不得产生副作用");

        // 按正确顺序推进仍然成功
        assertTrue(orchestrator.handleEvent(event("order-3", "step-1", EventType.STEP_SUCCEEDED)).applied());
        assertTrue(orchestrator.handleEvent(event("order-3", "step-2", EventType.STEP_SUCCEEDED)).applied());
        assertEquals(1, actions.get(0).get());
        assertEquals(1, actions.get(1).get());
    }

    @Test
    void concurrentAdvancementAppliesExactlyOncePerStep() throws InterruptedException {
        List<AtomicInteger> actions = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        registerThreeStepSaga("conc", actions, comps);

        String sagaId = orchestrator.createSaga("conc", "order-4");
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        var appliedPerStep = new ConcurrentHashMap<String, AtomicInteger>();

        for (int step = 1; step <= 3; step++) {
            String stepName = "step-" + step;
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        EventAck ack = orchestrator.handleEvent(
                                event("order-4", stepName, EventType.STEP_SUCCEEDED));
                        if (ack.applied()) {
                            appliedPerStep.computeIfAbsent(stepName, k -> new AtomicInteger())
                                    .incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            go.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
        }
        pool.shutdownNow();

        assertEquals(SagaStatus.COMPLETED, orchestrator.getSaga(sagaId).getStatus());
        for (int step = 1; step <= 3; step++) {
            assertEquals(1, appliedPerStep.get("step-" + step).get(),
                    "并发下同一步骤只允许一个事件被应用");
            assertEquals(1, actions.get(step - 1).get(),
                    "并发下步骤动作不得重复执行");
        }
    }

    @Test
    void failedStepTriggersReverseCompensation() {
        List<AtomicInteger> actions = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        registerThreeStepSaga("fail", actions, comps);

        String sagaId = orchestrator.createSaga("fail", "order-5");
        orchestrator.handleEvent(event("order-5", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("order-5", "step-2", EventType.STEP_SUCCEEDED));

        EventAck ack = orchestrator.handleEvent(event("order-5", "step-3", EventType.STEP_FAILED));
        assertTrue(ack.applied());
        assertEquals(SagaStatus.COMPENSATED, orchestrator.getSaga(sagaId).getStatus());
        assertEquals(1, comps.get(0).get(), "step-1 应补偿一次");
        assertEquals(1, comps.get(1).get(), "step-2 应补偿一次");
        assertEquals(0, comps.get(2).get(), "step-3 未执行成功，不应补偿");
    }

    @Test
    void compensationRetriesUntilSuccess() {
        List<AtomicInteger> actions = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        AtomicInteger hookCalls = new AtomicInteger();
        // 补偿前两次失败，第三次成功
        registerThreeStepSaga("retry", actions, comps, ctx -> {
            if (hookCalls.incrementAndGet() < 3) {
                throw new IllegalStateException("注入的补偿失败");
            }
        });

        String sagaId = orchestrator.createSaga("retry", "order-6");
        orchestrator.handleEvent(event("order-6", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("order-6", "step-2", EventType.STEP_FAILED));

        SagaInstance instance = orchestrator.getSaga(sagaId);
        assertEquals(SagaStatus.COMPENSATED, instance.getStatus());
        assertEquals(3, instance.getSteps().get(0).getCompensationAttempts(),
                "补偿应重试至第三次成功");
    }

    @Test
    void compensationExhaustionLeadsToManualInterventionTerminalState() {
        List<AtomicInteger> actions = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        // 补偿永远失败
        registerThreeStepSaga("stuck", actions, comps, ctx -> {
            throw new IllegalStateException("注入的永久补偿失败");
        });

        String sagaId = orchestrator.createSaga("stuck", "order-7");
        orchestrator.handleEvent(event("order-7", "step-1", EventType.STEP_SUCCEEDED));
        orchestrator.handleEvent(event("order-7", "step-2", EventType.STEP_FAILED));

        SagaInstance instance = orchestrator.getSaga(sagaId);
        assertEquals(SagaStatus.MANUAL_INTERVENTION, instance.getStatus(),
                "补偿重试耗尽后必须进入人工介入终态，不得停留在中间态");
        assertEquals(3, instance.getSteps().get(0).getCompensationAttempts(),
                "补偿尝试次数应等于最大重试次数");

        // 终态后的事件被忽略且可区分
        EventAck late = orchestrator.handleEvent(event("order-7", "step-2", EventType.STEP_SUCCEEDED));
        assertFalse(late.applied());
        assertEquals(AckReason.IGNORED_TERMINAL_STATE, late.reason());
        assertEquals(SagaStatus.MANUAL_INTERVENTION, orchestrator.getSaga(sagaId).getStatus());
    }

    @Test
    void concurrentCreateWithSameBusinessKeyReturnsSameSaga() throws InterruptedException {
        List<AtomicInteger> actions = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<AtomicInteger> comps = List.of(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        registerThreeStepSaga("create", actions, comps);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        var ids = ConcurrentHashMap.<String>newKeySet();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    go.await();
                    ids.add(orchestrator.createSaga("create", "order-8"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1, ids.size(), "同一业务键并发创建必须收敛到同一流程实例");
    }
}
