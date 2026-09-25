package com.github.highcumontoa.eventsagaorchestratorjava.flow;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.SagaContext;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.StepAction;
import com.github.highcumontoa.eventsagaorchestratorjava.domain.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试/示例用可控步骤动作：
 *  - 正向与补偿的真实副作用分别计数（doSuccessCount / compensateSuccessCount），
 *    用于断言“重复事件不产生重复副作用”“补偿按预期次数执行”。
 *  - 前 failTimes 次正向调用、compensateFailTimes 次补偿调用返回失败；
 *    failException 时抛异常以验证异常归一化；timeoutReason 时返回 STEP_TIMEOUT。
 */
public class ControllableStepAction implements StepAction {

    private static final Logger log = LoggerFactory.getLogger(ControllableStepAction.class);

    private final String name;
    private final AtomicInteger doCalls = new AtomicInteger();
    private final AtomicInteger compensateCalls = new AtomicInteger();
    private final AtomicInteger doSuccess = new AtomicInteger();
    private final AtomicInteger compensateSuccess = new AtomicInteger();

    private volatile int failTimes;
    private volatile int compensateFailTimes;
    private volatile boolean failException;
    private volatile boolean timeout;

    public ControllableStepAction(String name) {
        this.name = name;
    }

    @Override
    public StepResult doStep(SagaContext context) {
        int call = doCalls.incrementAndGet();
        log.info("[SIDE-EFFECT:FORWARD] step={} call={} businessKey={}",
                name, call, context == null ? null : context.<String>get("businessKey"));
        if (call <= failTimes) {
            if (failException) {
                throw new IllegalStateException("simulated downstream failure at " + name);
            }
            return StepResult.fail(timeout ? "STEP_TIMEOUT" : "DOWNSTREAM_ERROR",
                    "simulated forward failure #" + call);
        }
        doSuccess.incrementAndGet();
        return StepResult.ok();
    }

    @Override
    public StepResult compensate(SagaContext context) {
        int call = compensateCalls.incrementAndGet();
        log.info("[SIDE-EFFECT:COMPENSATE] step={} call={} businessKey={}",
                name, call, context == null ? null : context.<String>get("businessKey"));
        if (call <= compensateFailTimes) {
            if (failException) {
                throw new IllegalStateException("simulated compensation failure at " + name);
            }
            return StepResult.fail("COMPENSATION_DOWNSTREAM_ERROR",
                    "simulated compensation failure #" + call);
        }
        compensateSuccess.incrementAndGet();
        return StepResult.ok();
    }

    public ControllableStepAction failForward(int times) {
        this.failTimes = times;
        return this;
    }

    public ControllableStepAction failCompensation(int times) {
        this.compensateFailTimes = times;
        return this;
    }

    public ControllableStepAction asException() {
        this.failException = true;
        return this;
    }

    public ControllableStepAction asTimeout() {
        this.timeout = true;
        return this;
    }

    public String getName() {
        return name;
    }

    public int getDoCalls() {
        return doCalls.get();
    }

    public int getCompensateCalls() {
        return compensateCalls.get();
    }

    public int getDoSuccessCount() {
        return doSuccess.get();
    }

    public int getCompensateSuccessCount() {
        return compensateSuccess.get();
    }
}
