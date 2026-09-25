package com.github.highcumontoa.eventsagaorchestratorjava.flow;

import com.github.highcumontoa.eventsagaorchestratorjava.domain.StepDefinition;
import com.github.highcumontoa.eventsagaorchestratorjava.engine.SagaFlow;
import com.github.highcumontoa.eventsagaorchestratorjava.engine.SagaFlowRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 示例“下单”流程：reserveInventory -> debitPayment -> createOrder -> notifyShipping。
 * 动作均为可控 Bean，默认全部成功；测试中按名字取出后注入故障。
 */
@Configuration
public class OrderFlowConfiguration {

    public static final String FLOW_TYPE = "ORDER";

    @Bean
    public ControllableStepAction reserveInventoryAction() {
        return new ControllableStepAction("reserveInventory");
    }

    @Bean
    public ControllableStepAction debitPaymentAction() {
        return new ControllableStepAction("debitPayment");
    }

    @Bean
    public ControllableStepAction createOrderAction() {
        return new ControllableStepAction("createOrder");
    }

    @Bean
    public ControllableStepAction notifyShippingAction() {
        return new ControllableStepAction("notifyShipping");
    }

    @Bean
    public SagaFlow orderFlow(ControllableStepAction reserveInventoryAction,
                              ControllableStepAction debitPaymentAction,
                              ControllableStepAction createOrderAction,
                              ControllableStepAction notifyShippingAction) {
        return new SagaFlow() {
            @Override
            public String flowType() {
                return FLOW_TYPE;
            }

            @Override
            public List<StepDefinition> steps() {
                return List.of(
                        new StepDefinition(0, "reserveInventory", reserveInventoryAction),
                        new StepDefinition(1, "debitPayment", debitPaymentAction),
                        new StepDefinition(2, "createOrder", createOrderAction),
                        new StepDefinition(3, "notifyShipping", notifyShippingAction));
            }
        };
    }

    @Bean
    public Object orderFlowRegistrar(SagaFlowRegistry registry, SagaFlow orderFlow) {
        registry.register(orderFlow);
        return new Object();
    }
}
