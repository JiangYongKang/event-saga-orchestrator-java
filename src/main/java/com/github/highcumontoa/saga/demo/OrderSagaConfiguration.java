package com.github.highcumontoa.saga.demo;

import com.github.highcumontoa.saga.core.SagaDefinitionRegistry;
import com.github.highcumontoa.saga.domain.SagaDefinition;
import com.github.highcumontoa.saga.domain.StepDefinition;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 示例流程：订单履约（预留库存 -> 扣款 -> 创建发货单）。
 * 每步都带补偿动作，全部在单进程内执行，便于本地复现。
 */
@Configuration
public class OrderSagaConfiguration {

    public static final String ORDER_FULFILLMENT = "order-fulfillment";

    private static final Logger log = LoggerFactory.getLogger(OrderSagaConfiguration.class);

    private final SagaDefinitionRegistry registry;

    public OrderSagaConfiguration(SagaDefinitionRegistry registry) {
        this.registry = registry;
    }

    @PostConstruct
    void registerDefinitions() {
        registry.register(new SagaDefinition(ORDER_FULFILLMENT, List.of(
                new StepDefinition("reserve-inventory",
                        ctx -> log.info("[demo] 预留库存 businessKey={}", ctx.businessKey()),
                        ctx -> log.info("[demo] 释放库存 businessKey={}", ctx.businessKey())),
                new StepDefinition("charge-payment",
                        ctx -> log.info("[demo] 扣款 businessKey={}", ctx.businessKey()),
                        ctx -> log.info("[demo] 退款 businessKey={}", ctx.businessKey())),
                new StepDefinition("create-shipment",
                        ctx -> log.info("[demo] 创建发货单 businessKey={}", ctx.businessKey()),
                        ctx -> log.info("[demo] 取消发货单 businessKey={}", ctx.businessKey()))
        )));
        log.info("[demo] 已注册流程定义 {}", ORDER_FULFILLMENT);
    }
}
