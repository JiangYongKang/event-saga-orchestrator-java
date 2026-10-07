package com.github.highcumontoa.saga.domain;

import java.time.Duration;

/** 步骤定义：名称、正向动作、补偿动作与执行时间预算（null 表示不限制）。 */
public record StepDefinition(String name, StepAction action, StepAction compensation,
                             Duration timeout) {

    /** 兼容构造：不设置时间预算。 */
    public StepDefinition(String name, StepAction action, StepAction compensation) {
        this(name, action, compensation, null);
    }
}
