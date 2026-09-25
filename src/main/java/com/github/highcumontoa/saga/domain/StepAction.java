package com.github.highcumontoa.saga.domain;

/** 步骤正向动作或补偿动作，可能抛出异常表示失败。 */
@FunctionalInterface
public interface StepAction {
    void run(StepContext context) throws Exception;
}
