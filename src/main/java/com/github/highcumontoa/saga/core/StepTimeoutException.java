package com.github.highcumontoa.saga.core;

/** 步骤执行超出时间预算（内部使用，不对外泄露堆栈）。 */
public class StepTimeoutException extends Exception {

    public StepTimeoutException(String message) {
        super(message);
    }
}
