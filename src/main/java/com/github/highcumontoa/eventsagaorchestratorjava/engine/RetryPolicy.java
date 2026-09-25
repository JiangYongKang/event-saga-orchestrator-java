package com.github.highcumontoa.eventsagaorchestratorjava.engine;

import java.time.Duration;

/**
 * 重试策略：最大尝试次数与初始退避（骨架）。
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff) {

    public static RetryPolicy forward() {
        return new RetryPolicy(3, Duration.ofMillis(10));
    }

    public static RetryPolicy compensation() {
        return new RetryPolicy(3, Duration.ofMillis(10));
    }
}
