package com.github.highcumontoa.eventsagaorchestratorjava.engine;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/** 按 businessKey 分段加锁，保证同一流程的并发推进被串行化。 */
@Component
public class KeyLockManager {

    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public ReentrantLock lockFor(String businessKey) {
        return locks.computeIfAbsent(businessKey, k -> new ReentrantLock());
    }
}
