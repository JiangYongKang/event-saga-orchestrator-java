package com.github.highcumontoa.eventsagaorchestratorjava.deadletter;

import com.github.highcumontoa.eventsagaorchestratorjava.event.SagaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 单进程内存死信队列：
 * 补偿重试耗尽、无法归类处理的事件进入死信，绝不静默丢弃；
 * 同时输出 error 级别事件流水，便于人工介入定位。
 */
@Component
public class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    private final CopyOnWriteArrayList<DeadLetterEntry> entries = new CopyOnWriteArrayList<>();

    public void publish(SagaEvent event, String reasonCode, String detail) {
        DeadLetterEntry entry = new DeadLetterEntry(event, reasonCode, detail, Instant.now());
        entries.add(entry);
        log.error("[DEAD-LETTER] businessKey={} eventId={} type={} reason={} detail={}",
                event == null ? null : event.businessKey(),
                event == null ? null : event.eventId(),
                event == null ? null : event.type(),
                reasonCode,
                detail);
    }

    public List<DeadLetterEntry> findByBusinessKey(String businessKey) {
        return entries.stream()
                .filter(e -> e.event() != null && businessKey.equals(e.event().businessKey()))
                .toList();
    }

    public List<DeadLetterEntry> findAll() {
        return List.copyOf(entries);
    }

    public int size() {
        return entries.size();
    }
}
