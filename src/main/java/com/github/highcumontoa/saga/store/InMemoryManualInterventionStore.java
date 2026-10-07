package com.github.highcumontoa.saga.store;

import com.github.highcumontoa.saga.domain.ManualInterventionEntry;
import com.github.highcumontoa.saga.domain.ManualInterventionStatus;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于 ConcurrentHashMap 的人工介入清单存储。
 * 以 sagaId 为键，同一流程至多一条记录，重复登记只覆盖更新。
 */
@Component
public class InMemoryManualInterventionStore implements ManualInterventionStore {

    private final Map<String, ManualInterventionEntry> bySagaId = new ConcurrentHashMap<>();

    @Override
    public void upsert(ManualInterventionEntry entry) {
        bySagaId.put(entry.getSagaId(), entry);
    }

    @Override
    public Optional<ManualInterventionEntry> findBySagaId(String sagaId) {
        return Optional.ofNullable(bySagaId.get(sagaId));
    }

    @Override
    public List<ManualInterventionEntry> find(String businessKey, ManualInterventionStatus status) {
        return bySagaId.values().stream()
                .filter(e -> businessKey == null || e.getBusinessKey().equals(businessKey))
                .filter(e -> status == null || e.getStatus() == status)
                .sorted(Comparator.comparing(ManualInterventionEntry::getCreatedAt))
                .toList();
    }

    @Override
    public int purgeResolved() {
        int[] removed = {0};
        bySagaId.values().removeIf(e -> {
            if (e.getStatus() == ManualInterventionStatus.RESOLVED) {
                removed[0]++;
                return true;
            }
            return false;
        });
        return removed[0];
    }
}
