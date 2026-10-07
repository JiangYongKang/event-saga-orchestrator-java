package com.github.highcumontoa.saga.manual;

import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 基于 ConcurrentHashMap 的人工介入清单存储。 */
@Component
public class InMemoryManualInterventionStore implements ManualInterventionStore {

    private final Map<String, ManualInterventionEntry> byId = new ConcurrentHashMap<>();
    private final Map<String, String> entryIdBySagaId = new ConcurrentHashMap<>();

    @Override
    public void upsert(ManualInterventionEntry entry) {
        byId.put(entry.getEntryId(), entry);
        entryIdBySagaId.put(entry.getSagaId(), entry.getEntryId());
    }

    @Override
    public Optional<ManualInterventionEntry> findById(String entryId) {
        return Optional.ofNullable(byId.get(entryId));
    }

    @Override
    public Optional<ManualInterventionEntry> findBySagaId(String sagaId) {
        return Optional.ofNullable(entryIdBySagaId.get(sagaId)).map(byId::get);
    }

    @Override
    public List<ManualInterventionEntry> query(String businessKey, ManualInterventionStatus status) {
        return byId.values().stream()
                .filter(e -> businessKey == null || e.getBusinessKey().equals(businessKey))
                .filter(e -> status == null || e.getStatus() == status)
                .sorted(Comparator.comparing(ManualInterventionEntry::getCreatedAt))
                .toList();
    }

    @Override
    public boolean remove(String entryId) {
        ManualInterventionEntry removed = byId.remove(entryId);
        if (removed != null) {
            entryIdBySagaId.remove(removed.getSagaId(), entryId);
            return true;
        }
        return false;
    }
}
