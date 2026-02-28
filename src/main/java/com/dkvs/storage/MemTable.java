package com.dkvs.storage;

import com.dkvs.model.VersionedValue;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable {

    private final ConcurrentSkipListMap<String, VersionedValue> data = new ConcurrentSkipListMap<>();
    private final AtomicLong sizeBytes = new AtomicLong(0);

    public void put(String key, VersionedValue value) {
        VersionedValue old = data.put(key, value);
        long delta = estimateEntrySize(key, value);
        if (old != null) {
            delta -= estimateEntrySize(key, old);
        }
        sizeBytes.addAndGet(delta);
    }

    public VersionedValue get(String key) {
        return data.get(key);
    }

    public long getSizeBytes() {
        return sizeBytes.get();
    }

    public int entryCount() {
        return data.size();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public Map<String, VersionedValue> snapshot() {
        return new ConcurrentSkipListMap<>(data);
    }

    public void clear() {
        data.clear();
        sizeBytes.set(0);
    }

    private long estimateEntrySize(String key, VersionedValue value) {
        // key bytes + value data bytes + version (8) + timestamp (8) + overhead
        return key.length() * 2L + value.data().length + 16 + 64;
    }
}
