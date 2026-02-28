package com.dkvs.storage;

import com.dkvs.config.DkvsProperties;
import com.dkvs.model.VersionedValue;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@Component
public class StorageEngine {

    private final DkvsProperties properties;
    private final MeterRegistry meterRegistry;

    private CommitLog commitLog;
    private MemTable memTable;
    private SSTableManager ssTableManager;
    private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final AtomicLong keyCount = new AtomicLong(0);

    public StorageEngine(DkvsProperties properties, MeterRegistry meterRegistry) {
        this.properties = properties;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void init() throws IOException {
        Path dataDir = Path.of(properties.getDataDir(), properties.getNodeId());
        this.commitLog = new CommitLog(dataDir);
        this.memTable = new MemTable();
        this.ssTableManager = new SSTableManager(dataDir);

        // Replay commit log for crash recovery
        Map<String, VersionedValue> recovered = commitLog.replay();
        if (!recovered.isEmpty()) {
            log.info("Recovered {} entries from commit log", recovered.size());
            recovered.forEach(memTable::put);
        }

        keyCount.set(memTable.entryCount());

        // Register metrics
        Gauge.builder("dkvs.storage.key_count", keyCount, AtomicLong::get)
                .description("Number of key-value pairs on this node")
                .tag("node", properties.getNodeId())
                .register(meterRegistry);

        Gauge.builder("dkvs.storage.memtable_size_bytes", memTable, MemTable::getSizeBytes)
                .description("Current memtable size in bytes")
                .tag("node", properties.getNodeId())
                .register(meterRegistry);

        Gauge.builder("dkvs.storage.sstable_count", ssTableManager, SSTableManager::tableCount)
                .description("Number of SSTables on disk")
                .tag("node", properties.getNodeId())
                .register(meterRegistry);

        log.info("Storage engine initialized for node {} at {}", properties.getNodeId(), dataDir);
    }

    public void put(String key, VersionedValue value) throws IOException {
        flushLock.readLock().lock();
        try {
            commitLog.append(key, value);
            VersionedValue existing = memTable.get(key);
            if (existing == null) {
                keyCount.incrementAndGet();
            }
            memTable.put(key, value);
        } finally {
            flushLock.readLock().unlock();
        }

        // Check if memtable needs flushing
        if (memTable.getSizeBytes() >= properties.getMemtableFlushThreshold()) {
            flushMemTable();
        }
    }

    public VersionedValue get(String key) {
        flushLock.readLock().lock();
        try {
            // Check memtable first
            VersionedValue value = memTable.get(key);
            if (value != null) {
                return value;
            }
        } finally {
            flushLock.readLock().unlock();
        }

        // Fall back to SSTables
        return ssTableManager.get(key);
    }

    private void flushMemTable() throws IOException {
        flushLock.writeLock().lock();
        try {
            if (memTable.isEmpty() || memTable.getSizeBytes() < properties.getMemtableFlushThreshold()) {
                return; // Double-check after acquiring write lock
            }

            log.info("Flushing memtable: {} entries, {} bytes",
                    memTable.entryCount(), memTable.getSizeBytes());

            Map<String, VersionedValue> snapshot = memTable.snapshot();
            ssTableManager.flush(snapshot);
            memTable.clear();
            commitLog.reset();

            log.info("Memtable flush complete");
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    @PreDestroy
    public void shutdown() throws IOException {
        log.info("Shutting down storage engine");
        // Flush remaining memtable data
        flushLock.writeLock().lock();
        try {
            if (!memTable.isEmpty()) {
                Map<String, VersionedValue> snapshot = memTable.snapshot();
                ssTableManager.flush(snapshot);
                memTable.clear();
                commitLog.reset();
            }
            commitLog.close();
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    public long getKeyCount() {
        return keyCount.get();
    }
}
