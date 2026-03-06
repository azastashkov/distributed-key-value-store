package org.dkvs.storage;

import org.dkvs.model.VersionedValue;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SSTableManager {

    private final Path dataDir;
    private final CopyOnWriteArrayList<SSTable> tables = new CopyOnWriteArrayList<>();
    private final AtomicLong tableCounter = new AtomicLong(0);

    public SSTableManager(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        loadExistingTables();
    }

    private void loadExistingTables() throws IOException {
        List<Path> sstFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "sstable-*.dat")) {
            for (Path path : stream) {
                sstFiles.add(path);
            }
        }
        sstFiles.sort(Comparator.comparing(Path::getFileName));
        for (Path path : sstFiles) {
            try {
                SSTable table = SSTable.open(path);
                tables.add(table);
                // Extract counter from filename to avoid collisions
                String name = path.getFileName().toString();
                String num = name.replace("sstable-", "").replace(".dat", "");
                long val = Long.parseLong(num);
                tableCounter.updateAndGet(cur -> Math.max(cur, val + 1));
                log.info("Loaded SSTable: {} with {} entries", path.getFileName(), table.entryCount());
            } catch (IOException e) {
                log.warn("Failed to load SSTable {}: {}", path, e.getMessage());
            }
        }
    }

    /**
     * Flushes a sorted map of entries to a new SSTable.
     */
    public SSTable flush(Map<String, VersionedValue> sortedData) throws IOException {
        long id = tableCounter.getAndIncrement();
        Path filePath = dataDir.resolve(String.format("sstable-%010d.dat", id));
        SSTable table = SSTable.write(filePath, sortedData);
        tables.add(table);
        log.info("Flushed SSTable: {} with {} entries", filePath.getFileName(), table.entryCount());
        return table;
    }

    /**
     * Searches SSTables from newest to oldest, using BloomFilter to skip irrelevant ones.
     */
    public VersionedValue get(String key) {
        // Search newest first
        for (int i = tables.size() - 1; i >= 0; i--) {
            SSTable table = tables.get(i);
            if (!table.mightContain(key)) {
                continue;
            }
            try {
                VersionedValue value = table.get(key);
                if (value != null) {
                    return value;
                }
            } catch (IOException e) {
                log.warn("Error reading SSTable {}: {}", table.getFilePath(), e.getMessage());
            }
        }
        return null;
    }

    public int totalEntryCount() {
        return tables.stream().mapToInt(SSTable::entryCount).sum();
    }

    public int tableCount() {
        return tables.size();
    }
}
