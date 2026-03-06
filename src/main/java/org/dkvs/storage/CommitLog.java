package org.dkvs.storage;

import org.dkvs.model.VersionedValue;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class CommitLog {

    private final Path logFile;
    private final ReentrantLock writeLock = new ReentrantLock();
    private DataOutputStream outputStream;

    public CommitLog(Path dataDir) throws IOException {
        this.logFile = dataDir.resolve("commitlog.bin");
        Files.createDirectories(dataDir);
        this.outputStream = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(logFile.toFile(), true)));
    }

    /**
     * Appends a key-value entry to the commit log.
     * Format: keyLen(int) | keyBytes | dataLen(int) | dataBytes | version(long) | timestamp(long)
     */
    public void append(String key, VersionedValue value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        writeLock.lock();
        try {
            outputStream.writeInt(keyBytes.length);
            outputStream.write(keyBytes);
            outputStream.writeInt(value.data().length);
            outputStream.write(value.data());
            outputStream.writeLong(value.version());
            outputStream.writeLong(value.timestamp());
            outputStream.flush();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Replays the commit log, returning all entries as a sorted map.
     * Used for recovery after a crash.
     */
    public Map<String, VersionedValue> replay() {
        ConcurrentSkipListMap<String, VersionedValue> entries = new ConcurrentSkipListMap<>();
        if (!Files.exists(logFile) || logFile.toFile().length() == 0) {
            return entries;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(logFile.toFile())))) {
            while (true) {
                try {
                    int keyLen = in.readInt();
                    byte[] keyBytes = new byte[keyLen];
                    in.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    int dataLen = in.readInt();
                    byte[] data = new byte[dataLen];
                    in.readFully(data);

                    long version = in.readLong();
                    long timestamp = in.readLong();

                    VersionedValue existing = entries.get(key);
                    VersionedValue newValue = new VersionedValue(data, version, timestamp);
                    if (existing == null || newValue.version() > existing.version()) {
                        entries.put(key, newValue);
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            log.warn("Error replaying commit log, partial recovery: {}", e.getMessage());
        }
        return entries;
    }

    /**
     * Resets the commit log after a successful memtable flush.
     */
    public void reset() throws IOException {
        writeLock.lock();
        try {
            outputStream.close();
            this.outputStream = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(logFile.toFile(), false)));
        } finally {
            writeLock.unlock();
        }
    }

    public void close() throws IOException {
        writeLock.lock();
        try {
            outputStream.close();
        } finally {
            writeLock.unlock();
        }
    }
}
