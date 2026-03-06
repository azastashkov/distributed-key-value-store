package org.dkvs.storage;

import org.dkvs.model.VersionedValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommitLogTest {

    @TempDir
    Path tempDir;

    private CommitLog commitLog;

    @BeforeEach
    void setUp() throws IOException {
        commitLog = new CommitLog(tempDir);
    }

    @Test
    void appendAndReplayShouldRecoverEntries() throws IOException {
        commitLog.append("key1", new VersionedValue("value1".getBytes(), 1, 1000L));
        commitLog.append("key2", new VersionedValue("value2".getBytes(), 1, 2000L));
        commitLog.append("key3", new VersionedValue("value3".getBytes(), 1, 3000L));

        Map<String, VersionedValue> recovered = commitLog.replay();

        assertEquals(3, recovered.size());
        assertArrayEquals("value1".getBytes(), recovered.get("key1").data());
        assertArrayEquals("value2".getBytes(), recovered.get("key2").data());
        assertArrayEquals("value3".getBytes(), recovered.get("key3").data());
    }

    @Test
    void replayShouldKeepHigherVersion() throws IOException {
        commitLog.append("key1", new VersionedValue("old".getBytes(), 1, 1000L));
        commitLog.append("key1", new VersionedValue("new".getBytes(), 5, 5000L));

        Map<String, VersionedValue> recovered = commitLog.replay();

        assertEquals(1, recovered.size());
        assertArrayEquals("new".getBytes(), recovered.get("key1").data());
        assertEquals(5, recovered.get("key1").version());
    }

    @Test
    void replayEmptyLogShouldReturnEmptyMap() {
        Map<String, VersionedValue> recovered = commitLog.replay();
        assertTrue(recovered.isEmpty());
    }

    @Test
    void resetShouldClearLog() throws IOException {
        commitLog.append("key1", new VersionedValue("value1".getBytes(), 1, 1000L));
        commitLog.reset();

        Map<String, VersionedValue> recovered = commitLog.replay();
        assertTrue(recovered.isEmpty());
    }

    @Test
    void reopenedLogShouldRecoverData() throws IOException {
        commitLog.append("key1", new VersionedValue("value1".getBytes(), 1, 1000L));
        commitLog.close();

        // Reopen
        CommitLog reopened = new CommitLog(tempDir);
        Map<String, VersionedValue> recovered = reopened.replay();

        assertEquals(1, recovered.size());
        assertArrayEquals("value1".getBytes(), recovered.get("key1").data());
        reopened.close();
    }
}
