package com.dkvs.storage;

import com.dkvs.model.VersionedValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class SSTableManagerTest {

    @TempDir
    Path tempDir;

    private SSTableManager manager;

    @BeforeEach
    void setUp() throws IOException {
        manager = new SSTableManager(tempDir);
    }

    @Test
    void flushAndGetShouldWork() throws IOException {
        TreeMap<String, VersionedValue> data = new TreeMap<>();
        data.put("k1", new VersionedValue("v1".getBytes(), 1, 1000L));
        data.put("k2", new VersionedValue("v2".getBytes(), 1, 1000L));

        manager.flush(data);

        assertEquals(1, manager.tableCount());
        assertArrayEquals("v1".getBytes(), manager.get("k1").data());
        assertArrayEquals("v2".getBytes(), manager.get("k2").data());
    }

    @Test
    void newerTableShouldTakePrecedence() throws IOException {
        TreeMap<String, VersionedValue> batch1 = new TreeMap<>();
        batch1.put("k1", new VersionedValue("old".getBytes(), 1, 1000L));
        manager.flush(batch1);

        TreeMap<String, VersionedValue> batch2 = new TreeMap<>();
        batch2.put("k1", new VersionedValue("new".getBytes(), 2, 2000L));
        manager.flush(batch2);

        assertEquals(2, manager.tableCount());
        VersionedValue result = manager.get("k1");
        // Newest table is checked first, so we get the newer value
        assertArrayEquals("new".getBytes(), result.data());
        assertEquals(2, result.version());
    }

    @Test
    void nonExistentKeyShouldReturnNull() throws IOException {
        TreeMap<String, VersionedValue> data = new TreeMap<>();
        data.put("k1", new VersionedValue("v1".getBytes(), 1, 1000L));
        manager.flush(data);

        assertNull(manager.get("missing"));
    }

    @Test
    void reloadFromDiskShouldRestoreTables() throws IOException {
        TreeMap<String, VersionedValue> data = new TreeMap<>();
        data.put("persistent", new VersionedValue("data".getBytes(), 1, 1000L));
        manager.flush(data);

        // Create a new manager pointing at the same directory
        SSTableManager reloaded = new SSTableManager(tempDir);
        assertEquals(1, reloaded.tableCount());
        assertArrayEquals("data".getBytes(), reloaded.get("persistent").data());
    }
}
