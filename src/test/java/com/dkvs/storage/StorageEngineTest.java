package com.dkvs.storage;

import com.dkvs.config.DkvsProperties;
import com.dkvs.model.VersionedValue;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class StorageEngineTest {

    @TempDir
    Path tempDir;

    private StorageEngine engine;

    @BeforeEach
    void setUp() throws IOException {
        DkvsProperties props = new DkvsProperties();
        props.setNodeId("test-node");
        props.setDataDir(tempDir.toString());
        props.setMemtableFlushThreshold(1_048_576);

        engine = new StorageEngine(props, new SimpleMeterRegistry());
        engine.init();
    }

    @Test
    void putAndGetShouldWork() throws IOException {
        engine.put("key1", new VersionedValue("hello".getBytes(), 1, 1000L));

        VersionedValue result = engine.get("key1");
        assertNotNull(result);
        assertArrayEquals("hello".getBytes(), result.data());
    }

    @Test
    void getNonExistentShouldReturnNull() {
        assertNull(engine.get("missing"));
    }

    @Test
    void overwriteShouldReturnLatestValue() throws IOException {
        engine.put("key1", new VersionedValue("v1".getBytes(), 1, 1000L));
        engine.put("key1", new VersionedValue("v2".getBytes(), 2, 2000L));

        VersionedValue result = engine.get("key1");
        assertArrayEquals("v2".getBytes(), result.data());
        assertEquals(2, result.version());
    }

    @Test
    void memtableFlushShouldPersistData() throws IOException {
        DkvsProperties props = new DkvsProperties();
        props.setNodeId("flush-test");
        props.setDataDir(tempDir.toString());
        props.setMemtableFlushThreshold(100); // Very small threshold to trigger flush

        StorageEngine smallEngine = new StorageEngine(props, new SimpleMeterRegistry());
        smallEngine.init();

        // Write enough data to trigger a flush
        for (int i = 0; i < 50; i++) {
            String key = "key-" + i;
            byte[] value = ("value-that-is-long-enough-to-fill-memtable-" + i).getBytes();
            smallEngine.put(key, new VersionedValue(value, 1, 1000L));
        }

        // Data should still be readable (from SSTable or memtable)
        for (int i = 0; i < 50; i++) {
            VersionedValue result = smallEngine.get("key-" + i);
            assertNotNull(result, "key-" + i + " should exist");
        }

        smallEngine.shutdown();
    }

    @Test
    void keyCountShouldTrackEntries() throws IOException {
        assertEquals(0, engine.getKeyCount());

        engine.put("a", new VersionedValue("1".getBytes(), 1, 1000L));
        assertEquals(1, engine.getKeyCount());

        engine.put("b", new VersionedValue("2".getBytes(), 1, 1000L));
        assertEquals(2, engine.getKeyCount());

        // Overwriting shouldn't increase count
        engine.put("a", new VersionedValue("3".getBytes(), 2, 2000L));
        assertEquals(2, engine.getKeyCount());
    }

    @Test
    void shutdownShouldFlushRemainingData() throws IOException {
        engine.put("persist-me", new VersionedValue("important".getBytes(), 1, 1000L));
        engine.shutdown();

        // Reopen a new engine on the same data dir
        DkvsProperties props = new DkvsProperties();
        props.setNodeId("test-node");
        props.setDataDir(tempDir.toString());
        StorageEngine reopened = new StorageEngine(props, new SimpleMeterRegistry());
        reopened.init();

        VersionedValue result = reopened.get("persist-me");
        assertNotNull(result);
        assertArrayEquals("important".getBytes(), result.data());
        reopened.shutdown();
    }
}
