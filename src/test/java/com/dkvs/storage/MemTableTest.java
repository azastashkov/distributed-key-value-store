package com.dkvs.storage;

import com.dkvs.model.VersionedValue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MemTableTest {

    @Test
    void putAndGetShouldWork() {
        MemTable table = new MemTable();
        VersionedValue val = new VersionedValue("hello".getBytes(), 1, 1000L);

        table.put("key1", val);

        VersionedValue result = table.get("key1");
        assertNotNull(result);
        assertArrayEquals("hello".getBytes(), result.data());
        assertEquals(1, result.version());
    }

    @Test
    void getNonExistentKeyShouldReturnNull() {
        MemTable table = new MemTable();
        assertNull(table.get("missing"));
    }

    @Test
    void overwriteShouldUpdateValue() {
        MemTable table = new MemTable();
        table.put("key1", new VersionedValue("v1".getBytes(), 1, 1000L));
        table.put("key1", new VersionedValue("v2".getBytes(), 2, 2000L));

        VersionedValue result = table.get("key1");
        assertNotNull(result);
        assertArrayEquals("v2".getBytes(), result.data());
        assertEquals(2, result.version());
    }

    @Test
    void entryCountShouldTrackDistinctKeys() {
        MemTable table = new MemTable();
        table.put("a", new VersionedValue("1".getBytes(), 1, 1000L));
        table.put("b", new VersionedValue("2".getBytes(), 1, 1000L));
        table.put("a", new VersionedValue("3".getBytes(), 2, 2000L)); // overwrite

        assertEquals(2, table.entryCount());
    }

    @Test
    void snapshotShouldReturnSortedCopy() {
        MemTable table = new MemTable();
        table.put("cherry", new VersionedValue("c".getBytes(), 1, 1000L));
        table.put("apple", new VersionedValue("a".getBytes(), 1, 1000L));
        table.put("banana", new VersionedValue("b".getBytes(), 1, 1000L));

        Map<String, VersionedValue> snapshot = table.snapshot();
        assertEquals(3, snapshot.size());
        // Verify sorted order
        String[] keys = snapshot.keySet().toArray(new String[0]);
        assertEquals("apple", keys[0]);
        assertEquals("banana", keys[1]);
        assertEquals("cherry", keys[2]);
    }

    @Test
    void clearShouldResetEverything() {
        MemTable table = new MemTable();
        table.put("key1", new VersionedValue("value".getBytes(), 1, 1000L));

        table.clear();

        assertTrue(table.isEmpty());
        assertEquals(0, table.entryCount());
        assertEquals(0, table.getSizeBytes());
        assertNull(table.get("key1"));
    }

    @Test
    void sizeShouldGrowWithEntries() {
        MemTable table = new MemTable();
        assertEquals(0, table.getSizeBytes());

        table.put("key1", new VersionedValue("value".getBytes(), 1, 1000L));
        assertTrue(table.getSizeBytes() > 0);

        long sizeAfterOne = table.getSizeBytes();
        table.put("key2", new VersionedValue("another-value".getBytes(), 1, 1000L));
        assertTrue(table.getSizeBytes() > sizeAfterOne);
    }
}
