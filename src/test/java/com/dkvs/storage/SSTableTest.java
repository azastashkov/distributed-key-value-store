package com.dkvs.storage;

import com.dkvs.model.VersionedValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class SSTableTest {

    @TempDir
    Path tempDir;

    @Test
    void writeAndReadShouldWork() throws IOException {
        Path file = tempDir.resolve("test.dat");
        TreeMap<String, VersionedValue> data = new TreeMap<>();
        data.put("apple", new VersionedValue("fruit".getBytes(), 1, 1000L));
        data.put("banana", new VersionedValue("yellow".getBytes(), 2, 2000L));
        data.put("cherry", new VersionedValue("red".getBytes(), 3, 3000L));

        SSTable table = SSTable.write(file, data);

        assertEquals(3, table.entryCount());
        assertArrayEquals("fruit".getBytes(), table.get("apple").data());
        assertArrayEquals("yellow".getBytes(), table.get("banana").data());
        assertArrayEquals("red".getBytes(), table.get("cherry").data());
    }

    @Test
    void getNonExistentKeyShouldReturnNull() throws IOException {
        Path file = tempDir.resolve("test.dat");
        TreeMap<String, VersionedValue> data = new TreeMap<>();
        data.put("key1", new VersionedValue("v1".getBytes(), 1, 1000L));

        SSTable table = SSTable.write(file, data);
        assertNull(table.get("nonexistent"));
    }

    @Test
    void bloomFilterShouldRejectNonExistentKeys() throws IOException {
        Path file = tempDir.resolve("test.dat");
        TreeMap<String, VersionedValue> data = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            data.put("key-" + i, new VersionedValue(("val-" + i).getBytes(), 1, 1000L));
        }

        SSTable table = SSTable.write(file, data);

        // Keys in the table should pass bloom filter
        for (int i = 0; i < 100; i++) {
            assertTrue(table.mightContain("key-" + i));
        }

        // Most keys NOT in the table should be rejected
        int passed = 0;
        for (int i = 100; i < 1100; i++) {
            if (table.mightContain("key-" + i)) passed++;
        }
        assertTrue(passed < 50, "Bloom filter passing too many non-existent keys: " + passed);
    }

    @Test
    void openExistingShouldRestoreState() throws IOException {
        Path file = tempDir.resolve("test.dat");
        TreeMap<String, VersionedValue> data = new TreeMap<>();
        data.put("x", new VersionedValue("123".getBytes(), 5, 5000L));
        data.put("y", new VersionedValue("456".getBytes(), 10, 10000L));
        SSTable.write(file, data);

        // Reopen from disk
        SSTable reopened = SSTable.open(file);
        assertEquals(2, reopened.entryCount());
        assertArrayEquals("123".getBytes(), reopened.get("x").data());
        assertEquals(5, reopened.get("x").version());
        assertArrayEquals("456".getBytes(), reopened.get("y").data());
        assertEquals(10, reopened.get("y").version());
    }

    @Test
    void largeSSTableShouldWork() throws IOException {
        Path file = tempDir.resolve("large.dat");
        TreeMap<String, VersionedValue> data = new TreeMap<>();
        for (int i = 0; i < 10000; i++) {
            String key = String.format("key-%06d", i);
            data.put(key, new VersionedValue(("value-" + i).getBytes(), i, (long) i * 1000));
        }

        SSTable table = SSTable.write(file, data);
        assertEquals(10000, table.entryCount());

        // Spot-check a few keys
        assertArrayEquals("value-0".getBytes(), table.get("key-000000").data());
        assertArrayEquals("value-5000".getBytes(), table.get("key-005000").data());
        assertArrayEquals("value-9999".getBytes(), table.get("key-009999").data());
        assertNull(table.get("key-010000"));
    }
}
