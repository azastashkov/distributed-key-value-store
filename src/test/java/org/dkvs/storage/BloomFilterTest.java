package org.dkvs.storage;

import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    @Test
    void addedKeysShouldBeFound() {
        BloomFilter filter = new BloomFilter(1000, 0.01);

        filter.add("hello");
        filter.add("world");
        filter.add("test-key-123");

        assertTrue(filter.mightContain("hello"));
        assertTrue(filter.mightContain("world"));
        assertTrue(filter.mightContain("test-key-123"));
    }

    @Test
    void nonExistentKeysShouldMostlyNotBeFound() {
        BloomFilter filter = new BloomFilter(1000, 0.01);

        for (int i = 0; i < 1000; i++) {
            filter.add("key-" + i);
        }

        // Check false positive rate with keys that were never added
        int falsePositives = 0;
        int testCount = 10000;
        for (int i = 1000; i < 1000 + testCount; i++) {
            if (filter.mightContain("key-" + i)) {
                falsePositives++;
            }
        }

        double fpRate = (double) falsePositives / testCount;
        // Allow up to 5% false positive rate (we configured 1%)
        assertTrue(fpRate < 0.05, "False positive rate too high: " + fpRate);
    }

    @Test
    void serializationShouldPreserveState() throws IOException {
        BloomFilter original = new BloomFilter(100, 0.01);
        original.add("alpha");
        original.add("beta");
        original.add("gamma");

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        original.writeTo(new DataOutputStream(baos));

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BloomFilter restored = BloomFilter.readFrom(new DataInputStream(bais));

        assertTrue(restored.mightContain("alpha"));
        assertTrue(restored.mightContain("beta"));
        assertTrue(restored.mightContain("gamma"));
    }

    @Test
    void emptyFilterShouldNotContainAnything() {
        BloomFilter filter = new BloomFilter(100, 0.01);
        assertFalse(filter.mightContain("any-key"));
    }
}
