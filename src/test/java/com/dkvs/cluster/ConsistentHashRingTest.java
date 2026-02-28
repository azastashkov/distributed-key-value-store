package com.dkvs.cluster;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {

    @Test
    void singleNodeShouldAlwaysBeSelected() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        ring.addNode("node-1");

        List<String> replicas = ring.getReplicaNodes("any-key", 3);
        assertEquals(1, replicas.size());
        assertEquals("node-1", replicas.get(0));
    }

    @Test
    void replicasShouldBeDistinctPhysicalNodes() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        List<String> replicas = ring.getReplicaNodes("test-key", 3);
        assertEquals(3, replicas.size());
        assertEquals(3, new HashSet<>(replicas).size()); // All distinct
    }

    @Test
    void sameKeyShouldMapToSameNodes() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        List<String> first = ring.getReplicaNodes("stable-key", 2);
        List<String> second = ring.getReplicaNodes("stable-key", 2);

        assertEquals(first, second);
    }

    @Test
    void removeNodeShouldRedistribute() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        ring.removeNode("node-2");

        assertEquals(2, ring.size());
        List<String> replicas = ring.getReplicaNodes("key", 3);
        // Should only have 2 nodes now
        assertEquals(2, replicas.size());
        assertFalse(replicas.contains("node-2"));
    }

    @Test
    void distributionShouldBeReasonablyEven() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");
        ring.addNode("node-3");

        Map<String, Integer> counts = new HashMap<>();
        int totalKeys = 10000;

        for (int i = 0; i < totalKeys; i++) {
            String primary = ring.getPrimaryNode("key-" + i);
            counts.merge(primary, 1, Integer::sum);
        }

        // Each node should have at least 20% of keys (ideal is 33%)
        for (int count : counts.values()) {
            assertTrue(count > totalKeys * 0.2,
                    "Uneven distribution: " + counts);
        }
    }

    @Test
    void emptyRingShouldReturnEmptyList() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        List<String> replicas = ring.getReplicaNodes("key", 3);
        assertTrue(replicas.isEmpty());
    }

    @Test
    void requestMoreReplicasThanNodesShouldReturnAllNodes() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        ring.addNode("node-1");
        ring.addNode("node-2");

        List<String> replicas = ring.getReplicaNodes("key", 5);
        assertEquals(2, replicas.size());
    }

    @Test
    void addNodeShouldBeReflectedInRing() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        ring.addNode("node-1");

        assertEquals(1, ring.size());
        assertTrue(ring.containsNode("node-1"));
        assertFalse(ring.containsNode("node-2"));
    }
}
