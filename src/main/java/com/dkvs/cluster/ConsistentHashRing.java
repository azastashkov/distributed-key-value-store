package com.dkvs.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashRing {

    private final int virtualNodes;
    private final ConcurrentSkipListMap<Long, String> ring = new ConcurrentSkipListMap<>();
    private final Set<String> physicalNodes = Collections.synchronizedSet(new LinkedHashSet<>());

    public ConsistentHashRing(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    public void addNode(String nodeId) {
        physicalNodes.add(nodeId);
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(nodeId + "-vnode-" + i);
            ring.put(hash, nodeId);
        }
    }

    public void removeNode(String nodeId) {
        physicalNodes.remove(nodeId);
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(nodeId + "-vnode-" + i);
            ring.remove(hash);
        }
    }

    public boolean containsNode(String nodeId) {
        return physicalNodes.contains(nodeId);
    }

    /**
     * Returns up to {@code count} distinct physical nodes responsible for the given key,
     * walking clockwise around the ring.
     */
    public List<String> getReplicaNodes(String key, int count) {
        if (ring.isEmpty()) {
            return Collections.emptyList();
        }

        long hash = hash(key);
        List<String> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        // Walk clockwise from the hash position
        NavigableMap<Long, String> tailMap = ring.tailMap(hash, true);
        collectNodes(tailMap.values(), result, seen, count);

        // Wrap around if needed
        if (result.size() < count) {
            collectNodes(ring.values(), result, seen, count);
        }

        return result;
    }

    /**
     * Returns the primary node (first replica) responsible for the given key.
     */
    public String getPrimaryNode(String key) {
        List<String> nodes = getReplicaNodes(key, 1);
        return nodes.isEmpty() ? null : nodes.get(0);
    }

    public Set<String> getPhysicalNodes() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(physicalNodes));
    }

    public int size() {
        return physicalNodes.size();
    }

    private void collectNodes(Collection<String> values, List<String> result, Set<String> seen, int count) {
        for (String nodeId : values) {
            if (result.size() >= count) break;
            if (seen.add(nodeId)) {
                result.add(nodeId);
            }
        }
    }

    static long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // Use first 8 bytes as a long
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
