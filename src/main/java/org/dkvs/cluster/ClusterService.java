package org.dkvs.cluster;

import org.dkvs.config.DkvsProperties;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class ClusterService {

    private final DkvsProperties properties;
    @Getter
    private final ConsistentHashRing hashRing;
    private final ConcurrentHashMap<String, Node> nodes = new ConcurrentHashMap<>();

    public ClusterService(DkvsProperties properties) {
        this.properties = properties;
        this.hashRing = new ConsistentHashRing(properties.getVirtualNodes());
    }

    @PostConstruct
    public void init() {
        // Register self
        Node self = new Node(
                properties.getNodeId(),
                properties.getHost(),
                properties.getPort(),
                0L,
                Node.Status.UP,
                System.currentTimeMillis()
        );
        nodes.put(self.getId(), self);
        hashRing.addNode(self.getId());
        log.info("Local node registered: {} at {}:{}", self.getId(), self.getHost(), self.getPort());
    }

    public Node getLocalNode() {
        return nodes.get(properties.getNodeId());
    }

    public Node getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public Collection<Node> getAllNodes() {
        return Collections.unmodifiableCollection(nodes.values());
    }

    public List<Node> getAliveNodes() {
        return nodes.values().stream()
                .filter(n -> n.getStatus() == Node.Status.UP)
                .toList();
    }

    /**
     * Returns the alive replica nodes for the given key.
     * Falls back to available up nodes if not enough replicas.
     */
    public List<Node> getReplicaNodesForKey(String key, int count) {
        List<String> nodeIds = hashRing.getReplicaNodes(key, count);
        List<Node> result = new ArrayList<>();
        for (String id : nodeIds) {
            Node node = nodes.get(id);
            if (node != null && node.getStatus() == Node.Status.UP) {
                result.add(node);
            }
        }
        return result;
    }

    /**
     * Increments the local node's heartbeat counter.
     */
    public void incrementHeartbeat() {
        Node local = getLocalNode();
        if (local != null) {
            local.setHeartbeat(local.getHeartbeat() + 1);
            local.setLastUpdated(System.currentTimeMillis());
        }
    }

    /**
     * Merges an incoming membership list from a gossip peer.
     * Higher heartbeat wins; also handles UP/DOWN transitions.
     */
    public void mergeGossipState(List<Node> incoming) {
        long now = System.currentTimeMillis();
        for (Node remote : incoming) {
            // Skip self
            if (remote.getId().equals(properties.getNodeId())) {
                continue;
            }

            Node existing = nodes.get(remote.getId());
            if (existing == null) {
                // New node discovered
                remote.setStatus(Node.Status.UP);
                remote.setLastUpdated(now);
                nodes.put(remote.getId(), remote);
                if (!hashRing.containsNode(remote.getId())) {
                    hashRing.addNode(remote.getId());
                }
                log.info("Discovered new node via gossip: {} at {}:{}", remote.getId(), remote.getHost(), remote.getPort());
            } else if (remote.getHeartbeat() > existing.getHeartbeat()) {
                // Update with newer info
                existing.setHeartbeat(remote.getHeartbeat());
                existing.setHost(remote.getHost());
                existing.setPort(remote.getPort());
                existing.setLastUpdated(now);
                if (existing.getStatus() == Node.Status.DOWN) {
                    existing.setStatus(Node.Status.UP);
                    if (!hashRing.containsNode(existing.getId())) {
                        hashRing.addNode(existing.getId());
                    }
                    log.info("Node {} marked UP via gossip", existing.getId());
                }
            }
        }
    }

    /**
     * Marks nodes as DOWN if they haven't been heard from within the timeout.
     */
    public void detectFailures() {
        long now = System.currentTimeMillis();
        for (Node node : nodes.values()) {
            if (node.getId().equals(properties.getNodeId())) {
                continue; // Don't mark self as down
            }
            if (node.getStatus() == Node.Status.UP
                    && (now - node.getLastUpdated()) > properties.getNodeDownTimeoutMs()) {
                node.setStatus(Node.Status.DOWN);
                hashRing.removeNode(node.getId());
                log.warn("Node {} marked DOWN (no heartbeat for {}ms)", node.getId(), now - node.getLastUpdated());
            }
        }
    }

    public List<String> getSeedAddresses() {
        return Arrays.stream(properties.getSeeds().split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}
