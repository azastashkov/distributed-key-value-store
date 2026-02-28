package com.dkvs.cluster;

import com.dkvs.config.DkvsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterServiceTest {

    private ClusterService clusterService;
    private DkvsProperties properties;

    @BeforeEach
    void setUp() {
        properties = new DkvsProperties();
        properties.setNodeId("node-1");
        properties.setHost("localhost");
        properties.setPort(8080);
        properties.setSeeds("localhost:8080,localhost:8081,localhost:8082");
        properties.setVirtualNodes(150);
        properties.setNodeDownTimeoutMs(5000);

        clusterService = new ClusterService(properties);
        clusterService.init();
    }

    @Test
    void localNodeShouldBeRegistered() {
        Node local = clusterService.getLocalNode();
        assertNotNull(local);
        assertEquals("node-1", local.getId());
        assertEquals(Node.Status.UP, local.getStatus());
    }

    @Test
    void mergeGossipShouldAddNewNodes() {
        Node remote = new Node("node-2", "localhost", 8081, 1, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(remote));

        assertNotNull(clusterService.getNode("node-2"));
        assertEquals(2, clusterService.getAllNodes().size());
    }

    @Test
    void mergeGossipShouldUpdateHigherHeartbeat() {
        Node remote = new Node("node-2", "localhost", 8081, 5, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(remote));

        Node updated = new Node("node-2", "localhost", 8081, 10, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(updated));

        assertEquals(10, clusterService.getNode("node-2").getHeartbeat());
    }

    @Test
    void mergeGossipShouldNotDowngradHeartbeat() {
        Node remote = new Node("node-2", "localhost", 8081, 10, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(remote));

        Node stale = new Node("node-2", "localhost", 8081, 5, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(stale));

        assertEquals(10, clusterService.getNode("node-2").getHeartbeat());
    }

    @Test
    void mergeGossipShouldIgnoreSelf() {
        Node self = new Node("node-1", "localhost", 8080, 999, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(self));

        // Self heartbeat should NOT be updated by gossip
        assertNotEquals(999, clusterService.getLocalNode().getHeartbeat());
    }

    @Test
    void detectFailuresShouldMarkDownOldNodes() {
        properties.setNodeDownTimeoutMs(100);

        Node remote = new Node("node-2", "localhost", 8081, 1, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(remote));

        // Simulate time passing: set lastUpdated to the past
        clusterService.getNode("node-2").setLastUpdated(System.currentTimeMillis() - 200);

        clusterService.detectFailures();

        assertEquals(Node.Status.DOWN, clusterService.getNode("node-2").getStatus());
    }

    @Test
    void getAliveNodesShouldExcludeDownNodes() {
        Node remote = new Node("node-2", "localhost", 8081, 1, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(remote));

        // Simulate time passing
        clusterService.getNode("node-2").setLastUpdated(System.currentTimeMillis() - 200);

        properties.setNodeDownTimeoutMs(100);
        clusterService.detectFailures();

        List<Node> alive = clusterService.getAliveNodes();
        assertEquals(1, alive.size());
        assertEquals("node-1", alive.get(0).getId());
    }

    @Test
    void getReplicaNodesForKeyShouldReturnAliveNodes() {
        Node node2 = new Node("node-2", "localhost", 8081, 1, Node.Status.UP, System.currentTimeMillis());
        Node node3 = new Node("node-3", "localhost", 8082, 1, Node.Status.UP, System.currentTimeMillis());
        clusterService.mergeGossipState(List.of(node2, node3));

        List<Node> replicas = clusterService.getReplicaNodesForKey("test-key", 3);
        assertEquals(3, replicas.size());
        for (Node n : replicas) {
            assertEquals(Node.Status.UP, n.getStatus());
        }
    }

    @Test
    void getSeedAddressesShouldParseConfig() {
        List<String> seeds = clusterService.getSeedAddresses();
        assertEquals(3, seeds.size());
        assertTrue(seeds.contains("localhost:8080"));
        assertTrue(seeds.contains("localhost:8081"));
        assertTrue(seeds.contains("localhost:8082"));
    }
}
