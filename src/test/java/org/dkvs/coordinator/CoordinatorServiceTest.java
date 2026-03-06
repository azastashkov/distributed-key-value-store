package org.dkvs.coordinator;

import org.dkvs.cluster.ClusterService;
import org.dkvs.cluster.Node;
import org.dkvs.config.DkvsProperties;
import org.dkvs.model.VersionedValue;
import org.dkvs.storage.StorageEngine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorServiceTest {

    @TempDir
    Path tempDir;

    private CoordinatorService coordinator;
    private StorageEngine storageEngine;

    @BeforeEach
    void setUp() throws IOException {
        DkvsProperties props = new DkvsProperties();
        props.setNodeId("node-1");
        props.setHost("localhost");
        props.setPort(8080);
        props.setSeeds("localhost:8080");
        props.setDataDir(tempDir.toString());
        props.setReplicationFactor(1); // Single node for unit tests
        props.setWriteQuorum(1);
        props.setReadQuorum(1);
        props.setVirtualNodes(150);
        props.setMemtableFlushThreshold(1_048_576);

        ClusterService clusterService = new ClusterService(props);
        clusterService.init();

        storageEngine = new StorageEngine(props, new SimpleMeterRegistry());
        storageEngine.init();

        coordinator = new CoordinatorService(clusterService, storageEngine, props);
    }

    @Test
    void putAndGetShouldWorkLocally() throws Exception {
        coordinator.put("my-key", "my-value".getBytes());

        VersionedValue result = coordinator.get("my-key");
        assertNotNull(result);
        assertArrayEquals("my-value".getBytes(), result.data());
    }

    @Test
    void getNonExistentKeyShouldReturnNull() throws Exception {
        VersionedValue result = coordinator.get("missing");
        assertNull(result);
    }

    @Test
    void multiplePutsShouldReturnLatestVersion() throws Exception {
        coordinator.put("key1", "first".getBytes());
        Thread.sleep(5); // Ensure different timestamps
        coordinator.put("key1", "second".getBytes());

        VersionedValue result = coordinator.get("key1");
        assertNotNull(result);
        assertArrayEquals("second".getBytes(), result.data());
    }

    @Test
    void putShouldHandleBinaryData() throws Exception {
        byte[] binaryData = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE};
        coordinator.put("binary-key", binaryData);

        VersionedValue result = coordinator.get("binary-key");
        assertNotNull(result);
        assertArrayEquals(binaryData, result.data());
    }

    @Test
    void putShouldHandleLargeValues() throws Exception {
        byte[] largeValue = new byte[100_000];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        coordinator.put("large-key", largeValue);

        VersionedValue result = coordinator.get("large-key");
        assertNotNull(result);
        assertArrayEquals(largeValue, result.data());
    }

    @Test
    void multipleKeysShouldBeIndependent() throws Exception {
        coordinator.put("k1", "v1".getBytes());
        coordinator.put("k2", "v2".getBytes());
        coordinator.put("k3", "v3".getBytes());

        assertArrayEquals("v1".getBytes(), coordinator.get("k1").data());
        assertArrayEquals("v2".getBytes(), coordinator.get("k2").data());
        assertArrayEquals("v3".getBytes(), coordinator.get("k3").data());
    }

    @Test
    void coordinatorWithMultipleLocalReplicasShouldWork() throws Exception {
        // This tests that even with replicationFactor > available nodes,
        // writes still succeed with adjusted quorum
        DkvsProperties props = new DkvsProperties();
        props.setNodeId("single");
        props.setHost("localhost");
        props.setPort(9999);
        props.setSeeds("localhost:9999");
        props.setDataDir(tempDir.resolve("multi").toString());
        props.setReplicationFactor(3); // Want 3 but only 1 node
        props.setWriteQuorum(2);       // Will be adjusted to 1
        props.setReadQuorum(2);        // Will be adjusted to 1
        props.setVirtualNodes(150);
        props.setMemtableFlushThreshold(1_048_576);

        ClusterService cs = new ClusterService(props);
        cs.init();
        StorageEngine se = new StorageEngine(props, new SimpleMeterRegistry());
        se.init();
        CoordinatorService coord = new CoordinatorService(cs, se, props);

        coord.put("test", "value".getBytes());
        VersionedValue result = coord.get("test");
        assertNotNull(result);
        assertArrayEquals("value".getBytes(), result.data());
    }
}
