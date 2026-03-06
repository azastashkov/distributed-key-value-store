package org.dkvs.coordinator;

import org.dkvs.cluster.ClusterService;
import org.dkvs.cluster.Node;
import org.dkvs.config.DkvsProperties;
import org.dkvs.model.VersionedValue;
import org.dkvs.storage.StorageEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Service
public class CoordinatorService {

    private final ClusterService clusterService;
    private final StorageEngine storageEngine;
    private final DkvsProperties properties;
    private final RestClient restClient;
    private final ExecutorService executor;

    public CoordinatorService(ClusterService clusterService, StorageEngine storageEngine, DkvsProperties properties) {
        this.clusterService = clusterService;
        this.storageEngine = storageEngine;
        this.properties = properties;
        this.restClient = RestClient.builder().build();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Coordinates a PUT across replica nodes with write quorum.
     */
    public void put(String key, byte[] data) throws Exception {
        long version = System.currentTimeMillis();
        long timestamp = System.currentTimeMillis();
        VersionedValue value = new VersionedValue(data, version, timestamp);

        int replicationFactor = properties.getReplicationFactor();
        int writeQuorum = properties.getWriteQuorum();

        List<Node> replicas = clusterService.getReplicaNodesForKey(key, replicationFactor);
        if (replicas.isEmpty()) {
            throw new RuntimeException("No available replica nodes for key");
        }

        // Adjust quorum if not enough nodes
        int effectiveQuorum = Math.min(writeQuorum, replicas.size());

        String localNodeId = properties.getNodeId();
        List<Future<Boolean>> futures = new ArrayList<>();

        for (Node replica : replicas) {
            futures.add(executor.submit(() -> {
                if (replica.getId().equals(localNodeId)) {
                    // Write locally
                    storageEngine.put(key, value);
                    return true;
                } else {
                    // Write to remote replica
                    return sendPutToReplica(replica, key, value);
                }
            }));
        }

        // Wait for write quorum
        int acks = 0;
        List<Exception> errors = new ArrayList<>();
        for (Future<Boolean> future : futures) {
            try {
                boolean success = future.get(5, TimeUnit.SECONDS);
                if (success) {
                    acks++;
                }
            } catch (TimeoutException e) {
                errors.add(e);
                log.warn("Write to replica timed out for key {}", key);
            } catch (Exception e) {
                errors.add(e);
                log.warn("Write to replica failed for key {}: {}", key, e.getMessage());
            }

            if (acks >= effectiveQuorum) {
                break;
            }
        }

        if (acks < effectiveQuorum) {
            throw new RuntimeException(
                    String.format("Write quorum not met: got %d acks, need %d. Errors: %s",
                            acks, effectiveQuorum, errors));
        }

        log.debug("PUT key={} version={} acks={}/{}", key, version, acks, replicas.size());
    }

    /**
     * Coordinates a GET across replica nodes with read quorum.
     * Returns the value with the highest version.
     */
    public VersionedValue get(String key) throws Exception {
        int replicationFactor = properties.getReplicationFactor();
        int readQuorum = properties.getReadQuorum();

        List<Node> replicas = clusterService.getReplicaNodesForKey(key, replicationFactor);
        if (replicas.isEmpty()) {
            throw new RuntimeException("No available replica nodes for key");
        }

        int effectiveQuorum = Math.min(readQuorum, replicas.size());

        String localNodeId = properties.getNodeId();
        List<Future<VersionedValue>> futures = new ArrayList<>();

        for (Node replica : replicas) {
            futures.add(executor.submit(() -> {
                if (replica.getId().equals(localNodeId)) {
                    return storageEngine.get(key);
                } else {
                    return sendGetToReplica(replica, key);
                }
            }));
        }

        // Collect responses from at least readQuorum replicas
        List<VersionedValue> responses = new ArrayList<>();
        int responseCount = 0;
        for (Future<VersionedValue> future : futures) {
            try {
                VersionedValue val = future.get(5, TimeUnit.SECONDS);
                responseCount++;
                if (val != null) {
                    responses.add(val);
                }
            } catch (TimeoutException e) {
                log.warn("Read from replica timed out for key {}", key);
            } catch (Exception e) {
                log.warn("Read from replica failed for key {}: {}", key, e.getMessage());
            }

            if (responseCount >= effectiveQuorum) {
                break;
            }
        }

        if (responseCount < effectiveQuorum) {
            throw new RuntimeException(
                    String.format("Read quorum not met: got %d responses, need %d",
                            responseCount, effectiveQuorum));
        }

        // If replicas returned no data, check local storage as a fallback.
        // This covers the case where the hash ring changed after a write
        // (e.g. during cluster startup) and the current replicas differ
        // from the ones that originally stored the value.
        if (responses.isEmpty()) {
            boolean localWasQueried = replicas.stream()
                    .anyMatch(r -> r.getId().equals(localNodeId));
            if (!localWasQueried) {
                VersionedValue localValue = storageEngine.get(key);
                if (localValue != null) {
                    return localValue;
                }
            }
            return null;
        }

        // Return the value with the highest version
        return responses.stream()
                .max(Comparator.comparingLong(VersionedValue::version))
                .orElse(null);
    }

    private boolean sendPutToReplica(Node replica, String key, VersionedValue value) {
        try {
            String url = replica.baseUrl() + "/internal/data/" + encodeKey(key);
            restClient.put()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .header("X-DKVS-Version", String.valueOf(value.version()))
                    .header("X-DKVS-Timestamp", String.valueOf(value.timestamp()))
                    .body(value.data())
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (Exception e) {
            log.warn("PUT to replica {} failed: {}", replica.getId(), e.getMessage());
            return false;
        }
    }

    private VersionedValue sendGetToReplica(Node replica, String key) {
        try {
            String url = replica.baseUrl() + "/internal/data/" + encodeKey(key);
            var response = restClient.get()
                    .uri(url)
                    .retrieve()
                    .toEntity(byte[].class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                long version = Long.parseLong(
                        Objects.requireNonNull(response.getHeaders().getFirst("X-DKVS-Version")));
                long timestamp = Long.parseLong(
                        Objects.requireNonNull(response.getHeaders().getFirst("X-DKVS-Timestamp")));
                return new VersionedValue(response.getBody(), version, timestamp);
            }
            return null;
        } catch (Exception e) {
            log.warn("GET from replica {} failed: {}", replica.getId(), e.getMessage());
            return null;
        }
    }

    private String encodeKey(String key) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(key.getBytes());
    }
}
