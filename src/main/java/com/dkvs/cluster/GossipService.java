package com.dkvs.cluster;

import com.dkvs.config.DkvsProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
public class GossipService {

    private final ClusterService clusterService;
    private final DkvsProperties properties;
    private final RestClient restClient;

    public GossipService(ClusterService clusterService, DkvsProperties properties) {
        this.clusterService = clusterService;
        this.properties = properties;
        this.restClient = RestClient.builder()
                .defaultHeader("Content-Type", "application/json")
                .build();
    }

    @Scheduled(fixedDelayString = "${dkvs.gossip-interval-ms:1000}")
    public void gossip() {
        clusterService.incrementHeartbeat();
        clusterService.detectFailures();

        List<Node> targets = selectGossipTargets();
        if (targets.isEmpty()) {
            return;
        }

        List<Node> stateToSend = new ArrayList<>(clusterService.getAllNodes());

        for (Node target : targets) {
            sendGossip(target, stateToSend);
        }
    }

    private List<Node> selectGossipTargets() {
        Collection<Node> allNodes = clusterService.getAllNodes();
        String selfId = properties.getNodeId();

        // Collect peers (UP or DOWN, excluding self)
        List<Node> peers = allNodes.stream()
                .filter(n -> !n.getId().equals(selfId))
                .toList();

        if (!peers.isEmpty()) {
            // Pick one random peer
            Node target = peers.get(ThreadLocalRandom.current().nextInt(peers.size()));
            return List.of(target);
        }

        // No peers known yet - try seed addresses
        List<String> seeds = clusterService.getSeedAddresses();
        String selfAddress = properties.getHost() + ":" + properties.getPort();

        for (String seed : seeds) {
            if (seed.equals(selfAddress)) continue;
            String[] parts = seed.split(":");
            if (parts.length == 2) {
                Node seedNode = new Node();
                seedNode.setId("seed-" + seed);
                seedNode.setHost(parts[0]);
                seedNode.setPort(Integer.parseInt(parts[1]));
                seedNode.setStatus(Node.Status.UP);
                return List.of(seedNode);
            }
        }

        return List.of();
    }

    private void sendGossip(Node target, List<Node> state) {
        try {
            String url = target.baseUrl() + "/internal/gossip";
            restClient.post()
                    .uri(url)
                    .body(state)
                    .retrieve()
                    .toBodilessEntity();
            log.debug("Gossip sent to {}", target.address());
        } catch (Exception e) {
            log.debug("Gossip to {} failed: {}", target.address(), e.getMessage());
        }
    }
}
