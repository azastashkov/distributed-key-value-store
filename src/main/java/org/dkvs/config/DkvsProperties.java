package org.dkvs.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "dkvs")
public class DkvsProperties {

    private String nodeId = "node-1";
    private String host = "localhost";
    private int port = 8080;
    private String seeds = "localhost:8080";
    private int replicationFactor = 3;
    private int writeQuorum = 2;
    private int readQuorum = 2;
    private String dataDir = "./data";
    private int memtableFlushThreshold = 1_048_576; // 1 MB
    private long gossipIntervalMs = 1000;
    private int virtualNodes = 150;
    private long nodeDownTimeoutMs = 5000;
}
