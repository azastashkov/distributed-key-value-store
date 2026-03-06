package org.dkvs.cluster;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Node {

    public enum Status { UP, DOWN }

    private String id;
    private String host;
    private int port;
    private long heartbeat;
    private Status status;
    private long lastUpdated;

    public String address() {
        return host + ":" + port;
    }

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }
}
