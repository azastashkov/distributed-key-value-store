package com.dkvs.api;

import com.dkvs.cluster.ClusterService;
import com.dkvs.cluster.Node;
import com.dkvs.model.VersionedValue;
import com.dkvs.storage.StorageEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/internal")
@RequiredArgsConstructor
public class InternalController {

    private final StorageEngine storageEngine;
    private final ClusterService clusterService;

    @PutMapping("/data/{encodedKey}")
    public ResponseEntity<Void> put(@PathVariable String encodedKey,
                                    @RequestBody byte[] data,
                                    @RequestHeader("X-DKVS-Version") long version,
                                    @RequestHeader("X-DKVS-Timestamp") long timestamp) {
        try {
            String key = decodeKey(encodedKey);
            VersionedValue value = new VersionedValue(data, version, timestamp);

            // Only write if this version is newer than what we have
            VersionedValue existing = storageEngine.get(key);
            if (existing == null || value.version() > existing.version()) {
                storageEngine.put(key, value);
            }

            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Internal PUT failed for key {}: {}", encodedKey, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/data/{encodedKey}")
    public ResponseEntity<byte[]> get(@PathVariable String encodedKey) {
        try {
            String key = decodeKey(encodedKey);
            VersionedValue value = storageEngine.get(key);
            if (value == null) {
                return ResponseEntity.notFound().build();
            }
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-DKVS-Version", String.valueOf(value.version()));
            headers.set("X-DKVS-Timestamp", String.valueOf(value.timestamp()));
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(value.data());
        } catch (Exception e) {
            log.error("Internal GET failed for key {}: {}", encodedKey, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/gossip")
    public ResponseEntity<Void> receiveGossip(@RequestBody List<Node> state) {
        clusterService.mergeGossipState(state);
        return ResponseEntity.ok().build();
    }

    private String decodeKey(String encodedKey) {
        return new String(Base64.getUrlDecoder().decode(encodedKey));
    }
}
