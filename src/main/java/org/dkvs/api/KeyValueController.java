package org.dkvs.api;

import org.dkvs.coordinator.CoordinatorService;
import org.dkvs.model.VersionedValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;

@Slf4j
@RestController
@RequestMapping("/api/data")
@RequiredArgsConstructor
public class KeyValueController {

    private final CoordinatorService coordinatorService;

    @PutMapping("/{encodedKey}")
    public ResponseEntity<Void> put(@PathVariable String encodedKey,
                                    @RequestBody byte[] value) {
        try {
            String key = decodeKey(encodedKey);
            coordinatorService.put(key, value);
            return ResponseEntity.ok().build();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("PUT failed for key {}: {}", encodedKey, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/{encodedKey}")
    public ResponseEntity<byte[]> get(@PathVariable String encodedKey) {
        try {
            String key = decodeKey(encodedKey);
            VersionedValue value = coordinatorService.get(key);
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
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("GET failed for key {}: {}", encodedKey, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    private String decodeKey(String encodedKey) {
        return new String(Base64.getUrlDecoder().decode(encodedKey));
    }
}
