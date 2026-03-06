package org.dkvs.model;

import java.io.Serializable;
import java.util.Arrays;

public record VersionedValue(byte[] data, long version, long timestamp) implements Serializable {

    public VersionedValue {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VersionedValue that)) return false;
        return version == that.version
                && timestamp == that.timestamp
                && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(version);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "VersionedValue{dataLen=" + data.length + ", version=" + version + ", timestamp=" + timestamp + "}";
    }
}
