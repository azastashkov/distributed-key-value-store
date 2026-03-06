package org.dkvs.storage;

import org.dkvs.model.VersionedValue;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

/**
 * Immutable sorted string table stored on disk.
 * Format:
 *   [DATA SECTION]
 *     For each entry: keyLen(int) | keyBytes | dataLen(int) | dataBytes | version(long) | timestamp(long)
 *   [INDEX SECTION]
 *     entryCount(int)
 *     For each entry: keyLen(int) | keyBytes | offset(long)
 *   [BLOOM FILTER SECTION]
 *     BloomFilter serialized
 *   [FOOTER]
 *     indexOffset(long) | bloomOffset(long) | magic(int = 0xDEADBEEF)
 */
@Slf4j
public class SSTable {

    private static final int MAGIC = 0xDEADBEEF;

    @Getter
    private final Path filePath;
    private final BloomFilter bloomFilter;
    private final List<IndexEntry> index;

    private record IndexEntry(String key, long offset) {}

    private SSTable(Path filePath, BloomFilter bloomFilter, List<IndexEntry> index) {
        this.filePath = filePath;
        this.bloomFilter = bloomFilter;
        this.index = index;
    }

    /**
     * Writes a sorted map of entries to a new SSTable file.
     */
    public static SSTable write(Path filePath, Map<String, VersionedValue> sortedData) throws IOException {
        BloomFilter bloom = new BloomFilter(Math.max(sortedData.size(), 1), 0.01);
        List<IndexEntry> index = new ArrayList<>();

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(filePath.toFile())))) {

            long offset = 0;

            // Write data section
            for (Map.Entry<String, VersionedValue> entry : sortedData.entrySet()) {
                String key = entry.getKey();
                VersionedValue val = entry.getValue();
                bloom.add(key);
                index.add(new IndexEntry(key, offset));

                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeInt(val.data().length);
                out.write(val.data());
                out.writeLong(val.version());
                out.writeLong(val.timestamp());

                offset += 4 + keyBytes.length + 4 + val.data().length + 8 + 8;
            }

            // Write index section
            long indexOffset = offset;
            out.writeInt(index.size());
            for (IndexEntry ie : index) {
                byte[] kb = ie.key.getBytes(StandardCharsets.UTF_8);
                out.writeInt(kb.length);
                out.write(kb);
                out.writeLong(ie.offset);
            }

            // Write bloom filter section
            long bloomStart = indexOffset;
            // Calculate index section size to find bloom offset
            ByteArrayOutputStream indexBuf = new ByteArrayOutputStream();
            DataOutputStream indexCalc = new DataOutputStream(indexBuf);
            indexCalc.writeInt(index.size());
            for (IndexEntry ie : index) {
                byte[] kb = ie.key.getBytes(StandardCharsets.UTF_8);
                indexCalc.writeInt(kb.length);
                indexCalc.write(kb);
                indexCalc.writeLong(ie.offset);
            }
            long bloomOffset = indexOffset + indexBuf.size();

            bloom.writeTo(out);

            // Calculate bloom section size
            ByteArrayOutputStream bloomBuf = new ByteArrayOutputStream();
            bloom.writeTo(new DataOutputStream(bloomBuf));

            // Write footer
            out.writeLong(indexOffset);
            out.writeLong(bloomOffset);
            out.writeInt(MAGIC);

            out.flush();
        }

        return new SSTable(filePath, bloom, index);
    }

    /**
     * Opens an existing SSTable file, reading its index and bloom filter.
     */
    public static SSTable open(Path filePath) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            long fileLen = raf.length();
            // Read footer (indexOffset:8 + bloomOffset:8 + magic:4 = 20 bytes)
            raf.seek(fileLen - 20);
            long indexOffset = raf.readLong();
            long bloomOffset = raf.readLong();
            int magic = raf.readInt();
            if (magic != MAGIC) {
                throw new IOException("Invalid SSTable file: bad magic number");
            }

            // Read index
            raf.seek(indexOffset);
            DataInputStream indexIn = new DataInputStream(new BufferedInputStream(
                    new FileInputStream(filePath.toFile())));
            indexIn.skipNBytes(indexOffset);

            int entryCount = indexIn.readInt();
            List<IndexEntry> index = new ArrayList<>(entryCount);
            for (int i = 0; i < entryCount; i++) {
                int keyLen = indexIn.readInt();
                byte[] keyBytes = new byte[keyLen];
                indexIn.readFully(keyBytes);
                long offset = indexIn.readLong();
                index.add(new IndexEntry(new String(keyBytes, StandardCharsets.UTF_8), offset));
            }
            indexIn.close();

            // Read bloom filter
            DataInputStream bloomIn = new DataInputStream(new BufferedInputStream(
                    new FileInputStream(filePath.toFile())));
            bloomIn.skipNBytes(bloomOffset);
            BloomFilter bloom = BloomFilter.readFrom(bloomIn);
            bloomIn.close();

            return new SSTable(filePath, bloom, index);
        }
    }

    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    /**
     * Looks up a key using binary search on the index, then reads the data from disk.
     */
    public VersionedValue get(String key) throws IOException {
        // Binary search in the index
        int lo = 0, hi = index.size() - 1;
        long targetOffset = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = index.get(mid).key.compareTo(key);
            if (cmp < 0) {
                lo = mid + 1;
            } else if (cmp > 0) {
                hi = mid - 1;
            } else {
                targetOffset = index.get(mid).offset;
                break;
            }
        }

        if (targetOffset < 0) {
            return null;
        }

        // Read the entry at the offset
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            raf.seek(targetOffset);
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            int dataLen = raf.readInt();
            byte[] data = new byte[dataLen];
            raf.readFully(data);
            long version = raf.readLong();
            long timestamp = raf.readLong();
            return new VersionedValue(data, version, timestamp);
        }
    }

    public int entryCount() {
        return index.size();
    }
}
