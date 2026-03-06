package org.dkvs.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;

public class BloomFilter {

    private final BitSet bitSet;
    private final int bitSetSize;
    private final int numHashFunctions;

    public BloomFilter(int expectedElements, double falsePositiveRate) {
        this.bitSetSize = optimalBitSetSize(expectedElements, falsePositiveRate);
        this.numHashFunctions = optimalNumHashFunctions(expectedElements, bitSetSize);
        this.bitSet = new BitSet(bitSetSize);
    }

    private BloomFilter(BitSet bitSet, int bitSetSize, int numHashFunctions) {
        this.bitSet = bitSet;
        this.bitSetSize = bitSetSize;
        this.numHashFunctions = numHashFunctions;
    }

    public void add(String key) {
        int[] hashes = getHashes(key);
        for (int hash : hashes) {
            bitSet.set(Math.abs(hash % bitSetSize));
        }
    }

    public boolean mightContain(String key) {
        int[] hashes = getHashes(key);
        for (int hash : hashes) {
            if (!bitSet.get(Math.abs(hash % bitSetSize))) {
                return false;
            }
        }
        return true;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(bitSetSize);
        out.writeInt(numHashFunctions);
        byte[] bytes = bitSet.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static BloomFilter readFrom(DataInputStream in) throws IOException {
        int bitSetSize = in.readInt();
        int numHashFunctions = in.readInt();
        int byteLen = in.readInt();
        byte[] bytes = new byte[byteLen];
        in.readFully(bytes);
        BitSet bitSet = BitSet.valueOf(bytes);
        return new BloomFilter(bitSet, bitSetSize, numHashFunctions);
    }

    private int[] getHashes(String key) {
        int[] result = new int[numHashFunctions];
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(keyBytes);

            // Use pairs of bytes from the digest to generate hash values
            // Kirsch-Mitzenmacher technique: h(i) = h1 + i*h2
            int h1 = ((digest[0] & 0xFF) << 24) | ((digest[1] & 0xFF) << 16)
                    | ((digest[2] & 0xFF) << 8) | (digest[3] & 0xFF);
            int h2 = ((digest[4] & 0xFF) << 24) | ((digest[5] & 0xFF) << 16)
                    | ((digest[6] & 0xFF) << 8) | (digest[7] & 0xFF);

            for (int i = 0; i < numHashFunctions; i++) {
                result[i] = h1 + i * h2;
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
        return result;
    }

    private static int optimalBitSetSize(int n, double p) {
        return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalNumHashFunctions(int n, int m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }
}
