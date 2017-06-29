package com.clearspring.analytics.stream.frequency.compress;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public interface Compressor {
    public void deflate(long[][] data, int nonZeroCount);

    public long[][] inflate(int depth, int width);

    public void serialize(DataOutputStream dos) throws IOException;

    public void deserialize(DataInputStream dis) throws IOException;

    public int getType();

    public int getSize();
}
