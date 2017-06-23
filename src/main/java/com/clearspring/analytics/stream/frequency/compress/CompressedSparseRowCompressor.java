package com.clearspring.analytics.stream.frequency.compress;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class CompressedSparseRowCompressor implements Compressor {

    private long[] arrA;
    private int[] arrIA;
    private int[] arrJA;

    @Override
    public void deflate(long[][] data, int nonZeroCount) {
        int depth = data.length;
        int width = data[0].length;

        // initialize data structures
        this.arrA = new long[nonZeroCount];
        this.arrIA = new int[depth + 1];
        this.arrJA = new int[nonZeroCount];

        // populate data structures
        this.arrIA[0] = 0;
        int arrAIndex = 0;
        for (int i = 0; i < depth; i++) {
            int nonZeroElemsPerRow = 0;
            for (int j = 0; j < width; j++) {
                if (data[i][j] != 0) {
                    this.arrA[arrAIndex] = data[i][j];
                    this.arrJA[arrAIndex] = j;
                    arrAIndex++;
                    nonZeroElemsPerRow++;
                }
            }
            this.arrIA[i + 1] = this.arrIA[i] + nonZeroElemsPerRow;
        }
    }

    @Override
    public long[][] inflate(int depth, int width) {
        long[][] data = new long[depth][width];
        int arrAIndex = 0;
        for (int i = 0; i < depth; i++) {
            int nonZeroElemsPerRow = this.arrIA[i + 1] - this.arrIA[i];
            for (int j = arrAIndex; j < arrAIndex + nonZeroElemsPerRow; j++) {
                data[i][this.arrJA[j]] = arrA[j];
            }
            arrAIndex += nonZeroElemsPerRow;
        }
        return data;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(this.arrA.length);
        for (long i : this.arrA) {
            dos.writeLong(i);
        }
        dos.writeInt(this.arrIA.length);
        for (int i : this.arrIA) {
            dos.writeInt(i);
        }
        for (int i : this.arrJA) {
            dos.writeInt(i);
        }
    }

    @Override
    public void deserialize(DataInputStream dis) throws IOException {
        int nonZeroElemCount = dis.readInt();
        this.arrA = new long[nonZeroElemCount];
        for (int i = 0; i < nonZeroElemCount; i++) {
            this.arrA[i] = dis.readLong();
        }
        this.arrIA = new int[dis.readInt()];
        for(int i = 0; i < this.arrIA.length; i++){
            this.arrIA[i] = dis.readInt();
        }
        this.arrJA = new int[nonZeroElemCount];
        for(int i = 0; i < nonZeroElemCount; i++){
            this.arrJA[i] = dis.readInt();
        }
    }

}
