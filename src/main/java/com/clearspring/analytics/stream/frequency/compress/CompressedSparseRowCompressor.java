package com.clearspring.analytics.stream.frequency.compress;

import java.io.DataInputStream;
import java.io.DataOutputStream;

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
        /*printArr(data);
        System.out.println("A");;
        printLongArr(arrA);
        System.out.println("IA");
        printIntArr(arrIA);
        System.out.println("JA");
        printIntArr(arrJA);
        */
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
    public void serialize(DataOutputStream dos) {

    }

    @Override
    public void deserialize(DataInputStream dis) {

    }
}
