package com.clearspring.analytics.stream.frequency;

import com.clearspring.analytics.stream.frequency.compress.CompressedSparseRowCompressor;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * @author Thilina Buddhika
 */
public class CSRCompressionTest {
    @Test
    public void testInflationDeflation() {
        int depth = 5;
        int width = 2000;
        long[][] original = new long[depth][width];
        Random rnd = new Random();
        int nonZeroElems = 0;
        for (int i = 0; i < depth; i++) {
            for (int j = 0; j < width; j++) {
                if (rnd.nextDouble() < 0.01) {
                    original[i][j] = rnd.nextInt(1000);
                    nonZeroElems++;
                }
            }
        }
        System.out.println("Total number of non-zero elements: " + nonZeroElems);

        CompressedSparseRowCompressor compressor = new CompressedSparseRowCompressor();
        compressor.deflate(original, nonZeroElems);

        long[][] inflated = compressor.inflate(depth, width);
        assertTrue("Original array is not equal to the inflated array", arrCompare(original, inflated));
    }

    private boolean arrCompare(long[][] arr1, long[][] arr2) {
        if (arr1.length != arr2.length) {
            return false;
        }
        if (arr1[0].length != arr2[0].length) {
            return false;
        }
        for (int i = 0; i < arr1.length; i++) {
            for (int j = 0; j < arr1[0].length; j++) {
                if (arr1[i][j] != arr2[i][j]) {
                    return false;
                }
            }
        }
        return true;
    }
}
