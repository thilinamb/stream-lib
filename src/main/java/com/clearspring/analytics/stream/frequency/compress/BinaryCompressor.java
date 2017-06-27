package com.clearspring.analytics.stream.frequency.compress;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Thilina Buddhika
 */
public class BinaryCompressor implements Compressor {

    private int unCompressedSize;
    private byte[] compressedData;

    @Override
    public void deflate(long[][] data, int nonZeroCount) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.SIZE * data.length * data[0].length);
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[0].length; j++) {
                byteBuffer.putLong(data[i][j]);
            }
        }
        byteBuffer.flip();
        byte[] bytes = byteBuffer.array();
        this.unCompressedSize = bytes.length;
        ByteArrayOutputStream baos = null;
        DataOutputStream dos = null;
        GZIPOutputStream gzipOutputStream = null;
        try {
            baos = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos);
            gzipOutputStream = new GZIPOutputStream(dos);
            gzipOutputStream.write(bytes);
            gzipOutputStream.finish();
            gzipOutputStream.flush();
            dos.flush();
            baos.flush();
            this.compressedData = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (gzipOutputStream != null) {
                    gzipOutputStream.close();
                }
                if (dos != null) {
                    dos.close();
                }
                if (baos != null) {
                    baos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public long[][] inflate(int depth, int width) {
        long[][] sketch = new long[depth][width];
        GZIPInputStream gzipInputStream = null;
        ByteArrayInputStream bais = null;
        DataInputStream dis = null;
        try {
            bais = new ByteArrayInputStream(compressedData);
            dis = new DataInputStream(bais);
            gzipInputStream = new GZIPInputStream(dis);
            byte[] uncompressedData = new byte[this.unCompressedSize];
            int read = 0;
            while(read < this.unCompressedSize) {
                read += gzipInputStream.read(uncompressedData, read, uncompressedData.length - read);
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(uncompressedData.length);
            byteBuffer.put(uncompressedData);
            byteBuffer.flip();
            for (int i = 0; i < depth; i++) {
                for (int j = 0; j < width; j++) {
                    sketch[i][j] = byteBuffer.getLong();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bais != null) {
                    bais.close();
                }
                if (dis != null) {
                    dis.close();
                }
                if (gzipInputStream != null) {
                    gzipInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sketch;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(this.unCompressedSize);
        dos.writeInt(compressedData.length);
        dos.write(compressedData);
    }

    @Override
    public void deserialize(DataInputStream dis) throws IOException {
        this.unCompressedSize = dis.readInt();
        this.compressedData = new byte[dis.readInt()];
        dis.readFully(this.compressedData);
    }
}
