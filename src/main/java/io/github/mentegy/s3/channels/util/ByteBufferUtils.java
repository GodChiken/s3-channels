package io.github.mentegy.s3.channels.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public final class ByteBufferUtils {

    public final static int _16_KB = 16 * 1024;

    /**
     * Writes bigger source buffer into destination since regular Java ByteBuffer API does not alow this.
     *
     * @param dest destination buffer
     * @param src  source buffer
     * @return amount of written bytes into destination
     */
    public static int putBiggerBuffer(ByteBuffer dest, ByteBuffer src) {
        int written = dest.remaining();
        if (dest.remaining() >= src.remaining()) {
            dest.put(src);
            return written - dest.remaining();
        }
        int srcLimit = src.limit();
        src.limit(written);
        dest.put(src);
        src.limit(srcLimit);
        return written;
    }

    /**
     * Reads data from given input stream into byte buffer.
     * This helper does not overflow destination byte buffer.
     * Bytes are read until input stream have those or until byte buffer has remaining space for them.
     *
     * @param src         source input stream
     * @param dest        destination byte buffer
     * @param closeStream whereas to close src input stream
     * @return number of read (from input stream) / written (to buffer) bytes
     * @throws IOException - if any error occurs during input stream read
     */
    public static int readFromInputStream(InputStream src, ByteBuffer dest, boolean closeStream) throws IOException {
        try {
            int chunkSize = dest.remaining() > _16_KB ? _16_KB : dest.remaining();

            byte[] chunk = new byte[chunkSize];
            int read;

            int written = 0;

            while ((read = src.read(chunk)) != -1) {
                if (!dest.hasRemaining()) {
                    return written;
                }
                if (read > dest.remaining()) {
                    read = dest.remaining();
                }
                written += read;
                dest.put(chunk, 0, read);
            }

            return written;
        } finally {
            if (closeStream) {
                src.close();
            }
        }
    }

    public static int readFromInputStream(InputStream src, ByteBuffer dest) throws IOException {
        return readFromInputStream(src, dest, false);
    }
}
