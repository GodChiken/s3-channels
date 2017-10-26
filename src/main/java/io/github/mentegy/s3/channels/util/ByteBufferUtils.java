package io.github.mentegy.s3.channels.util;

import java.nio.ByteBuffer;

public final class ByteBufferUtils {

    public static int putBiggerBuffer(ByteBuffer dest, ByteBuffer src) {
        int srcLimit = src.limit();
        int written = dest.remaining();
        src.limit(written);
        dest.put(src);
        src.limit(srcLimit);
        return written;
    }
}
