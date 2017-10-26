package io.github.mentegy.s3.channels.util;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

@Tag("fast")
class ByteBufferUtilsTest {

    @Test
    void testPutBiggerBuffer() {
        ByteBuffer bf1 = ByteBuffer.allocate(4);
        ByteBuffer bf2 = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        bf2.limit(9);

        assertEquals(4, bf1.remaining());
        assertEquals(9, bf2.remaining());

        int written = ByteBufferUtils.putBiggerBuffer(bf1, bf2);
        assertEquals(4, written);

        assertFalse(bf1.hasRemaining());
        assertEquals(5, bf2.remaining());
        assertEquals(4, bf2.position());

        assertArrayEquals(new byte[]{1, 2, 3, 4}, bf1.array());

        // make cov happy
        new ByteBufferUtils();
    }
}
