package io.github.mentegy.s3.channels.util;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import static io.github.mentegy.s3.channels.util.ByteBufferUtils.*;
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

        int written = putBiggerBuffer(bf1, bf2);
        assertEquals(4, written);

        assertFalse(bf1.hasRemaining());
        assertEquals(5, bf2.remaining());
        assertEquals(4, bf2.position());

        assertArrayEquals(new byte[]{1, 2, 3, 4}, bf1.array());

        // make cov happy
        new ByteBufferUtils();
    }

    @Test
    void testPutBiggerBufferButNotActuallyBigger() {
        ByteBuffer bf1 = ByteBuffer.allocate(4);
        ByteBuffer bf2 =  ByteBuffer.allocate(2);
        assertThrows(IllegalArgumentException.class, () -> ByteBufferUtils.putBiggerBuffer(bf1, bf2));
    }

    @Test
    void testReadFromInputStream() throws IOException {
        InputStream is1 = new InputStream() {
            @Override
            public int read() throws IOException {
                return 1;
            }
        };
        assertEquals(_16_KB + 250, readFromInputStream(is1, ByteBuffer.allocate(_16_KB + 250)));

        InputStream is2 = new InputStream() {
            int todo = 50;
            @Override
            public int read() throws IOException {
                return todo > 0 ? todo-- : -1;
            }
        };
        assertEquals(50, readFromInputStream(is2, ByteBuffer.allocate(100)));

    }
}
