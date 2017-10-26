package io.github.mentegy.s3.channels;

import io.github.mentegy.s3.channels.impl.S3MPUFileChannel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("fast")
class S3MultiPartUploadFileChannelTest {


    private S3MultiPartUploadFileChannel channel =
            new S3MPUFileChannel("", "", "", 123, null, null,true);

    @Test
    void testConstructor() throws Exception {
        assertNotEquals(123, channel.partSize);
        assertEquals(S3MultiPartUploadFileChannel.MIN_PART_SIZE, channel.partSize);
    }


    @Test
    void testUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> channel.read(ByteBuffer.allocate(0)));
        assertThrows(UnsupportedOperationException.class, () -> channel.read(null, 1, 2));
        assertThrows(UnsupportedOperationException.class, () -> channel.read(null, 1));
        assertThrows(UnsupportedOperationException.class, () -> channel.write(null, 1, 2));
        assertThrows(UnsupportedOperationException.class, () -> channel.truncate(0));
        assertThrows(UnsupportedOperationException.class, () -> channel.force(true));
        assertThrows(UnsupportedOperationException.class, () -> channel.transferTo(1, 2, null));
        assertThrows(UnsupportedOperationException.class, () -> channel.transferFrom(null, 1 ,2));
        assertThrows(UnsupportedOperationException.class, () -> channel.map(null, 1 ,2));
        assertThrows(UnsupportedOperationException.class, () -> channel.lock(1, 1 ,true));
        assertThrows(UnsupportedOperationException.class, () -> channel.tryLock(1, 1 ,true));
    }
}
