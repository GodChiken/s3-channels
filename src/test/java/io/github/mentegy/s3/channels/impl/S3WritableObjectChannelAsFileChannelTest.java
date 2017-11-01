package io.github.mentegy.s3.channels.impl;

import io.github.mentegy.s3.channels.S3WritableObjectChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@Tag("fast")
class S3WritableObjectChannelAsFileChannelTest {
    private S3WritableObjectChannel mocked = mock(S3WritableObjectChannel.class);
    private S3WritableObjectChannelAsFileChannel channel = new S3WritableObjectChannelAsFileChannel(mocked);
    private ByteBuffer buffer = ByteBuffer.allocate(10);

    @BeforeEach
    void prepare() {
        reset(mocked);
        buffer.rewind();
    }

    @Test
    void writeTest() {
        channel.write(buffer);
        verify(mocked, times(1)).write(buffer);
    }


    @Test
    void writeTest2() {
        channel.write(buffer, 10);
        verify(mocked, times(1)).write(buffer, 10);
    }

    @Test
    void writeTest3() {
        ByteBuffer[] bufs = new ByteBuffer[]{buffer};
        channel.write(bufs, 0, 1);
        verify(mocked, times(1)).write(bufs, 0, 1);
    }


    @Test
    void positionTest() {
        channel.position();
        verify(mocked, times(1)).position();
    }


    @Test
    void positionSetTest() {
        channel.position(12);
        verify(mocked, times(1)).position(12);
    }


    @Test
    void sizeTest() {
        channel.size();
        verify(mocked, times(1)).size();
    }


    @Test
    void truncateTest() {
        channel.truncate(10);
        verify(mocked, times(1)).truncate(10);
    }


    @Test
    void forceTest() {
        channel.force(true);
        channel.force(false);
    }

    @Test
    void implCloseChannelTest() throws IOException {
        channel.close();
        verify(mocked, times(1)).close();
    }

    @Test
    void cancelTest() {
        channel.cancel();
        verify(mocked, times(1)).cancel();
    }

    @Test
    void getCancellationTest() {
        channel.getCancellation();
        verify(mocked, times(1)).getCancellation();
    }


    @Test
    void unsupportedTest() {
        assertThrows(UnsupportedOperationException.class, () -> channel.transferFrom(null, 0, 0));
        assertThrows(UnsupportedOperationException.class, () -> channel.transferTo(0, 0, null));
        assertThrows(UnsupportedOperationException.class, () -> channel.map(null, 0, 0));
        assertThrows(UnsupportedOperationException.class, () -> channel.lock(0, 0, true));
        assertThrows(UnsupportedOperationException.class, () -> channel.tryLock(0, 0, false));
        assertThrows(UnsupportedOperationException.class, () -> channel.read(buffer));
        assertThrows(UnsupportedOperationException.class, () -> channel.read(buffer, 0));
        assertThrows(UnsupportedOperationException.class, () -> channel.read(null, 0, 0));
    }
}
