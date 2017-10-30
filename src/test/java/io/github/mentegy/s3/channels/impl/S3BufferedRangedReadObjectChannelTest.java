package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.S3ReadableObjectChannel;
import io.github.mentegy.s3.channels.testutils.AbstractS3Suite;
import io.github.mentegy.s3.channels.testutils.FileGenerator;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("s3")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3BufferedRangedReadObjectChannelTest extends AbstractS3Suite {
    final String key = "S3BufferedRangedReadObjectChannelTest";
    FileGenerator.TempFile file;
    FileChannel fc;
    S3BufferedRangedReadObjectChannel s3Channel;
    AmazonS3 amazonS3 = spy(defaultAmazonS3());


    @BeforeAll
    void beforeAll() throws IOException {
        file = FileGenerator.randomTempFile(1024);
        fc = FileChannel.open(file.path, StandardOpenOption.READ);
        defaultAmazonS3().putObject(testBucket, key, file.path.toFile());
        s3Channel = (S3BufferedRangedReadObjectChannel) S3ReadableObjectChannel.builder()
                .amazonS3(amazonS3)
                .bucket(testBucket)
                .key(key)
                .buffered(50)
                .build();
        assertEquals(s3Channel.size(), 1024);
    }

    @AfterAll
    void afterAll() throws IOException {
        fc.close();
        Files.delete(file.path);
        assertTrue(s3Channel.isOpen());
        s3Channel.close();
        assertFalse(s3Channel.isOpen());
    }


    @Test
    void testAll() throws IOException {
        ByteBuffer bf1 = ByteBuffer.allocate(25);
        ByteBuffer bf2 = ByteBuffer.allocate(25);
        Runnable resetFields = () -> {
            bf1.rewind();
            bf2.rewind();
            reset(amazonS3);
        };

        // we cache 100-150 zone (see .buffered(50) in builder)
        assertEquals(
                fc.read(bf1, 100),
                s3Channel.read(bf2, 100));
        assertArrayEquals(bf1.array(), bf2.array());

        verify(amazonS3, times(1)).getObject(any());
        resetFields.run();

        // we still have data in channel buffer, so no new s3 calls
        assertEquals(
                fc.read(bf1, 125),
                s3Channel.read(bf2, 125));
        assertArrayEquals(bf1.array(), bf2.array());

        // no s3 call
        verify(amazonS3, times(0)).getObject(any());
        resetFields.run();


        // 5 bytes goes from cached buffer
        // next 45 from s3, but we download 50 since it's configured cache buffer size
        assertEquals(
                fc.read(bf1, 145),
                s3Channel.read(bf2, 145));
        assertArrayEquals(bf1.array(), bf2.array());

        verify(amazonS3, times(1)).getObject(any());
        resetFields.run();

        // 150..200 should be covered by buffer
        // Let's test this

        assertEquals(
                fc.read(bf1, 150),
                s3Channel.read(bf2, 150));
        assertArrayEquals(bf1.array(), bf2.array());

        bf1.rewind();
        bf2.rewind();

        assertEquals(
                fc.read(bf1, 175),
                s3Channel.read(bf2, 175));
        assertArrayEquals(bf1.array(), bf2.array());

        // must be no calls
        verify(amazonS3, times(0)).getObject(any());
    }

    @Test
    void testDontCacheIfInptuBufferIsGreaterThanBuffer() throws IOException {
        s3Channel.buffer.rewind();
        ByteBuffer dup = ByteBuffer.allocate(50);
        dup.put(s3Channel.buffer);

        ByteBuffer bf1 = ByteBuffer.allocate(150);
        ByteBuffer bf2 = ByteBuffer.allocate(150);
        assertEquals(
                fc.read(bf1),
                s3Channel.read(bf2));
        assertArrayEquals(dup.array(), s3Channel.buffer.array());
    }

    @Test
    void testHasNoRemaining() throws IOException{
        ByteBuffer b = ByteBuffer.allocate(25);
        b.position(25);
        assertEquals(0, s3Channel.read(b));
    }
}
