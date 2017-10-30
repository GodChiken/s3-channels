package io.github.mentegy.s3.channels.impl;

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

@Tag("s3")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3RangedReadObjectChannelTest extends AbstractS3Suite {
    final String key = "S3RangedReadObjectChannelTest";
    FileGenerator.TempFile file;
    FileChannel fc;
    S3ReadableObjectChannel s3Channel;

    @BeforeAll
    void beforeAll() throws IOException {
        file = FileGenerator.randomTempFile(1024);
        fc = FileChannel.open(file.path, StandardOpenOption.READ);
        defaultAmazonS3().putObject(testBucket, key, file.path.toFile());
        s3Channel = S3ReadableObjectChannel.builder()
                .amazonS3(defaultAmazonS3())
                .bucket(testBucket)
                .key(key)
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
    void testAbsoluteRead() throws IOException {
        ByteBuffer bf1 = ByteBuffer.allocate(50);
        ByteBuffer bf2 = ByteBuffer.allocate(50);

        assertEquals(fc.read(bf1, 65), s3Channel.read(bf2, 65));
        assertArrayEquals(bf1.array(), bf2.array());

        bf1.rewind().position(5).limit(25);
        bf2.rewind().position(5).limit(25);

        assertEquals(fc.read(bf1, 500), s3Channel.read(bf2, 500));
        assertArrayEquals(bf1.array(), bf2.array());
    }

    @Test
    void testRelativeRead() throws IOException {
        ByteBuffer bf1 = ByteBuffer.allocate(50);
        ByteBuffer bf2 = ByteBuffer.allocate(50);
        fc.position(25);
        s3Channel.position(25);

        assertEquals(fc.read(bf1), s3Channel.read(bf2));
        assertArrayEquals(bf1.array(), bf2.array());

        bf1.rewind().position(5).limit(25);
        bf2.rewind().position(5).limit(25);

        fc.position(678);
        s3Channel.position(678);

        assertEquals(fc.read(bf1), s3Channel.read(bf2));
        assertArrayEquals(bf1.array(), bf2.array());

        assertEquals(fc.position(), s3Channel.position());
    }

    @Test
    void testHasNoRemaining() throws IOException{
        ByteBuffer b = ByteBuffer.allocate(25);
        b.position(25);
        assertEquals(0, s3Channel.read(b));
    }

    @Test
    void testUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> s3Channel.truncate(1));
        assertThrows(UnsupportedOperationException.class, () -> s3Channel.write(null));
    }

    @Test
    void testPosition() {
        assertEquals(s3Channel.position(), s3Channel.position(-1).position());
        assertEquals(s3Channel.position(), s3Channel.position(s3Channel.size() + 123).position());
    }
}
