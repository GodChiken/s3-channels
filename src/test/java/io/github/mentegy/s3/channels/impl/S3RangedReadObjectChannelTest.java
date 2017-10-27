package io.github.mentegy.s3.channels.impl;

import io.github.mentegy.s3.channels.testutils.AbstractS3Suite;
import io.github.mentegy.s3.channels.testutils.FileGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

@Tag("s3")
class S3RangedReadObjectChannelTest extends AbstractS3Suite {
    FileGenerator.TempFile file;
    FileChannel fc;
    S3RangedReadObjectChannel s3Channel;
    final String key = "S3RangedReadObjectChannelTest";

    @BeforeAll
    void beforeAll() throws IOException {
        file = FileGenerator.randomTempFile(1024);
        fc =  FileChannel.open(file.path, StandardOpenOption.READ);
        defaultAmazonS3().putObject(testBucket, key, file.path.toFile());
        s3Channel = new S3RangedReadObjectChannel(key, testBucket, defaultAmazonS3());
    }

    @AfterAll
    void afterAll() throws IOException {
        fc.close();
        Files.delete(file.path);
        assertTrue(s3Channel.isOpen()); // always true
        s3Channel.close(); // no effect
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
    }
}
