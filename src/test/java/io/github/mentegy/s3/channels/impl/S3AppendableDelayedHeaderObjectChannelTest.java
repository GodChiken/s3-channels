package io.github.mentegy.s3.channels.impl;

import io.github.mentegy.s3.channels.S3WritableObjectChannel;
import io.github.mentegy.s3.channels.testutils.FileGenerator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static io.github.mentegy.s3.channels.S3WritableObjectChannel.MIN_PART_SIZE;
import static org.junit.jupiter.api.Assertions.*;

@Tag("s3")
class S3AppendableDelayedHeaderObjectChannelTest extends AbstractS3WritableObjectChannelSuite<S3AppendableDelayedHeaderObjectChannel> {

    private void initChannel(String fileKey) {
        this.key = "S3AppendableDelayedHeaderObjectChannelTest/" + fileKey;

        // header + 3 parts of 5mb each + 2048 bytes for last part
        file = FileGenerator.randomTempFile(headerSize + MIN_PART_SIZE * 3 + 2048);

        s3channel = (S3AppendableDelayedHeaderObjectChannel) defaultBuilder(initMultiPart().getUploadId())
                .delayedHeader(true)
                .build();
    }

    @Test
    void testAll() throws Exception {
        initChannel("testAll");

        assertTrue(s3channel.hasDelayedHeader());
        assertEquals(S3WritableObjectChannel.MIN_PART_SIZE, s3channel.headerSize());

        final FileChannel fc = FileChannel.open(file.path, StandardOpenOption.READ);
        final ByteBuffer chunk = ByteBuffer.allocate(chunkSize);

        fc.position(headerSize);
        s3channel.position(headerSize);
        assertEquals(headerSize, s3channel.position());
        assertEquals(headerSize, s3channel.size());

        int todo = (int) file.size - headerSize;
        while (todo >= chunkSize) {
            chunk.rewind();
            fc.read(chunk);
            chunk.rewind();
            assertEquals(chunkSize, s3channel.write(chunk));
            todo -= chunkSize;
        }
        if (todo > 0) {
            chunk.rewind();
            chunk.limit(todo);
            assertEquals(todo, fc.read(chunk));
            assertEquals(fc.position(), fc.size());
            chunk.rewind();
            assertEquals(todo, s3channel.write(chunk));
        }

        ByteBuffer header = ByteBuffer.allocate(headerSize);
        assertEquals(headerSize, fc.read(header, 0));

        assertThrows(BufferNotFitInHeaderException.class, () -> s3channel.write(ByteBuffer.allocate(1000), MIN_PART_SIZE - 5));
        assertThrows(IllegalArgumentException.class, () -> s3channel.position(MIN_PART_SIZE + 123));


        // simulate partial header write
        // 1. write by position
        header.position(0);
        header.limit(25);
        assertEquals(25, s3channel.write(header, 0));
        header.limit(50);
        assertEquals(25, s3channel.write(header, 25));

        // 2. relative write
        header.limit(headerSize);
        s3channel.position(50);

        assertThrows(BufferNotFitInHeaderException.class, () -> s3channel.write(ByteBuffer.allocate(MIN_PART_SIZE)));
        assertEquals(headerSize - 50, s3channel.write(header));

        fc.close();
        s3channel.close();
        assertFalse(s3channel.isOpen());

        verifyFileContentEquality();
        file.close();
    }
}
