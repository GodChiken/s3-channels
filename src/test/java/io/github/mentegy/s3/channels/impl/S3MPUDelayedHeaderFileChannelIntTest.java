package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.builder.S3MultiPartUploadFileChannelBuilder;
import io.github.mentegy.s3.channels.testutils.FileGenerator;
import io.github.mentegy.s3.channels.testutils.S3IntegrationTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.StandardOpenOption;

import static io.github.mentegy.s3.channels.S3MultiPartUploadFileChannel.MIN_PART_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("s3")
class S3MPUDelayedHeaderFileChannelIntTest extends S3IntegrationTest {

    private void initChannel(String fileKey) {
        this.key = "S3MPUDelayedHeaderFileChannelIntTest/" + fileKey;

        // header + 3 parts of 5mb each + 2048 bytes for last part
        file = FileGenerator.randomTempFile(headerSize + MIN_PART_SIZE * 3 + 2048);

        s3channel = (S3MPUDelayedHeaderFileChannel) defaultBuilder(initMultiPart().getUploadId())
                .withDelayedHeader()
                .build();
    }

    @Test
    void testAll() throws Exception {
        initChannel("testAll");
        final FileChannel fc = FileChannel.open(file.path, StandardOpenOption.READ);
        final ByteBuffer chunk = ByteBuffer.allocate(chunkSize);

        fc.position(headerSize);
        s3channel.position(headerSize);
        assertEquals(headerSize, s3channel.position());

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

        assertThrows(IllegalStateException.class,
                () -> s3channel.write(ByteBuffer.allocate(1000), MIN_PART_SIZE - 5),
                "Input data does not fit within header");

        // simulate partial header write
        header.position(0);
        header.limit(50);
        assertEquals(50, s3channel.write(header, 0));
        header.limit(headerSize);
        assertEquals(headerSize - 50, s3channel.write(header, 50));

        fc.close();
        s3channel.close();
        assertFalse(s3channel.isOpen());

        verifyFileContentEquality();
        file.close();
    }
}
