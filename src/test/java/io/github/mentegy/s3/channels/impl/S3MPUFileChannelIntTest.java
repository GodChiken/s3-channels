package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.builder.S3MultiPartUploadFileChannelBuilder;
import io.github.mentegy.s3.channels.testutils.FileGenerator;
import io.github.mentegy.s3.channels.testutils.S3IntegrationTest;
import io.github.mentegy.s3.channels.util.TestException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;

import static io.github.mentegy.s3.channels.S3MultiPartUploadFileChannel.MAX_PARTS;
import static io.github.mentegy.s3.channels.S3MultiPartUploadFileChannel.MIN_PART_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("s3")
class S3MPUFileChannelIntTest extends S3IntegrationTest {

    private void initChannel(String fileKey) {
        this.key = "S3MPUFileChannelIntTest/" + fileKey;

        // 3 parts of 5mb each + 2048 bytes for last part
        file = FileGenerator.randomTempFile(5 * 1024 * 1024 * 3 + 2048);

        s3channel = (S3MPUFileChannel) defaultBuilder(initMultiPart().getUploadId()).build();
    }

    @Test
    void testAll() throws Exception {
        initChannel("testAll");
        FileChannel fc = FileChannel.open(file.path, StandardOpenOption.READ);
        ByteBuffer chunk = ByteBuffer.allocate(chunkSize);
        int todo = (int) file.size;
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

        // setting lower position does not make any effect
        assertEquals(s3channel.position(s3channel.position() - 100).position(), s3channel.size());

        fc.close();
        s3channel.close();
        assertFalse(s3channel.isOpen());

        verifyFileContentEquality();
        file.close();
    }

    @Test
    void testTooManyIdsAndCancel() {
        initChannel("testTooManyIdsAndCancel");
        s3channel.id = MAX_PARTS + 1;

        assertThrows(IllegalStateException.class, () -> s3channel
                        .position(MIN_PART_SIZE - 1)
                        .write(ByteBuffer.allocate(100)),
                "Cannot upload more parts. Reached max parts number limit (10000). Please finish current upload");
        assertTrue(s3channel.isOpen());
        s3channel.cancel();
        assertFalse(s3channel.isOpen());
        file.close();
    }

    @Test
    void testFailedUploadPart() throws Exception {
        AmazonS3 mocked = mock(AmazonS3.class);
        s3channel = (S3MPUFileChannel) defaultBuilder("id")
                .withAmazonS3(mocked)
                .build();
        when(mocked.uploadPart(any())).thenThrow(new TestException());

        s3channel.skip(MIN_PART_SIZE).write(ByteBuffer.allocate(1));
        while (s3channel.workers.size() != 0) {
            System.out.println("waiting on workers...");
            // ┏(･o･)┛♪┗ (･o･) ┓
            Thread.sleep(50);
        }
        assertTrue(s3channel.getCancellationTask().isDone());
        assertFalse(s3channel.isOpen());

        assertThrows(IllegalStateException.class, () -> s3channel.write(ByteBuffer.allocate(1)));
        assertThrows(IllegalStateException.class, () -> s3channel.write(ByteBuffer.allocate(1), 0));

        verify(mocked, times(1)).abortMultipartUpload(any());
    }

    @Test
    void testFailedClose() throws Exception {
        AmazonS3 mocked = mock(AmazonS3.class);
        S3MultiPartUploadFileChannelBuilder builder = defaultBuilder("id")
                .withAmazonS3(mocked);
        s3channel = (S3MPUFileChannel) builder.build();
        when(mocked.completeMultipartUpload(any())).thenThrow(new TestException());

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> s3channel.close());

        while (!builder.getExecutorService().isTerminated()) {
            Thread.sleep(10);
        }

        assertTrue(ex.getCause() instanceof ExecutionException);
        assertTrue(ex.getCause().getCause() instanceof TestException);
        assertTrue(s3channel.getCancellationTask().isDone());
        verify(mocked, times(1)).abortMultipartUpload(any());
    }

}
