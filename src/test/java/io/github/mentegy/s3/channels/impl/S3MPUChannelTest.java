package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.UploadPartRequest;
import io.github.mentegy.s3.channels.builder.S3MultiPartUploadFileChannelBuilder;
import io.github.mentegy.s3.channels.testutils.FileGenerator;
import io.github.mentegy.s3.channels.util.TestException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;

import static io.github.mentegy.s3.channels.S3MultiPartUploadChannel.MAX_PARTS;
import static io.github.mentegy.s3.channels.S3MultiPartUploadChannel.MIN_PART_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("s3")
class S3MPUChannelTest extends AbstractS3MPUChannelSuite<S3MPUChannel> {

    private void initChannel(String fileKey) {
        this.key = "S3MPUChannelTest/" + fileKey;
        // 3 parts of 5mb each + 2048 bytes for last part
        file = FileGenerator.randomTempFile(5 * 1024 * 1024 * 3 + 2048);

        s3channel = (S3MPUChannel) defaultBuilder(initMultiPart().getUploadId()).build();
    }

    @Test
    void testAll() throws Exception {
        initChannel("testAll");

        assertFalse(s3channel.hasDelayedHeader());
        assertEquals(0, s3channel.headerSize());

        final FileChannel fc = FileChannel.open(file.path, StandardOpenOption.READ);
        final ByteBuffer chunk = ByteBuffer.allocate(chunkSize);

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
        assertEquals(s3channel.cancel(), s3channel.cancel());
        assertFalse(s3channel.isOpen());
        file.close();
    }

    @Test
    void testFailedUploadPart() throws Exception {
        final AmazonS3 mocked = mock(AmazonS3.class);
        s3channel = (S3MPUChannel) defaultBuilder("id")
                .failedPartUploadRetries(3)
                .amazonS3(mocked)
                .build();
        when(mocked.uploadPart(any())).thenThrow(new TestException());

        s3channel.skip(MIN_PART_SIZE).write(ByteBuffer.allocate(123));
        while (s3channel.getCancellation() == null) {
            Thread.sleep(25);
        }
        s3channel.getCancellation().get();
        assertTrue(!s3channel.getCancellation().isCompletedExceptionally());
        assertFalse(s3channel.isOpen());

        //coverage
        s3channel.startWorker(new UploadPartRequest().withPartNumber(1), 0);

        assertThrows(IllegalStateException.class, () -> s3channel.write(ByteBuffer.allocate(1)));

        verify(mocked, times(1)).abortMultipartUpload(any());
    }

    @Test
    void testExecutionExceptionOnClose() throws Exception {
        // test failed close
        final AmazonS3 mockedS3 = mock(AmazonS3.class);
        final S3MultiPartUploadFileChannelBuilder builder = defaultBuilder("id")
                .amazonS3(mockedS3);
        s3channel = (S3MPUChannel) builder.build();
        when(mockedS3.completeMultipartUpload(any())).thenThrow(new TestException());

        assertThrows(TestException.class, () -> s3channel.close());

        s3channel.close(); // already closed, no effect, should we throw exception?

        while (s3channel.getCancellation() == null) {
            Thread.sleep(10);
        }
        s3channel.getCancellation().get();

        assertTrue(!s3channel.getCancellation().isCompletedExceptionally());
        verify(mockedS3, times(1)).abortMultipartUpload(any());
    }

    @Test
    void makeCoverageHappy() throws Exception {
        // test failed close
        final AmazonS3 mockedS3 = mock(AmazonS3.class);
        final ExecutorService mockedES = mock(ExecutorService.class);


        doThrow(new OutOfMemoryError()).when(mockedES).execute(any());

        final S3MultiPartUploadFileChannelBuilder builder = defaultBuilder("id")
                .executorService(mockedES)
                .closeExecutorOnChannelClose(false)
                .amazonS3(mockedS3);
        s3channel = (S3MPUChannel) builder.build();
        assertThrows(RuntimeException.class, () -> s3channel.close());
        assertThrows(UnsupportedOperationException.class, () -> s3channel.truncate(-1));
        assertThrows(UnsupportedOperationException.class, () -> s3channel.truncate(123 + (long) Integer.MAX_VALUE));
        assertThrows(UnsupportedOperationException.class, () -> s3channel.read(null));
        assertThrows(UnsupportedOperationException.class, () -> s3channel.write(null, 1));
    }

}
