package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import io.github.mentegy.s3.channels.builder.S3MultiPartUploadFileChannelBuilder;
import io.github.mentegy.s3.channels.testutils.FileGenerator;
import io.github.mentegy.s3.channels.testutils.S3IntegrationTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("s3")
class S3MPUDelayedHeaderFileChannelIntTest extends S3IntegrationTest {

    private S3MPUDelayedHeaderFileChannel s3channel;
    private String key;
    private FileGenerator.TempFile file;
    private final int chunkSize = 1000;
    private final int headerSize = 128;

    private void initChannel(String fileKey) {
        this.key = "S3MPUDelayedHeaderFileChannelIntTest/" + fileKey;
        defaultAmazonS3().deleteObject(testBucket(), this.key);
        InitiateMultipartUploadResult resp = defaultAmazonS3()
                .initiateMultipartUpload(new InitiateMultipartUploadRequest(testBucket(), key));

        // header + 3 parts of 5mb each + 2048 bytes for last part
        file = FileGenerator.randomTempFile(headerSize + 5 * 1024 * 1024 * 3 + 2048);

        s3channel = (S3MPUDelayedHeaderFileChannel) new S3MultiPartUploadFileChannelBuilder()
                .withAmazonS3(defaultAmazonS3())
                .withExecutorService(Executors.newFixedThreadPool(4))
                .withBucket(testBucket())
                .withKey(key)
                .withUploadId(resp.getUploadId())
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

        // simulate partial header write
        header.position(0);
        header.limit(50);
        assertEquals(50, s3channel.write(header, 0));
        header.limit(headerSize);
        assertEquals(headerSize - 50, s3channel.write(header, 50));

        fc.close();
        s3channel.close();

        verifyFileContentEquality(file.path, key);
    }
}
