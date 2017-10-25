package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.github.mentegy.s3.channels.builder.S3MultiPartUploadFileChannelBuilder;
import io.github.mentegy.s3.channels.testutils.FileGenerator;
import io.github.mentegy.s3.channels.testutils.S3IntegrationTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("s3")
class S3MPUFileChannelIntTest extends S3IntegrationTest {

    private S3MPUFileChannel s3channel;
    private String key;
    private FileGenerator.TempFile file;
    private final int chunkSize = 1000;

    private void initChannel(String fileKey) {
        this.key = "S3MPUFileChannelIntTest/" + fileKey;
        getDefaultAmazonS3().deleteObject(getTestBucket(), this.key);
        InitiateMultipartUploadResult resp = getDefaultAmazonS3()
                .initiateMultipartUpload(new InitiateMultipartUploadRequest(getTestBucket(), key));

        // 3 parts of 5mb each + 2048 bytes for last part
        file = FileGenerator.randomTempFile(5 * 1024 * 1024 * 3 + 2048);

        s3channel = (S3MPUFileChannel) new S3MultiPartUploadFileChannelBuilder()
                .withAmazonS3(getDefaultAmazonS3())
                .withExecutorService(Executors.newFixedThreadPool(4))
                .withBucket(getTestBucket())
                .withKey(key)
                .withUploadId(resp.getUploadId())
                .build();
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
        fc.close();
        s3channel.close();

        Path tmp = Files.createTempFile("tmp", "test");
        ObjectMetadata obj = getDefaultAmazonS3().getObject(new GetObjectRequest(getTestBucket(), key), tmp.toFile());

        assertEquals(file.size, obj.getContentLength());
        assertArrayEquals(Files.readAllBytes(file.path), Files.readAllBytes(tmp));
    }
}
