package io.github.mentegy.s3.channels.builder;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import io.github.mentegy.s3.channels.S3WritableObjectChannel;
import io.github.mentegy.s3.channels.impl.S3AppendableDelayedHeaderObjectChannel;
import io.github.mentegy.s3.channels.impl.S3AppendableObjectChannel;
import io.github.mentegy.s3.channels.impl.S3WritableObjectChannelAsFileChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@Tag("fast")
class S3WritableObjectChannelBuilderTest {

    private AmazonS3 amazonS3 = mock(AmazonS3.class);
    private ExecutorService executorService = mock(ExecutorService.class);
    private S3WritableObjectChannelBuilder builder;

    @BeforeEach
    void iLoveJavaVoid() {
        builder = newBuilder();
    }

    private S3WritableObjectChannelBuilder newBuilder() {
        return new S3WritableObjectChannelBuilder()
                .key("key")
                .bucket("bucket")
                .uploadId("upldId")
                .executorService(executorService)
                .amazonS3(amazonS3)
                .failedPartUploadRetries(2);
    }

    @Test
    void testBuildNullArgs() {
        assertThrows(IllegalArgumentException.class, () ->
                newBuilder().key(null).build(), "Object key must be set");

        assertThrows(IllegalArgumentException.class, () ->
                newBuilder().bucket(null).build(), "S3 bucket must be set");

        assertThrows(IllegalArgumentException.class, () ->
                newBuilder().uploadId(null).build(), "Multi-part upload id must be set");

        assertThrows(IllegalArgumentException.class, () ->
                newBuilder().executorService(null).build(), "Executor service must be set");

        assertThrows(IllegalArgumentException.class, () ->
                newBuilder().amazonS3(null).build(), "Amazon s3 must be set");
    }

    @Test
    void testBuild() {
        assertEquals(S3AppendableObjectChannel.class, builder.build().getClass());
        assertEquals(S3AppendableDelayedHeaderObjectChannel.class, builder.delayedHeader(true).build().getClass());
        assertEquals(S3WritableObjectChannelAsFileChannel.class, builder.buildAsFileChannel().getClass());
    }

    @Test
    void testGetters() {
        assertEquals(S3WritableObjectChannel.MIN_PART_SIZE, builder.getPartSize());
        assertEquals(123, builder.partSize(123).getPartSize());
        assertEquals(amazonS3, builder.amazonS3());
        assertEquals(executorService, builder.executorService());
        assertEquals("upldId", builder.uploadId());
        assertEquals("bucket", builder.bucket());
        assertEquals("key", builder.key());
        assertEquals(2, builder.failedPartUploadRetries());
        assertFalse(builder.hasDelayedHeader());
        assertFalse(builder.closeExecutorOnChannelClose());
        assertTrue(builder.defaultCachedThreadPoolExecutor().closeExecutorOnChannelClose());

        InitiateMultipartUploadResult r = new InitiateMultipartUploadResult();
        r.setBucketName("bucket1");
        r.setKey("key1");
        r.setUploadId("upldId1");
        builder.initiateMultipartUploadResult(r);
        assertEquals("upldId1", builder.uploadId());
        assertEquals("bucket1", builder.bucket());
        assertEquals("key1", builder.key());

        builder.executorService().shutdown();
    }
}
