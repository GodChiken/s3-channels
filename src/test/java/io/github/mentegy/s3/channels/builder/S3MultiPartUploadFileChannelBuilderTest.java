package io.github.mentegy.s3.channels.builder;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.S3MultiPartUploadFileChannel;
import io.github.mentegy.s3.channels.impl.S3MPUDelayedHeaderFileChannel;
import io.github.mentegy.s3.channels.impl.S3MPUFileChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@Tag("fast")
class S3MultiPartUploadFileChannelBuilderTest {

    private AmazonS3 amazonS3 = mock(AmazonS3.class);
    private ExecutorService executorService = mock(ExecutorService.class);
    private S3MultiPartUploadFileChannelBuilder builder;

    @BeforeEach
    void initBuilder() {
        builder = new S3MultiPartUploadFileChannelBuilder()
                .withKey("key")
                .withBucket("bucket")
                .withUploadId("upldId")
                .withExecutorService(executorService)
                .withAmazonS3(amazonS3);
    }

    @Test
    void testBuildNullArgs() {
        String msg = assertThrows(IllegalArgumentException.class, () -> builder.withKey(null).build()).getMessage();
        assertEquals("Object key must be set", msg);

        initBuilder();
        msg = assertThrows(IllegalArgumentException.class, () -> builder.withBucket(null).build()).getMessage();
        assertEquals("S3 bucket must be set", msg);

        initBuilder();
        msg = assertThrows(IllegalArgumentException.class, () -> builder.withUploadId(null).build()).getMessage();
        assertEquals("Multi-part upload id must be set", msg);

        initBuilder();
        msg = assertThrows(IllegalArgumentException.class, () -> builder.withExecutorService(null).build()).getMessage();
        assertEquals("Executor service must be set", msg);

        initBuilder();
        msg = assertThrows(IllegalArgumentException.class, () -> builder.withAmazonS3(null).build()).getMessage();
        assertEquals("Amazon s3 must be set", msg);
    }

    @Test
    void testBuild() {
        assertEquals(S3MPUFileChannel.class, builder.build().getClass());
        assertEquals(S3MPUDelayedHeaderFileChannel.class, builder.withDelayedHeader().build().getClass());
    }

    @Test
    void testGetters() {
        assertEquals(S3MultiPartUploadFileChannel.MIN_PART_SIZE, builder.getPartSize());
        assertEquals(123, builder.withPartSize(123).getPartSize());
        assertEquals(amazonS3, builder.getAmazonS3());
        assertEquals(executorService, builder.getExecutorService());
        assertEquals("upldId", builder.getUploadId());
        assertEquals("bucket", builder.getBucket());
        assertEquals("key", builder.getKey());
        assertFalse(builder.hasDelayedHeader());
        assertTrue(builder.withClosingExecutorOnFinish().isCloseExecutorOnFinish());
        assertFalse(builder.setCloseExecutorOnFinish(false).isCloseExecutorOnFinish());

    }
}
