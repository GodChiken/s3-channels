package io.github.mentegy.s3.channels.builder;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.github.mentegy.s3.channels.impl.S3RangedReadObjectChannel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("fast")
class S3ReadableObjectChannelBuilderTest {

    private static AmazonS3 amazonS3 = mock(AmazonS3.class);
    private S3ReadableObjectChannelBuilder builder;

    @BeforeAll
    static void initMock() {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(100);
        when(amazonS3.getObjectMetadata(anyString(), anyString()))
                .thenReturn(meta);
    }

    @BeforeEach
    void iLoveJavaVoid() {
        builder = newBuilder();
    }

    private S3ReadableObjectChannelBuilder newBuilder() {
        return new S3ReadableObjectChannelBuilder()
                .key("key")
                .bucket("bucket")
                .amazonS3(amazonS3);
    }

    @Test
    void testBuildNullArgs() {
        assertThrows(IllegalArgumentException.class, () ->
                newBuilder().key(null).build(), "Object key must be set");

        assertThrows(IllegalArgumentException.class, () ->
                newBuilder().bucket(null).build(), "S3 bucket must be set");

        assertThrows(IllegalArgumentException.class, () ->
                newBuilder().amazonS3(null).build(), "Amazon s3 must be set");
    }

    @Test
    void testBuild() {
        assertEquals(S3RangedReadObjectChannel.class, builder.build().getClass());
    }

    @Test
    void testGetters() {
        assertEquals(amazonS3, builder.amazonS3());
        assertEquals("bucket", builder.bucket());
        assertEquals("key", builder.key());
        assertFalse(builder.isBuffered());
        assertEquals(123, builder.buffered(123).bufferSize().intValue());
        assertTrue(builder.isBuffered());
    }
}
