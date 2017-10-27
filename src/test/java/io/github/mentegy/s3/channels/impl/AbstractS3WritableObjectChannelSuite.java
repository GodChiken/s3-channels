package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import io.github.mentegy.s3.channels.S3WritableObjectChannel;
import io.github.mentegy.s3.channels.builder.S3WritableObjectChannelBuilder;
import io.github.mentegy.s3.channels.testutils.AbstractS3Suite;
import io.github.mentegy.s3.channels.testutils.FileGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public abstract class AbstractS3WritableObjectChannelSuite<T extends S3WritableObjectChannel> extends AbstractS3Suite {

    protected final int chunkSize = 1000;
    protected final int headerSize = 128;
    protected T s3channel;
    protected String key;
    protected FileGenerator.TempFile file;


    protected InitiateMultipartUploadResult initMultiPart() {
        defaultAmazonS3().deleteObject(testBucket, this.key);
        return defaultAmazonS3().initiateMultipartUpload(new InitiateMultipartUploadRequest(testBucket, key));
    }


    protected S3WritableObjectChannelBuilder defaultBuilder(String uploadId) {
        return S3WritableObjectChannel.builder()
                .amazonS3(defaultAmazonS3())
                .defaultExecutorService()
                .bucket(testBucket)
                .key(key == null ? "key" : key)
                .uploadId(uploadId);
    }


    protected void verifyFileContentEquality() throws IOException {
        final Path tmp = Files.createTempFile("tmp", "" + key.hashCode());
        defaultAmazonS3().getObject(new GetObjectRequest(testBucket, key), tmp.toFile());

        assertArrayEquals(Files.readAllBytes(file.path), Files.readAllBytes(tmp));

        Files.delete(tmp);
    }
}
