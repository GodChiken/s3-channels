package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.S3WritableObjectChannel;
import io.github.mentegy.s3.channels.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

/**
 * Extends {@link S3AppendableObjectChannel} by adding possibility to write delayed header.
 * Please refer parent implementation for more details.
 * <p>
 * Header size is always equal to the size of regular part. It will be uploaded together with
 * last part on {@link S3AppendableDelayedHeaderObjectChannel#close} call.
 */
public class S3AppendableDelayedHeaderObjectChannel extends S3AppendableObjectChannel {

    protected ByteBuffer header;
    protected long size;

    public S3AppendableDelayedHeaderObjectChannel(String key, String bucket, String uploadId, int partSize, AmazonS3 s3,
                                                  ExecutorService executor, boolean closeExecutorOnFinish, int failedPartUploadRetries) {
        super(key, bucket, uploadId, partSize, s3, executor, closeExecutorOnFinish, failedPartUploadRetries);
        this.header = ByteBuffer.allocate(this.partSize);
        this.partBody = ByteBuffer.allocate(this.partSize);
        this.id = 2;
        this.pos = 0;
        this.size = 0;
    }

    @Override
    public boolean hasDelayedHeader() {
        return true;
    }

    @Override
    public int headerSize() {
        return header.limit();
    }

    @Override
    public int write(ByteBuffer src, long position) {
        checkOnError();
        int bytes = src.remaining();
        if (position + bytes > header.limit()) {
            throw new BufferNotFitInHeaderException(bytes, (int)position, header.limit());
        } else {
            header.position((int)position);
            header.put(src);
            return bytes;
        }
    }

    @Override
    public int write(ByteBuffer src) {
        checkOnError();
        int bytes = src.remaining();

        if (pos < header.limit()) {
            // delayed write by relative position
            if (size > header.limit() && (bytes + pos) > header.limit()) {
                throw new BufferNotFitInHeaderException(bytes, (int)pos, header.limit());
            }
            header.position((int)pos);
            if (header.remaining() > bytes) {
                header.put(src);
                pos += bytes;
                size += bytes;
                return bytes;
            } else {
                bytes = ByteBufferUtils.putBiggerBuffer(header, src);
                pos += bytes;
                size += bytes;
                return bytes + write(src);
            }
        } else {
            bytes = super.write(src);
            size += bytes;
            return bytes;
        }
    }


    @Override
    protected void uploadPendingParts() {
        startWorker(createRequest(1, header), 0); // header part
        super.uploadPendingParts(); // last part

    }

    @Override
    public S3WritableObjectChannel position(long newPosition) {
        if (newPosition > size) {
            pos = size;
            truncate(newPosition - size);
        } if (newPosition == size) {
            pos = size;
        } else if (newPosition < header.limit()) {
            pos = newPosition;
        } else {
            throw new IllegalArgumentException("New position could not be set on already written data");
        }
        return this;
    }

    @Override
    public long size() {
        return size;
    }
}
