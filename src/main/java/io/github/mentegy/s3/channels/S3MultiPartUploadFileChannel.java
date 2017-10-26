package io.github.mentegy.s3.channels;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.builder.S3MultiPartUploadFileChannelBuilder;
import io.github.mentegy.s3.channels.util.CancelableByteChannel;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutorService;

public abstract class S3MultiPartUploadFileChannel extends FileChannel implements CancelableByteChannel {

    public static final int MAX_PARTS = 10_000;
    public static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    public final String key;
    public final String bucket;
    public final String uploadId;
    protected final int partSize;
    protected final AmazonS3 s3;
    protected final ExecutorService executor;
    protected final boolean closeExecutorOnFinish;

    public S3MultiPartUploadFileChannel(String key, String bucket, String uploadId, int partSize, AmazonS3 s3,
                                        ExecutorService executor, boolean closeExecutorOnFinish) {
        this.key = key;
        this.bucket = bucket;
        this.uploadId = uploadId;
        this.partSize = partSize >= MIN_PART_SIZE ? partSize : MIN_PART_SIZE;
        this.s3 = s3;
        this.executor = executor;
        this.closeExecutorOnFinish = closeExecutorOnFinish;
    }

    public static S3MultiPartUploadFileChannelBuilder builder() {
        return new S3MultiPartUploadFileChannelBuilder();
    }

    @Override
    public int read(ByteBuffer dst) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel truncate(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void force(boolean metaData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst, long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) {
        throw new UnsupportedOperationException();
    }
}
