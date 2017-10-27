package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import io.github.mentegy.s3.channels.S3WritableObjectChannel;
import io.github.mentegy.s3.channels.util.ByteBufferUtils;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * A write-only, stateful S3 object byte channel backed by S3 multi-part upload API.
 * This channel is append-only, e.g. already written data could not be changed and
 * position decreased.
 * <p>
 * Writes operation are non-blocking, e.g. caller does not wait on particular part
 * to be uploaded into S3. The actual upload will be performed on dedicated thread
 * only if part buffer is full-filled.
 * <p>
 * All operations are done on-the-fly, e.g. no sync with actual disk is made.
 * <p>
 * Not thread safe since maintaining current position
 */
public class S3AppendableObjectChannel extends S3WritableObjectChannel {

    protected final ConcurrentLinkedQueue<UploadPartResult> done = new ConcurrentLinkedQueue<>();
    protected final ConcurrentHashMap<Integer, CompletableFuture<Void>> workers = new ConcurrentHashMap<>();
    protected long pos;
    protected int id;
    protected ByteBuffer partBody;
    protected volatile CompletableFuture<Void> cancellation;
    protected volatile Throwable error = null;
    protected volatile boolean closed = false;

    public S3AppendableObjectChannel(String key, String bucket, String uploadId, int partSize, AmazonS3 s3,
                                     ExecutorService executor, boolean closeExecutorOnFinish, int failedPartUploadRetries) {
        super(key, bucket, uploadId, partSize, s3, executor, closeExecutorOnFinish, failedPartUploadRetries);
        this.id = 1;
        this.partBody = ByteBuffer.allocate(this.partSize);
    }

    @Override
    public boolean hasDelayedHeader() {
        return false;
    }

    @Override
    public int headerSize() {
        return 0;
    }

    @Override
    public int write(ByteBuffer src, int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src) {
        checkOnError();
        int bytes = src.remaining();
        if (partBody.remaining() >= bytes) {
            partBody.put(src);
            pos += bytes;
            return bytes;
        } else {
            bytes = ByteBufferUtils.putBiggerBuffer(partBody, src);
            uploadCurrentPart();
            pos += bytes;
            return bytes + write(src);
        }
    }

    protected void uploadCurrentPart() {
        UploadPartRequest req = createRequest(id, partBody);
        id += 1;
        partBody = ByteBuffer.allocate(partSize);
        startWorker(req, 0);
    }

    protected UploadPartRequest createRequest(int id, ByteBuffer buffer) {
        buffer.rewind();
        return new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(id)
                .withPartSize(buffer.limit())
                .withInputStream(new ByteArrayInputStream(buffer.array(), 0, buffer.limit()));
    }

    protected void startWorker(UploadPartRequest req, int retries) {
        int id = req.getPartNumber();

        CompletableFuture<Void> f = CompletableFuture
                .supplyAsync(() -> s3.uploadPart(req), executor)
                .handle((res, error) -> {
                    workers.remove(id);
                    if (res != null) {
                        done.add(res);
                    }
                    if (error != null && isOpen()) {
                        if (retries < failedPartUploadRetries) {
                            startWorker(req, retries + 1);
                        } else {
                            this.error = new IllegalStateException("Could not upload part " + id + " after "
                                    + retries + " retries. Aborting upload", error.getCause());
                            cancel();
                        }
                    }
                    return null;
                });

        workers.put(id, f);
    }

    @Override
    public S3WritableObjectChannel position(long newPosition) {
        return this;
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public long size() {
        return pos;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void close() {
        if (!isOpen()) {
            return;
        }
        closed = true;
        uploadPendingParts();
        CompletableFuture<Void>[] futures;
        synchronized (workers) {
            futures = workers.values().toArray(new CompletableFuture[workers.size()]);
        }
        CompletableFuture<Void> complete = CompletableFuture
                .allOf(futures)
                .thenApplyAsync((x) -> {
                    s3.completeMultipartUpload(new CompleteMultipartUploadRequest()
                            .withBucketName(bucket)
                            .withKey(key)
                            .withUploadId(uploadId)
                            .withPartETags(done));
                    return null;
                }, executor);

        try {
            complete.get();
        } catch (Exception e) {
            cancel();
            if (e instanceof ExecutionException &&
                    e.getCause() != null &&
                    e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            if (closeExecutorOnClose) {
                executor.shutdown();
            }
        }
    }

    protected void uploadPendingParts() {
        partBody.flip();
        if (partBody.limit() > 0) {
            startWorker(createRequest(id, partBody).withLastPart(true), 0);
        }
    }

    protected void checkOnError() {
        if (error != null) {
            throw new IllegalStateException("Caught error during uploading part to s3", error);
        } else if (id > MAX_PARTS) {
            throw new IllegalStateException("Reached max allowed number of parts (10000)");
        }
    }

    @Override
    public CompletableFuture<Void> cancel() {
        if (cancellation == null) {
            cancellation = CompletableFuture.runAsync(() ->
                    s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId)));
        }
        return cancellation;
    }

    @Override
    public CompletableFuture<Void> getCancellation() {
        return cancellation;
    }

    @Override
    public boolean isOpen() {
        return !closed && cancellation == null;
    }
}
