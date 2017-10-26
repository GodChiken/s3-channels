package io.github.mentegy.s3.channels;

import java.util.concurrent.CompletableFuture;

public interface AsynchronousCancellable<T> {
    CompletableFuture<T> cancel();
    CompletableFuture<T> getCancellation();
}
