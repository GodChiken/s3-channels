package io.github.mentegy.s3.channels.util;

import java.util.concurrent.ExecutionException;

public final class ExceptionUtils {

    public static RuntimeException mapExecutionException(Exception e) {
        if (e instanceof ExecutionException &&
                e.getCause() != null &&
                e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
        } else {
            throw new RuntimeException(e);
        }
    }

}
