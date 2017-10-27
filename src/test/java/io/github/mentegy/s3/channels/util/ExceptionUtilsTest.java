package io.github.mentegy.s3.channels.util;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static io.github.mentegy.s3.channels.util.ExceptionUtils.*;
import static org.junit.jupiter.api.Assertions.*;

@Tag("fast")
class ExceptionUtilsTest {

    @Test
    void testMapExecutionException() {
        new ExceptionUtils();


        assertThrows(RuntimeException.class, () ->
                mapExecutionException(new IllegalStateException()));

        assertThrows(RuntimeException.class, () ->
                mapExecutionException(new ExecutionException(null)));

        assertThrows(RuntimeException.class, () ->
                mapExecutionException(new ExecutionException(new IOException())));

        assertThrows(IllegalStateException.class, () ->
                mapExecutionException(new ExecutionException(new IllegalStateException())));

    }
}
