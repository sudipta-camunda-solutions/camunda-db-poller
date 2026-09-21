package io.camunda.connector.dbpoller.service;

/**
 * Thread-safe store for the polling watermark value.
 *
 * <p>Implementations MUST be thread-safe. All reads and writes may occur
 * concurrently from the polling thread and any management thread.
 */
public interface WatermarkStore {

    /** Returns the current watermark value, or the initial value if never written. */
    Object read();

    /** Persists the given watermark value so subsequent reads reflect it. */
    void write(Object watermark);

    /** Releases any resources held by this store. Default implementation is a no-op. */
    default void close() {
        // no-op
    }
}

