package io.camunda.connector.dbpoller.service;

import java.util.concurrent.atomic.AtomicReference;

/** In-memory, thread-safe {@link WatermarkStore} backed by an {@link AtomicReference}. */
public class InMemoryWatermarkStore implements WatermarkStore {

    private final AtomicReference<Object> current;

    /**
     * Creates a new store initialised with the given value.
     *
     * @param initialValue the starting watermark; may be {@code null}
     */
    public InMemoryWatermarkStore(Object initialValue) {
        this.current = new AtomicReference<>(initialValue);
    }

    @Override
    public Object read() {
        return current.get();
    }

    @Override
    public void write(Object watermark) {
        current.set(watermark);
    }
}

