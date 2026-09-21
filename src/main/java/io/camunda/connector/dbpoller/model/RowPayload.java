package io.camunda.connector.dbpoller.model;

import java.time.Instant;
import java.util.Map;

/**
 * Payload delivered to Camunda for each polled database row.
 *
 * <p>JSON shape example:
 * <pre>{@code
 * {
 *   "row": {
 *     "id": 42,
 *     "updated_at": "2024-01-15T10:30:00Z",
 *     "status": "PENDING"
 *   },
 *   "metadata": {
 *     "dialect": "POSTGRES",
 *     "watermark": "2024-01-15T10:30:00Z",
 *     "polledAt": "2024-01-15T10:30:01.123Z",
 *     "instanceId": "my-process"
 *   }
 * }
 * }</pre>
 */
public record RowPayload(Map<String, Object> row, Metadata metadata) {

    public record Metadata(
            String dialect,
            Object watermark,
            Instant polledAt,
            String instanceId
    ) {}
}

