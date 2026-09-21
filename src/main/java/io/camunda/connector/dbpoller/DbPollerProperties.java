package io.camunda.connector.dbpoller;

import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import io.camunda.connector.dbpoller.exception.DbPollerException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/**
 * Properties bound from the BPMN element template via
 * {@code InboundConnectorContext.bindProperties(DbPollerProperties.class)}.
 *
 * <p>Call {@link #validate()} after binding to enforce all constraints.
 */
public class DbPollerProperties {

    public enum WatermarkType { TIMESTAMP, BIGINT, STRING, UUID }

    // -------------------------------------------------------------------------
    // Connection
    // -------------------------------------------------------------------------

    private String jdbcUrl;
    private String username;
    private String password;
    private DatabaseDialect dialect;
    private Integer connectionTimeoutMs = 5_000;
    private Integer poolMaxSize = 2;

    // -------------------------------------------------------------------------
    // Polling
    // -------------------------------------------------------------------------

    private String pollingQuery;
    private Integer pollingIntervalSeconds = 30;
    private Integer batchSize = 100;
    private Integer queryTimeoutSeconds = 30;

    // -------------------------------------------------------------------------
    // Watermark
    // -------------------------------------------------------------------------

    private String watermarkColumn;
    private WatermarkType watermarkType = WatermarkType.TIMESTAMP;
    private String initialWatermark;

    // -------------------------------------------------------------------------
    // Correlation
    // -------------------------------------------------------------------------

    private String instanceId;

    // -------------------------------------------------------------------------
    // Resilience
    // -------------------------------------------------------------------------

    private Integer maxConsecutiveFailures = 5;
    private Integer circuitBreakerThreshold = 10;

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    /**
     * Validates all properties and auto-detects the dialect from the JDBC URL
     * when {@code dialect} is {@code null}.
     *
     * @throws DbPollerException if any constraint is violated
     */
    public void validate() {
        requireNonBlank(jdbcUrl, "jdbcUrl");
        requireNonBlank(username, "username");
        requireNonBlank(password, "password");
        requireNonBlank(pollingQuery, "pollingQuery");
        requireNonBlank(watermarkColumn, "watermarkColumn");

        if (!pollingQuery.contains(":lastWatermark")) {
            throw new DbPollerException(
                    "pollingQuery must contain the placeholder ':lastWatermark'");
        }
        if (pollingIntervalSeconds == null || pollingIntervalSeconds < 1) {
            throw new DbPollerException("pollingIntervalSeconds must be >= 1");
        }
        if (batchSize == null || batchSize < 1 || batchSize > 10_000) {
            throw new DbPollerException("batchSize must be between 1 and 10,000 inclusive");
        }

        parsedInitialWatermark();

        if (dialect == null) {
            dialect = DatabaseDialect.fromUrl(jdbcUrl);
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new DbPollerException(fieldName + " is required");
        }
    }

    // -------------------------------------------------------------------------
    // Watermark parsing
    // -------------------------------------------------------------------------

    /**
     * Parses {@code initialWatermark} according to the declared {@link WatermarkType}.
     * Returns a type-appropriate default when {@code initialWatermark} is blank or null.
     *
     * @return the parsed watermark value; never {@code null}
     * @throws DbPollerException if the raw value cannot be parsed
     */
    public Object parsedInitialWatermark() {
        boolean blank = initialWatermark == null || initialWatermark.isBlank();
        WatermarkType type = watermarkType == null ? WatermarkType.TIMESTAMP : watermarkType;

        return switch (type) {
            case TIMESTAMP -> {
                if (blank) yield Instant.EPOCH;
                try { yield Instant.parse(initialWatermark); }
                catch (Exception e) {
                    throw new DbPollerException(
                            "initialWatermark '" + initialWatermark + "' is not a valid " + type, e);
                }
            }
            case BIGINT -> {
                if (blank) yield 0L;
                try { yield Long.parseLong(initialWatermark); }
                catch (NumberFormatException e) {
                    throw new DbPollerException(
                            "initialWatermark '" + initialWatermark + "' is not a valid " + type, e);
                }
            }
            case UUID -> {
                if (blank) yield new UUID(0L, 0L);
                try { yield UUID.fromString(initialWatermark); }
                catch (IllegalArgumentException e) {
                    throw new DbPollerException(
                            "initialWatermark '" + initialWatermark + "' is not a valid " + type, e);
                }
            }
            case STRING -> blank ? "" : initialWatermark;
        };
    }

    // -------------------------------------------------------------------------
    // Hash
    // -------------------------------------------------------------------------

    /**
     * Returns the first 12 hex characters of the SHA-256 hash of {@code pollingQuery}.
     *
     * @throws DbPollerException if the JVM does not support SHA-256
     */
    public String getPollingQueryHash() {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            var bytes = digest.digest(pollingQuery.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes).substring(0, 12);
        } catch (NoSuchAlgorithmException e) {
            throw new DbPollerException("SHA-256 not available", e);
        }
    }

    // -------------------------------------------------------------------------
    // Getters / setters
    // -------------------------------------------------------------------------

    public String getJdbcUrl() { return jdbcUrl; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    /** Returns the dialect, auto-detecting from {@code jdbcUrl} if not explicitly set. */
    public DatabaseDialect getDialect() {
        if (dialect == null && jdbcUrl != null) {
            dialect = DatabaseDialect.fromUrl(jdbcUrl);
        }
        return dialect;
    }
    public void setDialect(DatabaseDialect dialect) { this.dialect = dialect; }

    public Integer getConnectionTimeoutMs() { return connectionTimeoutMs; }
    public void setConnectionTimeoutMs(Integer connectionTimeoutMs) { this.connectionTimeoutMs = connectionTimeoutMs; }

    public Integer getPoolMaxSize() { return poolMaxSize; }
    public void setPoolMaxSize(Integer poolMaxSize) { this.poolMaxSize = poolMaxSize; }

    public String getPollingQuery() { return pollingQuery; }
    public void setPollingQuery(String pollingQuery) { this.pollingQuery = pollingQuery; }

    public Integer getPollingIntervalSeconds() { return pollingIntervalSeconds; }
    public void setPollingIntervalSeconds(Integer pollingIntervalSeconds) { this.pollingIntervalSeconds = pollingIntervalSeconds; }

    public Integer getBatchSize() { return batchSize; }
    public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }

    public Integer getQueryTimeoutSeconds() { return queryTimeoutSeconds; }
    public void setQueryTimeoutSeconds(Integer queryTimeoutSeconds) { this.queryTimeoutSeconds = queryTimeoutSeconds; }

    public String getWatermarkColumn() { return watermarkColumn; }
    public void setWatermarkColumn(String watermarkColumn) { this.watermarkColumn = watermarkColumn; }

    public WatermarkType getWatermarkType() { return watermarkType; }
    public void setWatermarkType(WatermarkType watermarkType) { this.watermarkType = watermarkType; }

    public String getInitialWatermark() { return initialWatermark; }
    public void setInitialWatermark(String initialWatermark) { this.initialWatermark = initialWatermark; }

    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }

    public Integer getMaxConsecutiveFailures() { return maxConsecutiveFailures; }
    public void setMaxConsecutiveFailures(Integer maxConsecutiveFailures) { this.maxConsecutiveFailures = maxConsecutiveFailures; }

    public Integer getCircuitBreakerThreshold() { return circuitBreakerThreshold; }
    public void setCircuitBreakerThreshold(Integer circuitBreakerThreshold) { this.circuitBreakerThreshold = circuitBreakerThreshold; }

    // -------------------------------------------------------------------------
    // equals / hashCode on jdbcUrl + pollingQuery (stable instance key)
    // -------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbPollerProperties other)) return false;
        return Objects.equals(jdbcUrl, other.jdbcUrl)
                && Objects.equals(pollingQuery, other.pollingQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcUrl, pollingQuery);
    }
}

