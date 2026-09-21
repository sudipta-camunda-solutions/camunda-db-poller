package io.camunda.connector.dbpoller.service;

import io.camunda.connector.api.inbound.Activity;
import io.camunda.connector.api.inbound.InboundConnectorContext;
import io.camunda.connector.api.inbound.Severity;
import io.camunda.connector.dbpoller.DbPollerProperties;
import io.camunda.connector.dbpoller.DbPollerProperties.ConsumptionStrategy;
import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import io.camunda.connector.dbpoller.exception.DbPollerException;
import io.camunda.connector.dbpoller.model.RowPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Executes one polling iteration against the configured database. */
public class PollingService {

    private static final Logger LOG = LoggerFactory.getLogger(PollingService.class);
    private static final String WATERMARK_PLACEHOLDER = ":lastWatermark";

    private final InboundConnectorContext context;
    private final DbPollerProperties props;
    private final DataSource dataSource;
    private final WatermarkStore watermarkStore;
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    /**
     * Creates a new {@code PollingService}.
     *
     * @param context        the inbound connector context for correlation and logging
     * @param props          validated connector properties
     * @param dataSource     the pooled data source to query
     * @param watermarkStore the store tracking the current high-water mark; ignored
     *                       when {@link ConsumptionStrategy} is not {@code WATERMARK}
     */
    public PollingService(InboundConnectorContext context,
                          DbPollerProperties props,
                          DataSource dataSource,
                          WatermarkStore watermarkStore) {
        this.context = context;
        this.props = props;
        this.dataSource = dataSource;
        this.watermarkStore = watermarkStore;
    }

    /**
     * Executes one poll iteration: queries the database, correlates each row as a
     * BPMN message, and tracks progress per the configured {@link ConsumptionStrategy}
     * — either advancing the watermark to the highest value seen (default), or
     * immediately flipping a flag column / deleting the row after each successful
     * correlation.
     *
     * @throws DbPollerException on SQL errors or correlation failures
     */
    public void pollOnce() {
        long startNanos = System.nanoTime();
        ConsumptionStrategy strategy = props.getConsumptionStrategy();
        Object watermark = strategy == ConsumptionStrategy.WATERMARK ? watermarkStore.read() : null;
        String sql = buildSql();

        int rowsRead = 0;
        Object highestWatermarkSeen = null;

        try (var conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setQueryTimeout(props.getQueryTimeoutSeconds());
            if (strategy == ConsumptionStrategy.WATERMARK) {
                bindWatermark(ps, 1, watermark);
            }

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    var row = mapRow(rs, md);

                    if (strategy == ConsumptionStrategy.WATERMARK) {
                        Object rowWatermark = normalizeWatermark(row.get(props.getWatermarkColumn()));
                        var payload = new RowPayload(row, metadataFor(rowWatermark));
                        correlate(payload, rowWatermark);
                        highestWatermarkSeen = rowWatermark;
                    } else {
                        var payload = new RowPayload(row, metadataFor(null));
                        correlate(payload, row.get(props.getKeyColumn()));
                        applyConsumption(conn, row);
                    }
                    rowsRead++;
                }
            }

        } catch (SQLException ex) {
            consecutiveFailures.incrementAndGet();
            throw new DbPollerException("SQL error during poll: " + ex.getMessage(), ex);
        } catch (RuntimeException ex) {
            consecutiveFailures.incrementAndGet();
            throw ex;
        }

        if (strategy == ConsumptionStrategy.WATERMARK && rowsRead > 0 && highestWatermarkSeen != null) {
            watermarkStore.write(highestWatermarkSeen);
        }

        consecutiveFailures.set(0);

        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        if (strategy == ConsumptionStrategy.WATERMARK) {
            LOG.info("Polled {} row(s) in {} ms; watermark={}", rowsRead, elapsedMs, watermark);
        } else {
            LOG.info("Polled {} row(s) in {} ms; strategy={}", rowsRead, elapsedMs, strategy);
        }
        context.log(Activity.level(Severity.INFO)
                .tag("poll")
                .message(String.format("Polled %d row(s) in %d ms", rowsRead, elapsedMs)));
    }

    /** Returns the number of consecutive polling failures since the last success. */
    public int consecutiveFailures() {
        return consecutiveFailures.get();
    }

    /**
     * Builds the SQL to execute: replaces the watermark placeholder with {@code ?}
     * (a no-op when the strategy doesn't use one) and appends a pagination clause
     * if the query does not already contain one.
     */
    String buildSql() {
        String sql = props.getPollingQuery().replace(WATERMARK_PLACEHOLDER, "?");
        String upper = sql.toUpperCase();
        if (!upper.contains(" LIMIT ") && !upper.contains(" FETCH ") && !upper.contains(" TOP ")) {
            sql = sql + props.getDialect().paginationClause(props.getBatchSize());
        }
        return sql;
    }

    private RowPayload.Metadata metadataFor(Object rowWatermark) {
        return new RowPayload.Metadata(
                props.getDialect().name(),
                rowWatermark,
                Instant.now(),
                props.getInstanceId()
        );
    }

    private void correlate(RowPayload payload, Object rowKey) {
        try {
            context.correlateWithResult(payload);
        } catch (Exception ex) {
            throw new DbPollerException(
                    "Correlation failed for row with key=" + rowKey + ": " + ex.getMessage(),
                    ex);
        }
    }

    /**
     * Marks a row as consumed immediately after a successful correlation, per the
     * configured {@link ConsumptionStrategy}. No-op for {@code WATERMARK}.
     */
    private void applyConsumption(Connection conn, Map<String, Object> row) throws SQLException {
        ConsumptionStrategy strategy = props.getConsumptionStrategy();
        if (strategy == ConsumptionStrategy.WATERMARK) {
            return;
        }
        Object keyValue = row.get(props.getKeyColumn());
        if (keyValue == null) {
            throw new DbPollerException(
                    "Row is missing keyColumn '" + props.getKeyColumn() + "'; cannot mark it consumed");
        }
        String sql = strategy == ConsumptionStrategy.UPDATE_FLAG ? updateFlagSql() : deleteRowSql();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, keyValue);
            ps.executeUpdate();
        }
    }

    private String updateFlagSql() {
        DatabaseDialect dialect = props.getDialect();
        return "UPDATE " + dialect.quoteIdentifier(props.getTargetTable())
                + " SET " + dialect.quoteIdentifier(props.getFlagColumn()) + " = " + dialect.trueLiteral()
                + " WHERE " + dialect.quoteIdentifier(props.getKeyColumn()) + " = ?";
    }

    private String deleteRowSql() {
        DatabaseDialect dialect = props.getDialect();
        return "DELETE FROM " + dialect.quoteIdentifier(props.getTargetTable())
                + " WHERE " + dialect.quoteIdentifier(props.getKeyColumn()) + " = ?";
    }

    /**
     * Coerces a raw JDBC column value to the same Java type
     * {@link DbPollerProperties#parsedInitialWatermark()} would produce, so a
     * watermark read from the database is type-consistent with the initial
     * value regardless of driver-specific boxing quirks (e.g. SQLite's driver
     * returns {@code Integer} rather than {@code Long} for integer columns).
     */
    private Object normalizeWatermark(Object value) {
        if (value == null) {
            return null;
        }
        return switch (props.getWatermarkType()) {
            case BIGINT -> value instanceof Long ? value : ((Number) value).longValue();
            case STRING -> value instanceof String ? value : value.toString();
            case TIMESTAMP, UUID -> value;
        };
    }

    void bindWatermark(PreparedStatement ps, int idx, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.VARCHAR);
            return;
        }
        switch (props.getWatermarkType()) {
            case TIMESTAMP -> {
                Instant instant = (value instanceof Instant i)
                        ? i
                        : Instant.parse(value.toString());
                ps.setTimestamp(idx, Timestamp.from(instant));
            }
            case BIGINT -> ps.setLong(idx, ((Number) value).longValue());
            case UUID -> {
                UUID uuid = (value instanceof UUID u)
                        ? u
                        : UUID.fromString(value.toString());
                ps.setObject(idx, uuid);
            }
            case STRING -> ps.setString(idx, value.toString());
        }
    }

    static Map<String, Object> mapRow(ResultSet rs, ResultSetMetaData md) throws SQLException {
        int count = md.getColumnCount();
        var map = new LinkedHashMap<String, Object>(count * 2);
        for (int i = 1; i <= count; i++) {
            map.put(md.getColumnLabel(i), normalize(rs.getObject(i)));
        }
        return map;
    }

    private static Object normalize(Object value) {
        if (value == null) return null;
        if (value instanceof java.sql.Timestamp ts) return ts.toInstant();
        if (value instanceof java.sql.Date d) return d.toLocalDate();
        if (value instanceof java.sql.Time t) return t.toLocalTime();
        return value;
    }
}
