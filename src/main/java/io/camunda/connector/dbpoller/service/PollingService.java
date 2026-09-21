package io.camunda.connector.dbpoller.service;

import io.camunda.connector.api.inbound.Activity;
import io.camunda.connector.api.inbound.InboundConnectorContext;
import io.camunda.connector.api.inbound.Severity;
import io.camunda.connector.dbpoller.DbPollerProperties;
import io.camunda.connector.dbpoller.exception.DbPollerException;
import io.camunda.connector.dbpoller.model.RowPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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
     * @param watermarkStore the store tracking the current high-water mark
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
     * Executes one poll iteration: queries the database from the current watermark,
     * correlates each row as a BPMN message, and advances the watermark to the
     * highest value seen.
     *
     * @throws DbPollerException on SQL errors or correlation failures
     */
    public void pollOnce() {
        long startNanos = System.nanoTime();
        Object watermark = watermarkStore.read();
        String sql = buildSql();

        int rowsRead = 0;
        Object highestWatermarkSeen = null;

        try (var conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setQueryTimeout(props.getQueryTimeoutSeconds());
            bindWatermark(ps, 1, watermark);

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    var row = mapRow(rs, md);
                    Object rowWatermark = row.get(props.getWatermarkColumn());
                    var metadata = new RowPayload.Metadata(
                            props.getDialect().name(),
                            rowWatermark,
                            Instant.now(),
                            props.getInstanceId()
                    );
                    var payload = new RowPayload(row, metadata);
                    correlate(payload, rowWatermark);
                    highestWatermarkSeen = rowWatermark;
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

        if (rowsRead > 0 && highestWatermarkSeen != null) {
            watermarkStore.write(highestWatermarkSeen);
        }

        consecutiveFailures.set(0);

        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        LOG.info("Polled {} row(s) in {} ms; watermark={}", rowsRead, elapsedMs, watermark);
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
     * and appends a pagination clause if the query does not already contain one.
     */
    String buildSql() {
        String sql = props.getPollingQuery().replace(WATERMARK_PLACEHOLDER, "?");
        String upper = sql.toUpperCase();
        if (!upper.contains(" LIMIT ") && !upper.contains(" FETCH ") && !upper.contains(" TOP ")) {
            sql = sql + props.getDialect().paginationClause(props.getBatchSize());
        }
        return sql;
    }

    private void correlate(RowPayload payload, Object rowWatermark) {
        try {
            context.correlateWithResult(payload);
        } catch (Exception ex) {
            throw new DbPollerException(
                    "Correlation failed for row with watermark=" + rowWatermark + ": " + ex.getMessage(),
                    ex);
        }
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

