package io.camunda.connector.dbpoller.service;

import io.camunda.connector.dbpoller.DbPollerProperties.WatermarkType;
import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import io.camunda.connector.dbpoller.exception.DbPollerException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link WatermarkStore} backed by an operator-created JDBC table, so the
 * watermark survives connector restarts/redeployments.
 *
 * <p>The table must already exist (see README for DDL) with columns
 * {@code poller_key}, {@code watermark_value}, and {@code updated_at}. Writes
 * use a DELETE-then-INSERT transaction rather than a dialect-specific
 * UPSERT/MERGE, so the same code works unmodified across every supported
 * dialect.
 */
public class JdbcWatermarkStore implements WatermarkStore {

    private final DataSource dataSource;
    private final DatabaseDialect dialect;
    private final String tableName;
    private final String pollerKey;
    private final WatermarkType watermarkType;
    private final Object initialValue;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Creates a new store.
     *
     * @param dataSource    a writable data source pointed at the database holding the watermark table
     * @param dialect       the SQL dialect, used to quote identifiers
     * @param tableName     the (operator-created) watermark table name
     * @param pollerKey     stable identifier for this connector element's row
     * @param watermarkType the type used to parse/serialize the stored value
     * @param initialValue  value to return when no row exists yet for {@code pollerKey}
     */
    public JdbcWatermarkStore(DataSource dataSource, DatabaseDialect dialect, String tableName,
                               String pollerKey, WatermarkType watermarkType, Object initialValue) {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.tableName = tableName;
        this.pollerKey = pollerKey;
        this.watermarkType = watermarkType;
        this.initialValue = initialValue;
    }

    @Override
    public Object read() {
        lock.lock();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(selectSql())) {
            ps.setString(1, pollerKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return parse(rs.getString(1));
                }
            }
            return initialValue;
        } catch (SQLException ex) {
            throw new DbPollerException("Failed to read durable watermark for '" + pollerKey + "'", ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(Object watermark) {
        lock.lock();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement del = conn.prepareStatement(deleteSql())) {
                    del.setString(1, pollerKey);
                    del.executeUpdate();
                }
                try (PreparedStatement ins = conn.prepareStatement(insertSql())) {
                    ins.setString(1, pollerKey);
                    ins.setString(2, serialize(watermark));
                    ins.executeUpdate();
                }
                conn.commit();
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
            }
        } catch (SQLException ex) {
            throw new DbPollerException("Failed to persist durable watermark for '" + pollerKey + "'", ex);
        } finally {
            lock.unlock();
        }
    }

    private String selectSql() {
        return "SELECT " + col("watermark_value") + " FROM " + quotedTable()
                + " WHERE " + col("poller_key") + " = ?";
    }

    private String deleteSql() {
        return "DELETE FROM " + quotedTable() + " WHERE " + col("poller_key") + " = ?";
    }

    private String insertSql() {
        return "INSERT INTO " + quotedTable() + " ("
                + col("poller_key") + ", " + col("watermark_value") + ", " + col("updated_at")
                + ") VALUES (?, ?, CURRENT_TIMESTAMP)";
    }

    private String quotedTable() {
        return dialect.quoteIdentifier(tableName);
    }

    private String col(String name) {
        return dialect.quoteIdentifier(name);
    }

    private static String serialize(Object watermark) {
        return watermark == null ? null : watermark.toString();
    }

    private Object parse(String raw) {
        if (raw == null) {
            return initialValue;
        }
        return switch (watermarkType) {
            case TIMESTAMP -> Instant.parse(raw);
            case BIGINT -> Long.parseLong(raw);
            case UUID -> UUID.fromString(raw);
            case STRING -> raw;
        };
    }
}
