package io.camunda.connector.dbpoller.service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.camunda.connector.dbpoller.DbPollerProperties;
import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import io.camunda.connector.dbpoller.exception.DbPollerException;

import javax.sql.DataSource;

/** Factory that builds a configured {@link HikariDataSource} from connector properties. */
public final class DataSourceFactory {

    private DataSourceFactory() {}

    /**
     * Builds a read-only {@link DataSource} pool from the supplied properties.
     *
     * @param props validated connector properties
     * @return a fully initialised {@link HikariDataSource}
     * @throws DbPollerException if the dialect cannot be resolved or the pool cannot start
     */
    public static DataSource build(DbPollerProperties props) {
        DatabaseDialect dialect = props.getDialect();
        if (dialect == null) {
            throw new DbPollerException(
                    "Dialect could not be resolved; set jdbcUrl or dialect explicitly");
        }

        try {
            var cfg = new HikariConfig();
            cfg.setJdbcUrl(props.getJdbcUrl());
            cfg.setUsername(props.getUsername());
            cfg.setPassword(props.getPassword());
            cfg.setDriverClassName(dialect.driverClass());
            cfg.setMaximumPoolSize(props.getPoolMaxSize());
            cfg.setMinimumIdle(0);
            cfg.setConnectionTimeout(props.getConnectionTimeoutMs());
            cfg.setConnectionTestQuery(dialect.validationQuery());
            cfg.setPoolName("db-poller-" + props.getPollingQueryHash());
            // Read-only unless the configured consumption strategy or watermark
            // storage needs to UPDATE/DELETE/INSERT (see DbPollerProperties#requiresWriteAccess).
            cfg.setReadOnly(!props.requiresWriteAccess());
            cfg.setAutoCommit(true);

            long idleTimeout = Math.max(
                    30_000L,
                    (long) props.getPollingIntervalSeconds() * 1_000 + 10_000);
            cfg.setIdleTimeout(idleTimeout);

            return new HikariDataSource(cfg);
        } catch (RuntimeException ex) {
            throw new DbPollerException("Failed to initialize HikariCP pool", ex);
        }
    }
}

