package io.camunda.connector.dbpoller.service;

import io.camunda.connector.api.inbound.InboundConnectorContext;
import io.camunda.connector.dbpoller.DbPollerProperties;
import io.camunda.connector.dbpoller.DbPollerProperties.ConsumptionStrategy;
import io.camunda.connector.dbpoller.DbPollerProperties.WatermarkType;
import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Exercises the real {@code org.sqlite.JDBC} driver end-to-end (not mocked),
 * since SQLite needs no server to run against.
 */
class SqliteDialectIntegrationTest {

    private File dbFile;
    private DataSource dataSource;
    private InboundConnectorContext context;

    @BeforeEach
    void setUp() throws Exception {
        dbFile = File.createTempFile("db-poller-sqlite-test-", ".db");
        var ds = new SQLiteDataSource();
        ds.setUrl("jdbc:sqlite:" + dbFile.getAbsolutePath());
        this.dataSource = ds;
        this.context = mock(InboundConnectorContext.class);

        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE orders ("
                    + "id INTEGER PRIMARY KEY, "
                    + "seq INTEGER, "
                    + "status TEXT, "
                    + "processed BOOLEAN DEFAULT FALSE)");
            st.execute("INSERT INTO orders VALUES (1, 1, 'PENDING', FALSE)");
            st.execute("INSERT INTO orders VALUES (2, 2, 'PENDING', FALSE)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        Files.deleteIfExists(dbFile.toPath());
    }

    @Test
    void dialect_autoDetectedFromUrl() {
        assertThat(DatabaseDialect.fromUrl("jdbc:sqlite:" + dbFile.getAbsolutePath()))
                .isEqualTo(DatabaseDialect.SQLITE);
    }

    @Test
    void watermarkStrategy_pollsAndAdvancesWatermark_againstRealSqliteFile() {
        var props = new DbPollerProperties();
        props.setDialect(DatabaseDialect.SQLITE);
        props.setBatchSize(100);
        props.setQueryTimeoutSeconds(5);
        props.setPollingQuery("SELECT * FROM orders WHERE seq > :lastWatermark ORDER BY seq");
        props.setWatermarkColumn("seq");
        props.setWatermarkType(WatermarkType.BIGINT);

        var watermarkStore = new InMemoryWatermarkStore(0L);
        var service = new PollingService(context, props, dataSource, watermarkStore);

        service.pollOnce();

        verify(context, times(2)).correlateWithResult(any());
        assertThat(watermarkStore.read()).isEqualTo(2L);

        service.pollOnce();
        verify(context, times(2)).correlateWithResult(any()); // no new rows on second poll
    }

    @Test
    void deleteAfterReadStrategy_deletesRows_againstRealSqliteFile() throws Exception {
        var props = new DbPollerProperties();
        props.setDialect(DatabaseDialect.SQLITE);
        props.setBatchSize(100);
        props.setQueryTimeoutSeconds(5);
        props.setConsumptionStrategy(ConsumptionStrategy.DELETE_AFTER_READ);
        props.setPollingQuery("SELECT * FROM orders");
        props.setTargetTable("orders");
        props.setKeyColumn("id");

        var service = new PollingService(context, props, dataSource, new InMemoryWatermarkStore(null));

        service.pollOnce();

        verify(context, times(2)).correlateWithResult(any());
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM orders")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(0);
        }
    }
}
