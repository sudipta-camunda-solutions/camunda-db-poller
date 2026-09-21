package io.camunda.connector.dbpoller.service;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import io.camunda.connector.api.inbound.InboundConnectorContext;
import io.camunda.connector.dbpoller.DbPollerProperties;
import io.camunda.connector.dbpoller.DbPollerProperties.ConsumptionStrategy;
import io.camunda.connector.dbpoller.DbPollerProperties.WatermarkType;
import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Exercises the real {@code com.microsoft.sqlserver.jdbc} driver against a
 * containerized SQL Server, since dialect-specific SQL (bracket quoting,
 * OFFSET/FETCH pagination, boolean literal support) can't be trusted without
 * a real server.
 */
@Testcontainers(disabledWithoutDocker = true)
class SqlServerDialectIntegrationTest {

    @Container
    static MSSQLServerContainer<?> SQLSERVER = new MSSQLServerContainer<>(
            "mcr.microsoft.com/mssql/server:2022-latest").acceptLicense();

    private DataSource dataSource;
    private InboundConnectorContext context;

    @BeforeEach
    void setUp() throws Exception {
        var ds = new SQLServerDataSource();
        ds.setURL(SQLSERVER.getJdbcUrl());
        ds.setUser(SQLSERVER.getUsername());
        ds.setPassword(SQLSERVER.getPassword());
        this.dataSource = ds;
        this.context = mock(InboundConnectorContext.class);

        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("IF OBJECT_ID('dbo.orders', 'U') IS NOT NULL DROP TABLE dbo.orders");
            st.execute("CREATE TABLE dbo.orders ("
                    + "id INT PRIMARY KEY, "
                    + "seq BIGINT, "
                    + "status VARCHAR(50), "
                    + "processed BIT DEFAULT 0)");
            st.execute("INSERT INTO dbo.orders VALUES (1, 1, 'PENDING', 0)");
            st.execute("INSERT INTO dbo.orders VALUES (2, 2, 'PENDING', 0)");
        }
    }

    @Test
    void dialect_autoDetectedFromContainerUrl() {
        assertThat(DatabaseDialect.fromUrl(SQLSERVER.getJdbcUrl())).isEqualTo(DatabaseDialect.SQLSERVER);
    }

    @Test
    void watermarkStrategy_pollsAndAdvancesWatermark_usingOffsetFetchPagination() {
        var props = new DbPollerProperties();
        props.setDialect(DatabaseDialect.SQLSERVER);
        props.setBatchSize(100);
        props.setQueryTimeoutSeconds(10);
        props.setPollingQuery("SELECT * FROM dbo.orders WHERE seq > :lastWatermark ORDER BY seq");
        props.setWatermarkColumn("seq");
        props.setWatermarkType(WatermarkType.BIGINT);

        var watermarkStore = new InMemoryWatermarkStore(0L);
        var service = new PollingService(context, props, dataSource, watermarkStore);

        service.pollOnce();

        verify(context, times(2)).correlateWithResult(any());
        assertThat(watermarkStore.read()).isEqualTo(2L);

        service.pollOnce();
        verify(context, times(2)).correlateWithResult(any());
    }

    @Test
    void updateFlagStrategy_flipsBitColumn_usingBracketQuoting() throws Exception {
        var props = new DbPollerProperties();
        props.setDialect(DatabaseDialect.SQLSERVER);
        props.setBatchSize(100);
        props.setQueryTimeoutSeconds(10);
        props.setConsumptionStrategy(ConsumptionStrategy.UPDATE_FLAG);
        props.setPollingQuery("SELECT * FROM dbo.orders WHERE processed = 0");
        props.setTargetTable("orders");
        props.setKeyColumn("id");
        props.setFlagColumn("processed");

        var service = new PollingService(context, props, dataSource, new InMemoryWatermarkStore(null));

        service.pollOnce();

        verify(context, times(2)).correlateWithResult(any());
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM dbo.orders WHERE processed = 1")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(2);
        }
    }
}
