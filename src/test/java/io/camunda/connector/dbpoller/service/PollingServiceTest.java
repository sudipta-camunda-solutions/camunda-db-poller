package io.camunda.connector.dbpoller.service;

import io.camunda.connector.api.inbound.InboundConnectorContext;
import io.camunda.connector.dbpoller.DbPollerProperties;
import io.camunda.connector.dbpoller.DbPollerProperties.ConsumptionStrategy;
import io.camunda.connector.dbpoller.DbPollerProperties.WatermarkType;
import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PollingServiceTest {

    private DataSource dataSource;
    private InboundConnectorContext context;

    @BeforeEach
    void setUp() throws Exception {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:polling-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        this.dataSource = ds;
        this.context = mock(InboundConnectorContext.class);

        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE \"orders\" ("
                    + "\"id\" INT PRIMARY KEY, "
                    + "\"status\" VARCHAR(50), "
                    + "\"processed\" BOOLEAN DEFAULT FALSE)");
            st.execute("INSERT INTO \"orders\" VALUES (1, 'PENDING', FALSE)");
            st.execute("INSERT INTO \"orders\" VALUES (2, 'PENDING', FALSE)");
        }
    }

    private DbPollerProperties baseProps() {
        var props = new DbPollerProperties();
        props.setJdbcUrl("jdbc:h2:mem:unused");
        props.setUsername("sa");
        props.setPassword("");
        props.setDialect(DatabaseDialect.H2);
        props.setBatchSize(100);
        props.setQueryTimeoutSeconds(5);
        return props;
    }

    @Test
    void updateFlagStrategy_flipsFlagAfterCorrelation_andRowsAreNotRedelivered() throws Exception {
        var props = baseProps();
        props.setConsumptionStrategy(ConsumptionStrategy.UPDATE_FLAG);
        props.setPollingQuery("SELECT * FROM \"orders\" WHERE \"processed\" = FALSE");
        props.setTargetTable("orders");
        props.setKeyColumn("id");
        props.setFlagColumn("processed");

        var service = new PollingService(context, props, dataSource, new InMemoryWatermarkStore(null));

        service.pollOnce();
        verify(context, times(2)).correlateWithResult(any());

        // Second poll must find no un-flagged rows left.
        service.pollOnce();
        verify(context, times(2)).correlateWithResult(any());

        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM \"orders\" WHERE \"processed\" = TRUE")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(2);
        }
    }

    @Test
    void deleteAfterReadStrategy_deletesRowsAfterCorrelation() throws Exception {
        var props = baseProps();
        props.setConsumptionStrategy(ConsumptionStrategy.DELETE_AFTER_READ);
        props.setPollingQuery("SELECT * FROM \"orders\"");
        props.setTargetTable("orders");
        props.setKeyColumn("id");

        var service = new PollingService(context, props, dataSource, new InMemoryWatermarkStore(null));

        service.pollOnce();
        verify(context, times(2)).correlateWithResult(any());

        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM \"orders\"")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(0);
        }
    }

    @Test
    void watermarkStrategy_advancesWatermarkAndDoesNotRedeliverRows() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("ALTER TABLE \"orders\" ADD COLUMN \"seq\" BIGINT");
            st.execute("UPDATE \"orders\" SET \"seq\" = \"id\"");
        }

        var props = baseProps();
        props.setPollingQuery("SELECT * FROM \"orders\" WHERE \"seq\" > :lastWatermark ORDER BY \"seq\"");
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
}
