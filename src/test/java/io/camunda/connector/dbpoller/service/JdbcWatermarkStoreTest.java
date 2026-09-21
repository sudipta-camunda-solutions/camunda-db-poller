package io.camunda.connector.dbpoller.service;

import io.camunda.connector.dbpoller.DbPollerProperties.WatermarkType;
import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcWatermarkStoreTest {

    private static final String TABLE = "camunda_db_poller_watermark";

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        var ds = new JdbcDataSource();
        // Unique per-test DB name so tests don't see each other's tables/rows.
        ds.setURL("jdbc:h2:mem:watermark-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        this.dataSource = ds;

        // Quoted, case-preserved identifiers: JdbcWatermarkStore always quotes via
        // DatabaseDialect#quoteIdentifier, so the table must be created the same way.
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE \"" + TABLE + "\" ("
                    + "\"poller_key\" VARCHAR(255) PRIMARY KEY, "
                    + "\"watermark_value\" VARCHAR(255) NOT NULL, "
                    + "\"updated_at\" TIMESTAMP NOT NULL)");
        }
    }

    private JdbcWatermarkStore store(String pollerKey, WatermarkType type, Object initial) {
        return new JdbcWatermarkStore(dataSource, DatabaseDialect.H2, TABLE, pollerKey, type, initial);
    }

    @Test
    void read_returnsInitialValue_whenNoRowExists() {
        var store = store("poller-a", WatermarkType.TIMESTAMP, Instant.EPOCH);

        assertThat(store.read()).isEqualTo(Instant.EPOCH);
    }

    @Test
    void write_thenRead_roundTripsTimestamp() {
        var store = store("poller-a", WatermarkType.TIMESTAMP, Instant.EPOCH);
        Instant value = Instant.parse("2024-01-15T10:30:00Z");

        store.write(value);

        assertThat(store.read()).isEqualTo(value);
    }

    @Test
    void write_overwritesPreviousValue() {
        var store = store("poller-a", WatermarkType.BIGINT, 0L);

        store.write(10L);
        store.write(25L);

        assertThat(store.read()).isEqualTo(25L);
    }

    @Test
    void differentPollerKeys_areIsolated() {
        var storeA = store("poller-a", WatermarkType.BIGINT, 0L);
        var storeB = store("poller-b", WatermarkType.BIGINT, 0L);

        storeA.write(100L);

        assertThat(storeA.read()).isEqualTo(100L);
        assertThat(storeB.read()).isEqualTo(0L);
    }

    @Test
    void write_thenRead_roundTripsUuid() {
        var store = store("poller-a", WatermarkType.UUID, new UUID(0L, 0L));
        UUID value = UUID.randomUUID();

        store.write(value);

        assertThat(store.read()).isEqualTo(value);
    }
}
