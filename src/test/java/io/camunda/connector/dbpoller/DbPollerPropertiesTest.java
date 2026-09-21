package io.camunda.connector.dbpoller;

import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import io.camunda.connector.dbpoller.exception.DbPollerException;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DbPollerPropertiesTest {

    private static DbPollerProperties baseProps() {
        var props = new DbPollerProperties();
        props.setJdbcUrl("jdbc:postgresql://localhost:5432/test");
        props.setUsername("u");
        props.setPassword("p");
        props.setWatermarkColumn("updated_at");
        props.setPollingQuery("SELECT * FROM t WHERE updated_at > :lastWatermark ORDER BY updated_at");
        return props;
    }

    @Test
    void validate_rejectsMissingJdbcUrl() {
        var props = baseProps();
        props.setJdbcUrl(null);

        assertThatThrownBy(props::validate)
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining("jdbcUrl is required");
    }

    @Test
    void validate_rejectsMissingUsername() {
        var props = baseProps();
        props.setUsername("");

        assertThatThrownBy(props::validate)
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining("username is required");
    }

    @Test
    void validate_rejectsPollingQueryWithoutWatermarkPlaceholder() {
        var props = baseProps();
        props.setPollingQuery("SELECT * FROM t ORDER BY id");

        assertThatThrownBy(props::validate)
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining(":lastWatermark");
    }

    @Test
    void parsedInitialWatermark_parsesTimestamp() {
        var props = baseProps();
        props.setWatermarkType(DbPollerProperties.WatermarkType.TIMESTAMP);
        props.setInitialWatermark("2024-01-15T10:30:00Z");

        assertThat(props.parsedInitialWatermark())
                .isEqualTo(Instant.parse("2024-01-15T10:30:00Z"));
    }

    @Test
    void parsedInitialWatermark_parsesBigint() {
        var props = baseProps();
        props.setWatermarkType(DbPollerProperties.WatermarkType.BIGINT);
        props.setInitialWatermark("12345");

        assertThat(props.parsedInitialWatermark()).isEqualTo(12345L);
    }

    @Test
    void parsedInitialWatermark_passesStringThrough() {
        var props = baseProps();
        props.setWatermarkType(DbPollerProperties.WatermarkType.STRING);
        props.setInitialWatermark("hello");

        assertThat(props.parsedInitialWatermark()).isEqualTo("hello");
    }

    @Test
    void parsedInitialWatermark_throwsOnBadBigint() {
        var props = baseProps();
        props.setWatermarkType(DbPollerProperties.WatermarkType.BIGINT);
        props.setInitialWatermark("abc");

        assertThatThrownBy(props::parsedInitialWatermark)
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining("BIGINT");
    }

    @Test
    void databaseDialect_fromUrl_detectsPostgres() {
        assertThat(DatabaseDialect.fromUrl("jdbc:postgresql://localhost/db"))
                .isEqualTo(DatabaseDialect.POSTGRES);
    }

    @Test
    void databaseDialect_fromUrl_detectsMysql() {
        assertThat(DatabaseDialect.fromUrl("jdbc:mysql://localhost/db"))
                .isEqualTo(DatabaseDialect.MYSQL);
    }

    @Test
    void databaseDialect_fromUrl_detectsH2() {
        assertThat(DatabaseDialect.fromUrl("jdbc:h2:mem:testdb"))
                .isEqualTo(DatabaseDialect.H2);
    }

    @Test
    void databaseDialect_fromUrl_throwsOnUnknown() {
        assertThatThrownBy(() -> DatabaseDialect.fromUrl("jdbc:oracle:thin:@localhost:1521:xe"))
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining("No dialect found");
    }

    @Test
    void getPollingQueryHash_isStableAndTwelveChars() {
        var props = baseProps();
        String hash1 = props.getPollingQueryHash();
        String hash2 = props.getPollingQueryHash();

        assertThat(hash1).hasSize(12);
        assertThat(hash1).isEqualTo(hash2);
    }
}

