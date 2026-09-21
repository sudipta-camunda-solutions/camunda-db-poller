package io.camunda.connector.dbpoller;

import io.camunda.connector.dbpoller.dialect.DatabaseDialect;
import io.camunda.connector.dbpoller.exception.DbPollerException;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
    void databaseDialect_fromUrl_detectsOracle() {
        assertThat(DatabaseDialect.fromUrl("jdbc:oracle:thin:@localhost:1521:xe"))
                .isEqualTo(DatabaseDialect.ORACLE);
    }

    @Test
    void databaseDialect_fromUrl_detectsSqlServer() {
        assertThat(DatabaseDialect.fromUrl("jdbc:sqlserver://localhost:1433;databaseName=db"))
                .isEqualTo(DatabaseDialect.SQLSERVER);
    }

    @Test
    void databaseDialect_fromUrl_detectsSqlite() {
        assertThat(DatabaseDialect.fromUrl("jdbc:sqlite:test.db"))
                .isEqualTo(DatabaseDialect.SQLITE);
    }

    @Test
    void databaseDialect_fromUrl_throwsOnUnknown() {
        assertThatThrownBy(() -> DatabaseDialect.fromUrl("jdbc:db2://localhost:50000/mydb"))
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining("No dialect found");
    }

    @Test
    void databaseDialect_quoteIdentifier_usesSymmetricQuotesForPostgres() {
        assertThat(DatabaseDialect.POSTGRES.quoteIdentifier("my_col")).isEqualTo("\"my_col\"");
    }

    @Test
    void databaseDialect_quoteIdentifier_usesAsymmetricBracketsForSqlServer() {
        assertThat(DatabaseDialect.SQLSERVER.quoteIdentifier("my_col")).isEqualTo("[my_col]");
    }

    @Test
    void databaseDialect_paginationClause_oracleUsesFetchFirst() {
        assertThat(DatabaseDialect.ORACLE.paginationClause(50))
                .isEqualTo(" FETCH FIRST 50 ROWS ONLY");
    }

    @Test
    void databaseDialect_paginationClause_sqlServerUsesOffsetFetch() {
        assertThat(DatabaseDialect.SQLSERVER.paginationClause(50))
                .isEqualTo(" OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY");
    }

    @Test
    void databaseDialect_paginationClause_sqliteUsesLimit() {
        assertThat(DatabaseDialect.SQLITE.paginationClause(50)).isEqualTo(" LIMIT 50");
    }

    @Test
    void databaseDialect_trueLiteral_ansiDialectsUseTrueKeyword() {
        assertThat(DatabaseDialect.POSTGRES.trueLiteral()).isEqualTo("TRUE");
        assertThat(DatabaseDialect.MYSQL.trueLiteral()).isEqualTo("TRUE");
        assertThat(DatabaseDialect.H2.trueLiteral()).isEqualTo("TRUE");
        assertThat(DatabaseDialect.SQLITE.trueLiteral()).isEqualTo("TRUE");
    }

    @Test
    void databaseDialect_trueLiteral_sqlServerAndOracleUseNumericOne() {
        assertThat(DatabaseDialect.SQLSERVER.trueLiteral()).isEqualTo("1");
        assertThat(DatabaseDialect.ORACLE.trueLiteral()).isEqualTo("1");
    }

    @Test
    void getPollingQueryHash_isStableAndTwelveChars() {
        var props = baseProps();
        String hash1 = props.getPollingQueryHash();
        String hash2 = props.getPollingQueryHash();

        assertThat(hash1).hasSize(12);
        assertThat(hash1).isEqualTo(hash2);
    }

    // -------------------------------------------------------------------------
    // Consumption strategy
    // -------------------------------------------------------------------------

    @Test
    void validate_defaultStrategyStillRequiresWatermarkColumn() {
        var props = baseProps();
        props.setWatermarkColumn(null);

        assertThatThrownBy(props::validate)
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining("watermarkColumn is required");
    }

    @Test
    void validate_updateFlagStrategy_doesNotRequireWatermarkColumnOrPlaceholder() {
        var props = baseProps();
        props.setWatermarkColumn(null);
        props.setPollingQuery("SELECT * FROM orders WHERE processed = FALSE");
        props.setConsumptionStrategy(DbPollerProperties.ConsumptionStrategy.UPDATE_FLAG);
        props.setTargetTable("orders");
        props.setKeyColumn("id");
        props.setFlagColumn("processed");

        assertThatCode(props::validate).doesNotThrowAnyException();
    }

    @Test
    void validate_updateFlagStrategy_requiresFlagColumn() {
        var props = baseProps();
        props.setPollingQuery("SELECT * FROM orders WHERE processed = FALSE");
        props.setConsumptionStrategy(DbPollerProperties.ConsumptionStrategy.UPDATE_FLAG);
        props.setTargetTable("orders");
        props.setKeyColumn("id");

        assertThatThrownBy(props::validate)
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining("flagColumn is required");
    }

    @Test
    void validate_deleteAfterReadStrategy_requiresTargetTableAndKeyColumn() {
        var props = baseProps();
        props.setPollingQuery("SELECT * FROM orders");
        props.setConsumptionStrategy(DbPollerProperties.ConsumptionStrategy.DELETE_AFTER_READ);

        assertThatThrownBy(props::validate)
                .isInstanceOf(DbPollerException.class)
                .hasMessageContaining("targetTable is required");
    }

    @Test
    void validate_deleteAfterReadStrategy_doesNotRequireFlagColumn() {
        var props = baseProps();
        props.setPollingQuery("SELECT * FROM orders");
        props.setConsumptionStrategy(DbPollerProperties.ConsumptionStrategy.DELETE_AFTER_READ);
        props.setTargetTable("orders");
        props.setKeyColumn("id");

        assertThatCode(props::validate).doesNotThrowAnyException();
    }

    @Test
    void requiresWriteAccess_falseForDefaultWatermarkInMemory() {
        var props = baseProps();
        assertThat(props.requiresWriteAccess()).isFalse();
    }

    @Test
    void requiresWriteAccess_trueForUpdateFlag() {
        var props = baseProps();
        props.setConsumptionStrategy(DbPollerProperties.ConsumptionStrategy.UPDATE_FLAG);
        assertThat(props.requiresWriteAccess()).isTrue();
    }

    @Test
    void requiresWriteAccess_trueForDeleteAfterRead() {
        var props = baseProps();
        props.setConsumptionStrategy(DbPollerProperties.ConsumptionStrategy.DELETE_AFTER_READ);
        assertThat(props.requiresWriteAccess()).isTrue();
    }

    @Test
    void requiresWriteAccess_trueForDurableWatermarkStorage() {
        var props = baseProps();
        props.setWatermarkStorage(DbPollerProperties.WatermarkStorage.JDBC_TABLE);
        assertThat(props.requiresWriteAccess()).isTrue();
    }
}

