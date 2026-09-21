package io.camunda.connector.dbpoller.dialect;

import io.camunda.connector.dbpoller.exception.DbPollerException;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum DatabaseDialect {

    POSTGRES(
            "jdbc:postgresql:",
            "org.postgresql.Driver",
            "SELECT 1",
            "\"", "\"",
            "TRUE",
            batchSize -> " LIMIT " + batchSize
    ),
    MYSQL(
            "jdbc:mysql:",
            "com.mysql.cj.jdbc.Driver",
            "SELECT 1",
            "`", "`",
            "TRUE",
            batchSize -> " LIMIT " + batchSize
    ),
    H2(
            "jdbc:h2:",
            "org.h2.Driver",
            "SELECT 1",
            "\"", "\"",
            "TRUE",
            batchSize -> " LIMIT " + batchSize
    ),
    // Oracle has no native SQL boolean literal before 23c; UPDATE_FLAG's Flag
    // Column should be a NUMBER(1)-style column on Oracle, set to 1/0.
    ORACLE(
            "jdbc:oracle:",
            "oracle.jdbc.OracleDriver",
            "SELECT 1 FROM DUAL",
            "\"", "\"",
            "1",
            batchSize -> " FETCH FIRST " + batchSize + " ROWS ONLY"
    ),
    // SQL Server's T-SQL has no bare TRUE/FALSE literal; BIT columns use 1/0.
    SQLSERVER(
            "jdbc:sqlserver:",
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "SELECT 1",
            "[", "]",
            "1",
            // Requires an ORDER BY in the polling query; SQL Server rejects
            // OFFSET/FETCH without one.
            batchSize -> " OFFSET 0 ROWS FETCH NEXT " + batchSize + " ROWS ONLY"
    ),
    SQLITE(
            "jdbc:sqlite:",
            "org.sqlite.JDBC",
            "SELECT 1",
            "\"", "\"",
            "TRUE",
            batchSize -> " LIMIT " + batchSize
    );

    @FunctionalInterface
    public interface PaginationBuilder {
        String build(int batchSize);
    }

    private final String urlPrefix;
    private final String driverClass;
    private final String validationQuery;
    private final String openQuote;
    private final String closeQuote;
    private final String trueLiteral;
    private final PaginationBuilder paginationBuilder;

    DatabaseDialect(String urlPrefix, String driverClass, String validationQuery,
                    String openQuote, String closeQuote, String trueLiteral,
                    PaginationBuilder paginationBuilder) {
        this.urlPrefix = urlPrefix;
        this.driverClass = driverClass;
        this.validationQuery = validationQuery;
        this.openQuote = openQuote;
        this.closeQuote = closeQuote;
        this.trueLiteral = trueLiteral;
        this.paginationBuilder = paginationBuilder;
    }

    public static DatabaseDialect fromUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            throw new DbPollerException("jdbcUrl must not be null");
        }
        String lower = jdbcUrl.toLowerCase();
        for (DatabaseDialect dialect : values()) {
            if (lower.startsWith(dialect.urlPrefix)) {
                return dialect;
            }
        }
        String supported = Arrays.stream(values())
                .map(d -> d.urlPrefix)
                .collect(Collectors.joining(", "));
        throw new DbPollerException(
                "No dialect found for jdbcUrl '" + jdbcUrl + "'. Supported prefixes: " + supported);
    }

    public String driverClass() {
        return driverClass;
    }

    public String validationQuery() {
        return validationQuery;
    }

    /** Quotes a SQL identifier (table/column name) using this dialect's quoting style. */
    public String quoteIdentifier(String identifier) {
        String stripped = identifier.replace(openQuote, "").replace(closeQuote, "");
        return openQuote + stripped + closeQuote;
    }

    public String paginationClause(int batchSize) {
        return paginationBuilder.build(batchSize);
    }

    /** The literal this dialect accepts for "true" in a SQL expression (e.g. an UPDATE SET clause). */
    public String trueLiteral() {
        return trueLiteral;
    }
}
