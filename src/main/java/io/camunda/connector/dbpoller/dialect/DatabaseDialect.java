package io.camunda.connector.dbpoller.dialect;

import io.camunda.connector.dbpoller.exception.DbPollerException;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum DatabaseDialect {

    POSTGRES(
            "jdbc:postgresql:",
            "org.postgresql.Driver",
            "SELECT 1",
            "\"",
            batchSize -> " LIMIT " + batchSize
    ),
    MYSQL(
            "jdbc:mysql:",
            "com.mysql.cj.jdbc.Driver",
            "SELECT 1",
            "`",
            batchSize -> " LIMIT " + batchSize
    ),
    H2(
            "jdbc:h2:",
            "org.h2.Driver",
            "SELECT 1",
            "\"",
            batchSize -> " LIMIT " + batchSize
    );

    @FunctionalInterface
    public interface PaginationBuilder {
        String build(int batchSize);
    }

    private final String urlPrefix;
    private final String driverClass;
    private final String validationQuery;
    private final String identifierQuote;
    private final PaginationBuilder paginationBuilder;

    DatabaseDialect(String urlPrefix, String driverClass, String validationQuery,
                    String identifierQuote, PaginationBuilder paginationBuilder) {
        this.urlPrefix = urlPrefix;
        this.driverClass = driverClass;
        this.validationQuery = validationQuery;
        this.identifierQuote = identifierQuote;
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

    public String quoteIdentifier(String identifier) {
        String stripped = identifier.replace(identifierQuote, "");
        return identifierQuote + stripped + identifierQuote;
    }

    public String paginationClause(int batchSize) {
        return paginationBuilder.build(batchSize);
    }
}

