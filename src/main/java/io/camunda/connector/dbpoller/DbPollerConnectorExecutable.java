package io.camunda.connector.dbpoller;

import io.camunda.connector.api.annotation.InboundConnector;
import io.camunda.connector.api.inbound.Activity;
import io.camunda.connector.api.inbound.Health;
import io.camunda.connector.api.inbound.InboundConnectorContext;
import io.camunda.connector.api.inbound.InboundConnectorExecutable;
import io.camunda.connector.api.inbound.Severity;
import io.camunda.connector.dbpoller.exception.DbPollerException;
import io.camunda.connector.dbpoller.service.DataSourceFactory;
import io.camunda.connector.dbpoller.service.InMemoryWatermarkStore;
import io.camunda.connector.dbpoller.service.JdbcWatermarkStore;
import io.camunda.connector.dbpoller.service.PollingService;
import io.camunda.connector.dbpoller.service.WatermarkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Inbound connector that polls a JDBC database on a fixed-delay schedule. */
@InboundConnector(
        name = "Database Polling Connector",
        type = DbPollerConnectorExecutable.TYPE
)
public class DbPollerConnectorExecutable implements InboundConnectorExecutable<InboundConnectorContext> {

    private static final Logger LOG = LoggerFactory.getLogger(DbPollerConnectorExecutable.class);

    /** Connector type identifier used in BPMN element templates. */
    public static final String TYPE = "io.camunda:camunda-db-poller:1";

    private InboundConnectorContext context;
    private DbPollerProperties properties;
    private DataSource dataSource;
    private WatermarkStore watermarkStore;
    private PollingService pollingService;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> pollHandle;

    /**
     * Activates the connector: binds and validates properties, initialises the
     * data source and watermark store, then starts the daemon polling thread.
     *
     * @param context the inbound connector context provided by the runtime
     * @throws DbPollerException if activation fails for any reason
     */
    @Override
    public void activate(InboundConnectorContext context) {
        this.context = context;
        this.properties = context.bindProperties(DbPollerProperties.class);
        this.properties.validate();

        LOG.info("Activating DB Poller: dialect={}, interval={}s, batchSize={}",
                properties.getDialect(),
                properties.getPollingIntervalSeconds(),
                properties.getBatchSize());

        try {
            dataSource = DataSourceFactory.build(properties);
            watermarkStore = buildWatermarkStore(properties, dataSource);
            pollingService = new PollingService(context, properties, dataSource, watermarkStore);

            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                var t = new Thread(r, "db-poller-" + safeProcessId(context));
                t.setDaemon(true);
                return t;
            });

            pollHandle = scheduler.scheduleWithFixedDelay(
                    this::safePoll,
                    0,
                    properties.getPollingIntervalSeconds(),
                    TimeUnit.SECONDS
            );

            context.reportHealth(Health.up());
            context.log(Activity.level(Severity.INFO)
                    .tag("activation")
                    .message("DB Poller activated successfully"));

        } catch (Exception ex) {
            LOG.error("Failed to activate DB Poller connector", ex);
            context.reportHealth(Health.down(ex));
            shutdownQuietly();
            throw new DbPollerException("Activation failed", ex);
        }
    }

    /**
     * Chooses the {@link WatermarkStore} implementation for the configured
     * {@link DbPollerProperties.ConsumptionStrategy} and {@link DbPollerProperties.WatermarkStorage}.
     *
     * <p>Returns an unused, never-written {@link InMemoryWatermarkStore} for
     * {@code UPDATE_FLAG}/{@code DELETE_AFTER_READ}, since those strategies track
     * consumption in the source table itself rather than via a watermark value.
     */
    private static WatermarkStore buildWatermarkStore(DbPollerProperties props, DataSource dataSource) {
        if (props.getConsumptionStrategy() != DbPollerProperties.ConsumptionStrategy.WATERMARK) {
            return new InMemoryWatermarkStore(null);
        }
        Object initial = props.parsedInitialWatermark();
        if (props.getWatermarkStorage() == DbPollerProperties.WatermarkStorage.JDBC_TABLE) {
            String pollerKey = props.getInstanceId() != null && !props.getInstanceId().isBlank()
                    ? props.getInstanceId()
                    : props.getPollingQueryHash();
            return new JdbcWatermarkStore(dataSource, props.getDialect(), props.getWatermarkTableName(),
                    pollerKey, props.getWatermarkType(), initial);
        }
        return new InMemoryWatermarkStore(initial);
    }

    /**
     * Deactivates the connector by cancelling the scheduler and releasing resources.
     */
    @Override
    public void deactivate() {
        LOG.info("Deactivating DB Poller connector");
        shutdownQuietly();
    }

    private void safePoll() {
        try {
            pollingService.pollOnce();
            context.reportHealth(Health.up());
        } catch (Exception ex) {
            LOG.error("Poll iteration failed", ex);
            context.log(Activity.level(Severity.ERROR)
                    .tag("poll-failure")
                    .message(ex.getMessage()));
            if (pollingService.consecutiveFailures() >= properties.getCircuitBreakerThreshold()) {
                context.reportHealth(Health.down(ex));
            }
        }
    }

    private static String safeProcessId(InboundConnectorContext ctx) {
        try {
            return ctx.getDefinition().elements().get(0).bpmnProcessId();
        } catch (Exception ex) {
            return "unknown-process";
        }
    }

    private void shutdownQuietly() {
        if (pollHandle != null) {
            pollHandle.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }
        if (dataSource instanceof AutoCloseable ac) {
            try {
                ac.close();
            } catch (Exception ignored) {
                // intentionally swallowed during shutdown
            }
        }
    }
}


