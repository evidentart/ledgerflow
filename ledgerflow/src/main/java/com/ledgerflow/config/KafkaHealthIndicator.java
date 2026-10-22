package com.ledgerflow.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/** Reports whether the configured Kafka cluster can be reached. */
@Component
@ConditionalOnProperty(name = "ledgerflow.events.transport", havingValue = "kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(KafkaHealthIndicator.class);
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration CLIENT_CLOSE_TIMEOUT = Duration.ofMillis(100);

    private final Supplier<AdminClient> adminClientSupplier;
    private final Duration probeTimeout;

    @Autowired
    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this(() -> {
            Map<String, Object> properties = new HashMap<>(kafkaAdmin.getConfigurationProperties());
            properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) PROBE_TIMEOUT.toMillis());
            properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) PROBE_TIMEOUT.toMillis());
            return AdminClient.create(properties);
        }, PROBE_TIMEOUT);
    }

    KafkaHealthIndicator(Supplier<AdminClient> adminClientSupplier, Duration probeTimeout) {
        this.adminClientSupplier = adminClientSupplier;
        this.probeTimeout = probeTimeout;
    }

    @Override
    public Health health() {
        AdminClient adminClient = null;
        try {
            adminClient = adminClientSupplier.get();
            DescribeClusterResult cluster = adminClient.describeCluster();
            var clusterIdFuture = cluster.clusterId();
            var nodesFuture = cluster.nodes();
            KafkaFuture.allOf(clusterIdFuture, nodesFuture)
                    .get(probeTimeout.toMillis(), TimeUnit.MILLISECONDS);
            String clusterId = clusterIdFuture.get();
            if (clusterId == null) {
                log.warn("Kafka health probe returned no cluster ID");
                return Health.down().withDetail("reason", "Kafka cluster ID is unavailable").build();
            }
            int nodeCount = nodesFuture.get().size();
            return Health.up()
                    .withDetail("clusterId", clusterId)
                    .withDetail("nodes", nodeCount)
                    .build();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("Kafka health probe was interrupted", ex);
            return Health.down().withDetail("reason", "Kafka connectivity check was interrupted").build();
        } catch (Exception ex) {
            log.warn("Kafka health probe failed", ex);
            return Health.down().withDetail("reason", "Kafka broker is unavailable").build();
        } finally {
            if (adminClient != null) {
                try {
                    adminClient.close(CLIENT_CLOSE_TIMEOUT);
                } catch (RuntimeException ex) {
                    log.warn("Failed to close Kafka health probe client", ex);
                }
            }
        }
    }
}
