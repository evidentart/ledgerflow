package com.ledgerflow.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
            adminClient.describeCluster().nodes().get(probeTimeout.toMillis(), TimeUnit.MILLISECONDS);
            return Health.up().build();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return Health.down().withDetail("reason", "Kafka connectivity check was interrupted").build();
        } catch (Exception ex) {
            return Health.down().withDetail("reason", "Kafka broker is unavailable").build();
        } finally {
            if (adminClient != null) {
                try {
                    adminClient.close(CLIENT_CLOSE_TIMEOUT);
                } catch (RuntimeException ignored) {
                    // Closing the probe client must not replace the connectivity result.
                }
            }
        }
    }
}
