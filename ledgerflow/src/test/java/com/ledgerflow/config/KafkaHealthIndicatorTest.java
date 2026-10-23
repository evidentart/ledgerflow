package com.ledgerflow.config;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration;
import org.springframework.boot.actuate.health.HealthContributorRegistry;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(OutputCaptureExtension.class)
class KafkaHealthIndicatorTest {

    private final AdminClient adminClient = mock(AdminClient.class);
    private final DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
    private final KafkaFutureImpl<String> clusterIdFuture = new KafkaFutureImpl<>();
    private final KafkaFutureImpl<Collection<Node>> nodesFuture = new KafkaFutureImpl<>();

    private KafkaHealthIndicator indicator(Duration timeout) {
        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.clusterId()).thenReturn(clusterIdFuture);
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);
        return new KafkaHealthIndicator(() -> adminClient, timeout);
    }

    @Test
    void reportsUpWhenKafkaCanBeContacted() throws Exception {
        Map<String, Object> existingProperties = new HashMap<>();
        existingProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "configured-broker:9092");
        existingProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, "ledgerflow-admin");
        existingProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        existingProperties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        KafkaAdmin kafkaAdmin = new KafkaAdmin(existingProperties);

        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.clusterId()).thenReturn(clusterIdFuture);
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);
        clusterIdFuture.complete("cluster-123");
        nodesFuture.complete(List.of(mock(Node.class), mock(Node.class)));

        try (MockedStatic<AdminClient> adminClients = mockStatic(AdminClient.class)) {
            adminClients.when(() -> AdminClient.create(anyMap())).thenReturn(adminClient);

            var health = new KafkaHealthIndicator(kafkaAdmin).health();

            assertThat(health.getStatus().getCode()).isEqualTo("UP");
            assertThat(health.getDetails())
                    .containsEntry("clusterId", "cluster-123")
                    .containsEntry("nodes", 2);
            verify(adminClient).describeCluster();
            verify(describeClusterResult).clusterId();
            verify(describeClusterResult).nodes();

            ArgumentCaptor<Map<String, Object>> properties = ArgumentCaptor.forClass(Map.class);
            adminClients.verify(() -> AdminClient.create(properties.capture()));
            assertThat(properties.getValue())
                    .containsEntry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "configured-broker:9092")
                    .containsEntry(AdminClientConfig.CLIENT_ID_CONFIG, "ledgerflow-admin")
                    .containsEntry(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000)
                    .containsEntry(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2_000);
        }

        verify(adminClient).close(Duration.ofMillis(100));
    }

    @Test
    void reportsDownWhenClusterIdIsMissing(CapturedOutput output) {
        clusterIdFuture.complete(null);
        nodesFuture.complete(List.of(mock(Node.class)));

        var health = indicator(Duration.ofSeconds(1)).health();

        assertThat(health.getStatus().getCode()).isEqualTo("DOWN");
        assertThat(health.getDetails())
                .containsOnlyKeys("reason")
                .containsEntry("reason", "Kafka cluster ID is unavailable");
        assertThat(output).contains("Kafka health probe returned no cluster ID");
        verify(adminClient).close(Duration.ofMillis(100));
    }

    @Test
    void reportsDownWhenBrokerConnectionFails(CapturedOutput output) {
        clusterIdFuture.complete("cluster-123");
        nodesFuture.completeExceptionally(new IllegalStateException("sensitive broker details"));

        var health = indicator(Duration.ofSeconds(1)).health();

        assertThat(health.getStatus().getCode()).isEqualTo("DOWN");
        assertThat(health.getDetails()).containsEntry("reason", "Kafka broker is unavailable");
        assertThat(health.toString()).doesNotContain("sensitive broker details");
        assertThat(output).contains("Kafka health probe failed: ", "sensitive broker details")
                .doesNotContain("\tat ");
        verify(adminClient).close(Duration.ofMillis(100));
    }

    @Test
    void reportsDownWhenConnectivityProbeTimesOutWithinItsBound() {
        Duration timeout = Duration.ofMillis(75);
        clusterIdFuture.complete("cluster-123");

        long startedAt = System.nanoTime();
        var health = indicator(timeout).health();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        assertThat(health.getStatus().getCode()).isEqualTo("DOWN");
        assertThat(elapsedMillis).isLessThan(1_000);
        verify(adminClient).close(Duration.ofMillis(100));
    }

    @Test
    void restoresInterruptAndClosesClient(CapturedOutput output) {
        clusterIdFuture.complete("cluster-123");
        var health = indicator(Duration.ofSeconds(1));

        try {
            Thread.currentThread().interrupt();
            var result = health.health();

            assertThat(result.getStatus().getCode()).isEqualTo("DOWN");
            assertThat(result.getDetails())
                    .containsEntry("reason", "Kafka connectivity check was interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(output).contains("Kafka health probe was interrupted");
            verify(adminClient).close(Duration.ofMillis(100));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void registersKafkaHealthContributorOnlyForKafkaTransport() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        HealthContributorAutoConfiguration.class,
                        HealthEndpointAutoConfiguration.class))
                .withUserConfiguration(IndicatorConfiguration.class, KafkaHealthIndicator.class)
                .withPropertyValues("ledgerflow.events.transport=kafka")
                .run(context -> {
                    assertThat(context).hasBean("kafkaHealthIndicator");
                    assertThat(context.getBean(HealthContributorRegistry.class).getContributor("kafka"))
                            .isNotNull();
                });

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        HealthContributorAutoConfiguration.class,
                        HealthEndpointAutoConfiguration.class))
                .withUserConfiguration(IndicatorConfiguration.class, KafkaHealthIndicator.class)
                .withPropertyValues("ledgerflow.events.transport=noop")
                .run(context -> {
                    assertThat(context).doesNotHaveBean("kafkaHealthIndicator");
                    assertThat(context.getBean(HealthContributorRegistry.class).getContributor("kafka"))
                            .isNull();
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class IndicatorConfiguration {

        @Bean
        KafkaAdmin kafkaAdmin() {
            return mock(KafkaAdmin.class);
        }
    }
}
