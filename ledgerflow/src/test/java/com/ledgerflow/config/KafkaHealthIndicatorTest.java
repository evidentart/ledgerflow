package com.ledgerflow.config;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.HealthEndpointAutoConfiguration;
import org.springframework.boot.actuate.health.HealthContributorRegistry;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaAdmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaHealthIndicatorTest {

    private final AdminClient adminClient = mock(AdminClient.class);
    private final DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
    private final KafkaFuture<Collection<Node>> nodesFuture = mock(KafkaFuture.class);

    private KafkaHealthIndicator indicator(Duration timeout) {
        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);
        return new KafkaHealthIndicator(() -> adminClient, timeout);
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsUpWhenKafkaCanBeContacted() throws Exception {
        Map<String, Object> existingProperties = new HashMap<>();
        existingProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "configured-broker:9092");
        existingProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, "ledgerflow-admin");
        existingProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        existingProperties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        KafkaAdmin kafkaAdmin = new KafkaAdmin(existingProperties);

        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);
        when(nodesFuture.get(2_000, TimeUnit.MILLISECONDS)).thenReturn(List.of(mock(Node.class)));

        try (MockedStatic<AdminClient> adminClients = mockStatic(AdminClient.class)) {
            adminClients.when(() -> AdminClient.create(anyMap())).thenReturn(adminClient);

            var health = new KafkaHealthIndicator(kafkaAdmin).health();

            assertThat(health.getStatus().getCode()).isEqualTo("UP");
            verify(adminClient).describeCluster();
            verify(describeClusterResult).nodes();
            verify(nodesFuture).get(2_000, TimeUnit.MILLISECONDS);

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
    void reportsDownWhenBrokerConnectionFails() throws Exception {
        when(nodesFuture.get(anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new ExecutionException(new IllegalStateException("sensitive broker details")));

        var health = indicator(Duration.ofSeconds(1)).health();

        assertThat(health.getStatus().getCode()).isEqualTo("DOWN");
        assertThat(health.getDetails()).containsEntry("reason", "Kafka broker is unavailable");
        assertThat(health.toString()).doesNotContain("sensitive broker details");
        verify(adminClient).close(Duration.ofMillis(100));
    }

    @Test
    void reportsDownWhenConnectivityProbeTimesOutWithinItsBound() throws Exception {
        Duration timeout = Duration.ofMillis(75);
        doAnswer(invocation -> {
            TimeUnit.MILLISECONDS.sleep(invocation.getArgument(0));
            throw new TimeoutException();
        }).when(nodesFuture).get(anyLong(), eq(TimeUnit.MILLISECONDS));

        long startedAt = System.nanoTime();
        var health = indicator(timeout).health();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        assertThat(health.getStatus().getCode()).isEqualTo("DOWN");
        assertThat(elapsedMillis).isLessThan(500);
        verify(nodesFuture).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        verify(adminClient).close(Duration.ofMillis(100));
    }

    @Test
    void registersKafkaHealthContributorOnlyForKafkaTransport() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        HealthContributorAutoConfiguration.class,
                        HealthEndpointAutoConfiguration.class))
                .withUserConfiguration(IndicatorConfiguration.class)
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
                .withUserConfiguration(IndicatorConfiguration.class)
                .withPropertyValues("ledgerflow.events.transport=noop")
                .run(context -> {
                    assertThat(context).doesNotHaveBean("kafkaHealthIndicator");
                    assertThat(context.getBean(HealthContributorRegistry.class).getContributor("kafka"))
                            .isNull();
                });
    }

    @Configuration(proxyBeanMethods = false)
    @ComponentScan(basePackageClasses = KafkaHealthIndicator.class)
    static class IndicatorConfiguration {

        @Bean
        KafkaAdmin kafkaAdmin() {
            return mock(KafkaAdmin.class);
        }
    }
}
