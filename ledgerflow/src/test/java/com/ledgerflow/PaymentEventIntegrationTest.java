package com.ledgerflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgerflow.api.PaymentController;
import com.ledgerflow.domain.Payment;
import com.ledgerflow.domain.PaymentEventRecord;
import com.ledgerflow.repository.PaymentEventRepository;
import com.ledgerflow.repository.PaymentRepository;
import com.ledgerflow.service.IdempotencyService;
import com.ledgerflow.service.LedgerService;
import com.ledgerflow.service.PaymentService;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Exercises the controller, service and real PostgreSQL query without Kafka.
 * Flyway owns the schema. Validation is disabled only for this slice because the
 * existing currency CHAR/VARCHAR mismatch prevents full application startup.
 */
@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=none")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PaymentController.class, PaymentService.class, LedgerService.class, IdempotencyService.class})
@ImportAutoConfiguration(JacksonAutoConfiguration.class)
@Testcontainers
@ActiveProfiles("test")
class PaymentEventIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired PaymentController controller;
    @Autowired PaymentRepository payments;
    @Autowired PaymentEventRepository events;
    @Autowired EntityManager entityManager;
    @Autowired ObjectMapper mapper;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setMessageConverters(new MappingJackson2HttpMessageConverter(mapper))
                .build();
    }

    @Test
    void returnsRecordedEventWithExactlyTheResponseFields() throws Exception {
        UUID paymentId = createPayment();
        String payload = "{\"detail\":\"funds captured\",\"amountMinor\":5000}";
        PaymentEventRecord event = new PaymentEventRecord(UUID.randomUUID(), paymentId,
                "PaymentCaptured", payload, Instant.parse("2026-01-01T12:00:00Z"));
        events.saveAndFlush(event);
        entityManager.clear();

        mvc.perform(get("/v1/payments/{id}/events", paymentId))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0]", aMapWithSize(4)))
                .andExpect(jsonPath("$[0].eventId", is(event.getEventId().toString())))
                .andExpect(jsonPath("$[0].type", is("PaymentCaptured")))
                .andExpect(jsonPath("$[0].payload", is(payload)))
                .andExpect(jsonPath("$[0].occurredAt", is("2026-01-01T12:00:00Z")));
    }

    @Test
    void returnsOnlyThePaymentsEventsInChronologicalOrder() throws Exception {
        UUID paymentId = createPayment();
        Instant start = Instant.parse("2026-01-01T12:00:00Z");
        PaymentEventRecord first = event(paymentId, "PaymentAuthorized", start);
        PaymentEventRecord second = event(paymentId, "RiskApproved", start.plusSeconds(1));
        PaymentEventRecord third = event(paymentId, "PaymentCaptured", start.plusSeconds(2));
        events.saveAllAndFlush(List.of(third, first, second,
                event(createPayment(), "PaymentCaptured", start)));
        entityManager.clear();

        mvc.perform(get("/v1/payments/{id}/events", paymentId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(3)))
                .andExpect(jsonPath("$[0].eventId", is(first.getEventId().toString())))
                .andExpect(jsonPath("$[1].eventId", is(second.getEventId().toString())))
                .andExpect(jsonPath("$[2].eventId", is(third.getEventId().toString())));
    }

    @Test
    void returnsEmptyArrayForPaymentWithoutEvents() throws Exception {
        mvc.perform(get("/v1/payments/{id}/events", createPayment()))
                .andExpect(status().isOk())
                .andExpect(content().json("[]"));
    }

    @Test
    void returnsEmptyArrayForUnknownPayment() throws Exception {
        mvc.perform(get("/v1/payments/{id}/events", UUID.randomUUID()))
                .andExpect(status().isOk())
                .andExpect(content().json("[]"));
    }

    private UUID createPayment() {
        UUID id = UUID.randomUUID();
        payments.saveAndFlush(new Payment(id, "event-history-" + id, 5000, "INR"));
        return id;
    }

    private PaymentEventRecord event(UUID paymentId, String type, Instant occurredAt) {
        return new PaymentEventRecord(UUID.randomUUID(), paymentId, type, "{}", occurredAt);
    }
}
