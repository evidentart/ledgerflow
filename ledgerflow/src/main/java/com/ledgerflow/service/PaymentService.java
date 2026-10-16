package com.ledgerflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgerflow.api.dto.CreatePaymentRequest;
import com.ledgerflow.api.dto.PaymentResponse;
import com.ledgerflow.api.dto.PaymentEventResponse;
import com.ledgerflow.domain.Enums;
import com.ledgerflow.domain.IdempotencyRecord;
import com.ledgerflow.domain.Payment;
import com.ledgerflow.exception.ApiException;
import com.ledgerflow.repository.PaymentRepository;
import com.ledgerflow.repository.PaymentEventRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class PaymentService {

    private final PaymentRepository payments;
    private final PaymentEventRepository paymentEvents;
    private final LedgerService ledger;
    private final IdempotencyService idempotency;
    private final ObjectMapper objectMapper;

    public PaymentService(PaymentRepository payments, LedgerService ledger,
                          IdempotencyService idempotency, ObjectMapper objectMapper,
                          PaymentEventRepository paymentEvents) {
        this.payments = payments;
        this.paymentEvents = paymentEvents;
        this.ledger = ledger;
        this.idempotency = idempotency;
        this.objectMapper = objectMapper;
    }

    /**
     * Creates and captures a payment. The whole operation is idempotent on the
     * client-supplied key: a retry with the same key and body replays the stored
     * response; the same key with a different body is a 409.
     */
    @Transactional
    public PaymentResponse createPayment(CreatePaymentRequest request, String idempotencyKey) {
        String requestHash = idempotency.hash(serialize(request));

        var existing = idempotency.find(idempotencyKey);
        if (existing.isPresent()) {
            return replay(existing.get(), requestHash);
        }

        Payment payment = new Payment(
                UUID.randomUUID(), request.merchantReference(),
                request.amountMinor(), request.currency());
        payment.transitionTo(Enums.PaymentStatus.AUTHORIZED);
        payment.transitionTo(Enums.PaymentStatus.CAPTURED);
        payments.save(payment);

        ledger.postCapture(payment);

        PaymentResponse response = PaymentResponse.from(payment);
        persistIdempotency(idempotencyKey, requestHash, response);
        return response;
    }

    @Transactional
    public PaymentResponse refund(UUID paymentId) {
        Payment payment = payments.findById(paymentId).orElseThrow(() ->
                new ApiException(HttpStatus.NOT_FOUND, "Payment not found: " + paymentId));

        if (payment.getStatus() != Enums.PaymentStatus.CAPTURED) {
            throw new ApiException(HttpStatus.CONFLICT,
                    "Only CAPTURED payments can be refunded; current status is " + payment.getStatus());
        }

        ledger.postRefund(payment);
        payment.transitionTo(Enums.PaymentStatus.REFUNDED);
        payments.save(payment);
        return PaymentResponse.from(payment);
    }

    @Transactional(readOnly = true)
    public PaymentResponse get(UUID paymentId) {
        return payments.findById(paymentId)
                .map(PaymentResponse::from)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND,
                        "Payment not found: " + paymentId));
    }

    @Transactional(readOnly = true)
    public List<PaymentEventResponse> getEvents(UUID paymentId) {
        return paymentEvents.findByPaymentIdOrderByOccurredAtAsc(paymentId).stream()
                .map(PaymentEventResponse::from)
                .toList();
    }

    // --- idempotency helpers ------------------------------------------------

    private PaymentResponse replay(IdempotencyRecord record, String requestHash) {
        if (!record.getRequestHash().equals(requestHash)) {
            throw new ApiException(HttpStatus.CONFLICT,
                    "Idempotency-Key was already used with a different request body");
        }
        return deserialize(record.getResponseBody());
    }

    private void persistIdempotency(String key, String hash, PaymentResponse response) {
        try {
            idempotency.save(new IdempotencyRecord(
                    key, hash, HttpStatus.CREATED.value(), serialize(response)));
        } catch (DataIntegrityViolationException raced) {
            // A concurrent identical request won the insert. Replay its stored result.
            IdempotencyRecord winner = idempotency.find(key).orElseThrow(() -> raced);
            replay(winner, hash);
        }
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Serialization failed", e);
        }
    }

    private PaymentResponse deserialize(String json) {
        try {
            return objectMapper.readValue(json, PaymentResponse.class);
        } catch (Exception e) {
            throw new IllegalStateException("Deserialization failed", e);
        }
    }
}
