package com.ledgerflow.api;

import com.ledgerflow.api.dto.CreatePaymentRequest;
import com.ledgerflow.api.dto.PaymentResponse;
import com.ledgerflow.api.dto.PaymentEventResponse;
import com.ledgerflow.exception.ApiException;
import com.ledgerflow.service.PaymentService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> create(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody CreatePaymentRequest request) {

        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Idempotency-Key header is required");
        }
        PaymentResponse response = paymentService.createPayment(request, idempotencyKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{id}")
    public PaymentResponse get(@PathVariable UUID id) {
        return paymentService.get(id);
    }

    @GetMapping("/{id}/events")
    public List<PaymentEventResponse> events(@PathVariable UUID id) {
        return paymentService.getEvents(id);
    }

    @PostMapping("/{id}/refund")
    public PaymentResponse refund(@PathVariable UUID id) {
        return paymentService.refund(id);
    }
}
