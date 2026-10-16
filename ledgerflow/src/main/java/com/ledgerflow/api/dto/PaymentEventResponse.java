package com.ledgerflow.api.dto;

import com.ledgerflow.domain.PaymentEventRecord;

import java.time.Instant;
import java.util.UUID;

public record PaymentEventResponse(
        UUID eventId,
        String type,
        String payload,
        Instant occurredAt
) {
    public static PaymentEventResponse from(PaymentEventRecord event) {
        return new PaymentEventResponse(event.getEventId(), event.getType(),
                event.getPayload(), event.getOccurredAt());
    }
}
