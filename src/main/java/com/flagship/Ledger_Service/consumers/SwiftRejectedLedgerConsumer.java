package com.flagship.Ledger_Service.consumers;

import com.flagship.Ledger_Service.entities.LedgerEntry;
import com.flagship.Ledger_Service.entities.LedgerStatus;
import com.flagship.Ledger_Service.repo.LedgerEntryRepository;
import com.flagship.common_lib.dto.LedgerReversedEvent;
import com.flagship.common_lib.dto.SwiftRejectedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SwiftRejectedLedgerConsumer {

    private final LedgerEntryRepository ledgerEntryRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "swift.rejected",groupId = "ledger-swift-rejected")
    public void onSwiftRejected(SwiftRejectedEvent event, Acknowledgment ack) {

        log.info("In Ledger service's SwiftRejectedLedgerConsumer.");
        try {
            LedgerEntry entry = ledgerEntryRepository
                    .findByPaymentIdAndStatus(
                            event.paymentId(),
                            LedgerStatus.PENDING
                    )
                    .orElseThrow();

            entry.reverseFromSwift(event.reason());
            ledgerEntryRepository.save(entry);

            kafkaTemplate.send(
                    "ledger.reversed",
                    new LedgerReversedEvent(
                            entry.getLedgerEntryId(),
                            entry.getPaymentId(),
                            entry.getDebitAccountId(), // ✅ WALLET ID
                            entry.getAmount(),
                            entry.getCurrency(),
                            event.customerId()
                    )
            );
            ack.acknowledge();
        } catch (RuntimeException e) {
            log.error("Error processing paymentId={}", event.paymentId(), e);
            throw new RuntimeException(e);
        }
    }
}

