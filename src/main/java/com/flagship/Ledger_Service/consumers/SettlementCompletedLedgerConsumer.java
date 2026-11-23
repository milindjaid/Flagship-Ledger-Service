package com.flagship.Ledger_Service.consumers;

import com.flagship.Ledger_Service.entities.LedgerEntry;
import com.flagship.Ledger_Service.entities.LedgerStatus;
import com.flagship.Ledger_Service.repo.LedgerEntryRepository;
import com.flagship.common_lib.dto.LedgerPostedEvent;
import com.flagship.common_lib.dto.SettlementCompletedEvent;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class SettlementCompletedLedgerConsumer {

    private final LedgerEntryRepository ledgerEntryRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "settlement.completed",groupId = "ledger-settlement-completed")
    @Transactional
    public void onSettlementCompleted(SettlementCompletedEvent event, Acknowledgment ack) {
        log.info("In Ledger service's SettlementCompletedLedgerConsumer.");
        try {
            LedgerEntry entry = ledgerEntryRepository
                    .findByPaymentIdAndStatus(
                            event.paymentId(),
                            LedgerStatus.PENDING
                    )
                    .orElseThrow();

            entry.post(); // PENDING → POSTED

            ledgerEntryRepository.save(entry);

            // 👇 Ledger now emits wallet-aware event
            kafkaTemplate.send(
                    "ledger.posted",
                    new LedgerPostedEvent(
                            UUID.randomUUID(),
                            entry.getLedgerEntryId(),
                            entry.getDebitAccountId(), // walletId
                            entry.getAmount(),
                            entry.getCurrency(),
                            Instant.now(),
                            1,
                            event.customerId()
                    )
            );
            ack.acknowledge();
        } catch (RuntimeException e) {
            log.error("Error processing paymentId={}", event.paymentId(), e);
//            throw new RuntimeException(e);
        }
    }
}
