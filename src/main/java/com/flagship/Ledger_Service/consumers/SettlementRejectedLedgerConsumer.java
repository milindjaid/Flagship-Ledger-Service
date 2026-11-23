package com.flagship.Ledger_Service.consumers;

import com.flagship.Ledger_Service.entities.LedgerEntry;
import com.flagship.Ledger_Service.entities.LedgerStatus;
import com.flagship.Ledger_Service.repo.LedgerEntryRepository;
import com.flagship.common_lib.dto.LedgerReversedEvent;
import com.flagship.common_lib.dto.SettlementRejectedEvent;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SettlementRejectedLedgerConsumer {

    private final LedgerEntryRepository ledgerEntryRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "settlement.rejected",groupId = "ledger-settlement.rejected")
    @Transactional
    public void onSettlementRejected(SettlementRejectedEvent event, Acknowledgment ack) {

        log.info("In Ledger service's SettlementRejectedLedgerConsumer.");
        try {
            LedgerEntry entry = ledgerEntryRepository
                    .findByPaymentIdAndStatus(
                            event.paymentId(),
                            LedgerStatus.PENDING
                    )
                    .orElseThrow();

            entry.reverse(event.reason().name());

            ledgerEntryRepository.save(entry);

            kafkaTemplate.send(
                    "ledger.reversed",
                    new LedgerReversedEvent(
                            entry.getLedgerEntryId(),
                            entry.getPaymentId(),
                            entry.getDebitAccountId(), // walletId
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

