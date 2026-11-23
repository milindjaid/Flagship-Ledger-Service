package com.flagship.Ledger_Service.consumers;

import com.flagship.Ledger_Service.entities.LedgerStatus;
import com.flagship.Ledger_Service.repo.LedgerEntryRepository;
import com.flagship.Ledger_Service.services.LedgerService;
import com.flagship.common_lib.dto.WalletBlockedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class LedgerConsumer {

    private final LedgerService ledgerService;
    private final LedgerEntryRepository ledgerEntryRepository;

    @KafkaListener(topics = "wallet.blocked",groupId = "ledger-service")
    public void onWalletBlocked(WalletBlockedEvent event, Acknowledgment ack) {
        log.info("Ledger Consumer In wallet.blocked consumer {} ",event);
        try {
            if (ledgerEntryRepository.existsByPaymentIdAndStatus(event.paymentId(), LedgerStatus.PENDING)) {
                log.info("Duplicate event ignored for paymentId={}", event.paymentId());
                return;
            }
            ledgerService.createPendingEntry(
                    event.paymentId(),
                    event.amount(),
                    event.currency()
            );
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing paymentId={}", event.paymentId(), e);
            throw new RuntimeException(e);
        }
    }
}

