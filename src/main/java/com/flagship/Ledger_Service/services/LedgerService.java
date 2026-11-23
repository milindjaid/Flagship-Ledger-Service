package com.flagship.Ledger_Service.services;

import com.flagship.Ledger_Service.entities.LedgerAccountType;
import com.flagship.Ledger_Service.entities.LedgerEntry;
import com.flagship.Ledger_Service.entities.LedgerStatus;
import com.flagship.Ledger_Service.repo.LedgerEntryRepository;
import com.flagship.common_lib.dto.LedgerPendingCreatedEvent;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class LedgerService {

    private final LedgerEntryRepository repository;

    private final LedgerAccountService accountService;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public void createPendingEntry(
            UUID paymentId,
            BigDecimal amount,
            String currency) {

        if (ledgerEntryRepository.existsByPaymentIdAndStatus(paymentId, LedgerStatus.PENDING)){
            return;
        }

        UUID debitAccountId = accountService
                .getAccountId(LedgerAccountType.CUSTOMER, currency);

        UUID creditAccountId = accountService
                .getAccountId(LedgerAccountType.SUSPENSE, currency);

        LedgerEntry entry = LedgerEntry.pending(
                paymentId,
                debitAccountId,
                creditAccountId,
                amount,
                currency
        );

        ledgerEntryRepository.save(entry);

        // 3️⃣ Publish ledger.pending.created
        kafkaTemplate.send(
                "ledger.pending.created",
                new LedgerPendingCreatedEvent(
                        entry.getLedgerEntryId(),
                        entry.getPaymentId(),
                        entry.getAmount(),
                        entry.getCurrency()
                )
        );
    }
}
