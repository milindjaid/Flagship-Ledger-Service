package com.flagship.Ledger_Service.repo;

import com.flagship.Ledger_Service.entities.LedgerEntry;
import com.flagship.Ledger_Service.entities.LedgerStatus;
import lombok.NonNull;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface LedgerEntryRepository extends JpaRepository<@NonNull LedgerEntry,@NonNull Long> {
    Optional<LedgerEntry> findByPaymentIdAndStatus(UUID uuid, LedgerStatus pending);

    boolean existsByPaymentIdAndStatus(UUID paymentId, LedgerStatus ledgerStatus);
}
