package com.flagship.Ledger_Service.repo;

import com.flagship.Ledger_Service.entities.LedgerAccount;
import com.flagship.Ledger_Service.entities.LedgerAccountType;
import lombok.NonNull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface LedgerAccountRepository
        extends JpaRepository<@NonNull LedgerAccount, @NonNull UUID> {

    Optional<LedgerAccount> findByAccountTypeAndCurrency(
            LedgerAccountType accountType,
            String currency
    );
}
