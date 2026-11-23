package com.flagship.Ledger_Service.services;

import com.flagship.Ledger_Service.entities.LedgerAccountType;
import com.flagship.Ledger_Service.repo.LedgerAccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class LedgerAccountService {

    private final LedgerAccountRepository repository;

    public UUID getAccountId(LedgerAccountType type, String currency) {
        return repository
                .findByAccountTypeAndCurrency(type, currency)
                .orElseThrow(() ->
                        new IllegalStateException(
                                "Ledger account not found for " + type + " " + currency
                        )
                )
                .getLedgerAccountId();
    }
}
