package com.flagship.Ledger_Service.entities;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "ledger_entry")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Setter
public class LedgerEntry {

    @Id
    @Column(name = "ledger_entry_id", nullable = false)
    private UUID ledgerEntryId;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "debit_account_id", nullable = false)
    private UUID debitAccountId;

    @Column(name = "credit_account_id", nullable = false)
    private UUID creditAccountId;

    @Column(nullable = false, precision = 18, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "entry_type", nullable = false, length = 30)
    private LedgerEntryType entryType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private LedgerStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false, updatable = false)
    private Instant updatedAt;

    @Column(name = "reversal_Reason")
    private String reversalReason;


    // ---------- Lifecycle Callbacks ----------

    @PrePersist
    public void prePersist() {
        if (ledgerEntryId == null) {
            ledgerEntryId = UUID.randomUUID();
        }
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public void reverseFromSwift(String reason) {
        this.status = LedgerStatus.REJECTED;
        this.updatedAt = Instant.now();
    }

    public void reverse(String reason) {
        this.status = LedgerStatus.REVERSED;
        this.reversalReason = reason;
        this.updatedAt = Instant.now();
    }



    public static LedgerEntry pending(
            UUID paymentId,
            UUID debitAccountId,
            UUID creditAccountId,
            BigDecimal amount,
            String currency
    ) {
        return LedgerEntry.builder()
                .ledgerEntryId(UUID.randomUUID())
                .paymentId(paymentId)
                .debitAccountId(debitAccountId)
                .creditAccountId(creditAccountId)
                .amount(amount)
                .currency(currency)
                .entryType(LedgerEntryType.PAYMENT)
                .status(LedgerStatus.PENDING)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }

    public void post() {
        this.status = LedgerStatus.POSTED;
        this.updatedAt = Instant.now();
    }

}

