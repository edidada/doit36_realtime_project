package com.test.bigdata.fin.olap;

import java.time.LocalDate;
import java.util.Objects;

/** Amounts are signed minor currency units; a row is a transaction, not an account balance. */
public record Transaction(String tradeId, LocalDate tradeDate, String accountId,
                          String institutionId, String transactionType, String currency,
                          long amountCents) {
    public Transaction {
        requireText(tradeId, "tradeId");
        Objects.requireNonNull(tradeDate, "tradeDate");
        requireText(accountId, "accountId");
        requireText(institutionId, "institutionId");
        requireText(transactionType, "transactionType");
        if (currency == null || !currency.matches("[A-Z]{3}")) {
            throw new IllegalArgumentException("currency must be an uppercase three-letter code");
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
