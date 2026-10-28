package com.test.bigdata.fin.risk.fraud;

import java.util.Map;

import static com.test.bigdata.fin.risk.fraud.Database.*;

/** Durable, idempotent fake PSP. Separate transactions deliberately model a remote commit boundary. */
public final class SandboxProvider {
    private final Database database;
    public SandboxProvider(Database database) { this.database = database; }
    public record Outcome(String state, String reference) { }

    public Outcome charge(String paymentId) {
        return database.transaction(c -> {
            String key = "charge-" + paymentId;
            var existing = rows(c, "SELECT * FROM provider_ledger WHERE operation_key=?", key);
            if (!existing.isEmpty()) { return outcome(existing.get(0)); }
            var payment = one(c, "SELECT p.*,m.provider_mode FROM payments p JOIN orders o ON p.order_id=o.id "
                    + "JOIN merchants m ON m.id=o.merchant_id WHERE p.id=?", paymentId);
            if (!"PENDING".equals(text(payment, "status"))) {
                throw new IllegalStateException("PSP charge requires a durable PENDING intent");
            }
            String mode = text(payment, "provider_mode");
            if ("UNAVAILABLE".equals(mode)) { return new Outcome("UNKNOWN", null); }
            long amount = number(payment, "payable_cents");
            String user = text(payment, "user_id");
            boolean funded = !"DECLINE".equals(mode) && update(c,
                    "UPDATE wallets SET balance_cents=balance_cents-? WHERE user_id=? AND balance_cents>=?",
                    amount, user, amount) == 1;
            String reference = "psp-" + paymentId;
            String state = funded ? "CAPTURED" : "DECLINED";
            update(c, "INSERT INTO provider_ledger VALUES(?,?,?,?,?,?)", key, paymentId, user, amount, state, reference);
            // The PSP committed a capture but the application saw a timeout. A retry returns the stored outcome.
            return new Outcome(funded && "TIMEOUT_AFTER_CAPTURE".equals(mode) ? "UNKNOWN" : state, reference);
        });
    }

    public Outcome refund(String paymentId) {
        return database.transaction(c -> {
            var existing = rows(c, "SELECT * FROM provider_ledger WHERE operation_key=?", "refund-" + paymentId);
            if (!existing.isEmpty()) { return outcome(existing.get(0)); }
            var payment = one(c, "SELECT * FROM payments WHERE id=?", paymentId);
            if (!"REFUND_PENDING".equals(text(payment, "status"))) {
                throw new IllegalStateException("PSP refund requires durable REFUND_PENDING intent");
            }
            var charge = one(c, "SELECT * FROM provider_ledger WHERE operation_key=?", "charge-" + paymentId);
            if (!"CAPTURED".equals(text(charge, "outcome"))) { throw new IllegalStateException("No captured charge"); }
            long amount = number(charge, "amount_cents");
            String user = text(charge, "user_id");
            update(c, "UPDATE wallets SET balance_cents=balance_cents+? WHERE user_id=?", amount, user);
            String reference = "refund-" + paymentId;
            update(c, "INSERT INTO provider_ledger VALUES(?,?,?,?,?,?)", reference, paymentId, user, amount,
                    "REFUNDED", reference);
            return new Outcome("REFUNDED", reference);
        });
    }

    private static Outcome outcome(Map<String,Object> row) {
        return new Outcome(text(row, "outcome"), text(row, "reference_id"));
    }
}
