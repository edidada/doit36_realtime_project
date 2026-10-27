package cn.doitedu.fin.risk.fraud;

import java.util.ArrayList;
import java.util.List;

import static cn.doitedu.fin.risk.fraud.Contracts.*;

/** Deterministic, explainable sandbox policy. Scores are rule points, not fraud probabilities. */
public final class RiskPolicy {
    public static final String VERSION = "payment-coupon-rules-20261027-1";
    public Decision evaluate(FeatureSnapshot f) {
        List<String> reasons = new ArrayList<>();
        int score = 0;
        boolean hard = false;
        if (f.accountBlocked()) { reasons.add("ACCOUNT_BLOCKED"); hard = true; }
        if (!f.verified()) { reasons.add("IDENTITY_NOT_VERIFIED"); hard = true; }
        if (f.merchantBlocked()) { reasons.add("MERCHANT_BLOCKED"); hard = true; }
        if (f.compromisedDevice()) { reasons.add("DEVICE_COMPROMISED"); hard = true; }
        if (f.amountCents() > 1_000_000) { reasons.add("HARD_AMOUNT_LIMIT"); hard = true; }
        if (f.userAttempts() >= 5) { reasons.add("ACCOUNT_VELOCITY_10M"); hard = true; }
        if (f.deviceUsers() >= 3) { reasons.add("DEVICE_ACCOUNT_FARM_1D"); hard = true; }
        if (f.declinedAttempts() >= 3) { reasons.add("REPEATED_DECLINES_1H"); hard = true; }
        if (!f.trustedDevice()) { reasons.add("UNTRUSTED_DEVICE"); score += 30; }
        if (f.accountAgeHours() < 24) { reasons.add("NEW_ACCOUNT"); score += 25; }
        if (f.passwordResetAgeMinutes() < 60) { reasons.add("RECENT_PASSWORD_RESET"); score += 35; }
        if (!f.networkKnown()) { reasons.add("UNKNOWN_NETWORK"); score += 30; }
        if (f.proxyNetwork()) { reasons.add("PROXY_NETWORK"); score += 35; }
        if (f.countryMismatch()) { reasons.add("COUNTRY_MISMATCH"); score += 30; }
        if (f.amountCents() >= 100_000) { reasons.add("LARGE_AMOUNT"); score += 25; }
        if (f.amountCents() > Math.max(10_000, f.typicalAmountCents()) * 5) {
            reasons.add("UNUSUAL_AMOUNT"); score += 20;
        }
        if (f.availableBalanceCents() > 0 && f.amountCents() >= 10_000
                && java.math.BigDecimal.valueOf(f.amountCents()).compareTo(
                        java.math.BigDecimal.valueOf(f.availableBalanceCents()).multiply(new java.math.BigDecimal("0.95"))) >= 0) {
            reasons.add("BALANCE_DRAIN"); score += 35;
        }
        if (f.userAttempts() >= 3) { reasons.add("ELEVATED_VELOCITY"); score += 25; }
        if (reasons.isEmpty()) { reasons.add("BASELINE_PASSED"); }
        return new Decision(hard || score >= 75 ? "BLOCK" : score >= 30 ? "REVIEW" : "ALLOW",
                hard ? 100 : Math.min(100, score), reasons, VERSION);
    }
}
