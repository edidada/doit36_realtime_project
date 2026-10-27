package cn.doitedu.fin.risk.fraud;

import java.util.List;

public final class Contracts {
    private Contracts() { }

    public record Principal(String userId, String role, String sessionDeviceId) {
        public Principal(String userId, String role) { this(userId, role, userId + "-phone"); }
    }
    public record OrderRequest(String productId, int quantity) { }
    public record ClaimRequest(String campaignId, String deviceId) { }
    // Amount, owner, currency, provider outcome and risk flags are never supplied by this command.
    public record PaymentRequest(String orderId, String deviceId, String couponId) { }
    public record ReviewRequest(boolean approve, String reason) { }
    public record Decision(String action, int score, List<String> reasons, String ruleVersion) {
        public Decision { reasons = List.copyOf(reasons); }
    }
    public record OrderView(String id, String owner, String merchant, String currency,
                            long amountCents, String status, String paymentId) { }
    public record CouponView(String id, String owner, String campaign, String status,
                             long discountCents, long expiresAt, String paymentId) { }
    public record ClaimResult(String status, String reason, CouponView coupon) { }
    public record PaymentView(String id, String owner, String orderId, long grossCents,
                              long discountCents, long payableCents, String status,
                              Decision decision, String providerReference, long createdAt) { }
    public record FeatureSnapshot(boolean accountBlocked, boolean verified, long accountAgeHours,
                                  long passwordResetAgeMinutes, boolean trustedDevice, boolean compromisedDevice,
                                  boolean merchantBlocked, boolean networkKnown, boolean proxyNetwork,
                                  boolean countryMismatch, long amountCents, long typicalAmountCents,
                                  int userAttempts, int deviceUsers, int declinedAttempts, long availableBalanceCents) { }

    public static String id(String value, String field) {
        if (value == null || !value.matches("[a-zA-Z0-9_-]{1,80}")) {
            throw new ApiException(400, "INVALID_INPUT", "Invalid " + field);
        }
        return value;
    }

    public static final class ApiException extends RuntimeException {
        private final int status;
        private final String code;

        public ApiException(int status, String code, String message) {
            super(message); this.status = status; this.code = code;
        }
        public int status() { return status; }
        public String code() { return code; }
    }
}
