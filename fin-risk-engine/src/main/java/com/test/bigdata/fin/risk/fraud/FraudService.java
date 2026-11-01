package com.test.bigdata.fin.risk.fraud;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.test.bigdata.fin.risk.fraud.Contracts.*;
import static com.test.bigdata.fin.risk.fraud.Database.*;

/** Application orchestration: trusted features -> decision -> durable intent -> PSP -> reconciliation. */
public final class FraudService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FraudService.class);
    private final Database database;
    private final Clock clock;
    private final RiskPolicy policy = new RiskPolicy();
    private final SandboxProvider provider;

    public FraudService(Database database, Clock clock) {
        this.database = database; this.clock = clock; this.provider = new SandboxProvider(database);
    }

    public Principal authenticate(String token) {
        if (token == null || token.length() < 24 || token.length() > 256) { throw unauthorized(); }
        return database.transaction(c -> {
            var credentials = rows(c, "SELECT * FROM api_credentials WHERE token_hash=?", hash(token));
            if (credentials.isEmpty()) { throw unauthorized(); }
            var credential = credentials.get(0);
            return new Principal(text(credential, "user_id"), text(credential, "role"), text(credential, "device_id"));
        });
    }

    public void checkReady() {
        database.transaction(c -> one(c, "SELECT id FROM command_lock WHERE id=1"));
    }

    public OrderView createOrder(Principal actor, String key, OrderRequest request) {
        customer(actor); id(key, "Idempotency-Key"); id(request.productId(), "productId");
        if (request.quantity() < 1 || request.quantity() > 100) { throw invalid("quantity must be 1..100"); }
        return database.transaction(c -> {
            activeUser(c, actor.userId());
            String fingerprint = hash(json(request));
            var existing = rows(c, "SELECT * FROM order_commands WHERE user_id=? AND idem_key=?", actor.userId(), key);
            if (!existing.isEmpty()) {
                sameRequest(existing.get(0), fingerprint);
                return order(c, actor, text(existing.get(0), "order_id"));
            }
            var product = one(c, "SELECT * FROM products WHERE id=?", request.productId());
            long amount = Math.multiplyExact(number(product, "price_cents"), request.quantity());
            String orderId = newId();
            update(c, "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?)", orderId, actor.userId(),
                    text(product, "merchant_id"), text(product, "currency"), amount, "OPEN", null, clock.millis());
            // Explicit columns catch accidental schema drift and preserve the idempotency response binding.
            update(c, "INSERT INTO order_commands VALUES(?,?,?,?)", actor.userId(), key, fingerprint, orderId);
            audit(c, actor, "ORDER_CREATED", orderId, Map.of("amountCents", amount));
            return order(c, actor, orderId);
        });
    }

    public OrderView getOrder(Principal actor, String orderId) {
        id(orderId, "orderId"); return database.transaction(c -> order(c, actor, orderId));
    }

    public ClaimResult claim(Principal actor, String key, ClaimRequest request) {
        customer(actor); id(key, "Idempotency-Key"); id(request.campaignId(), "campaignId"); id(request.deviceId(), "deviceId");
        return database.transaction(c -> {
            String fingerprint = hash(json(request));
            var command = rows(c, "SELECT * FROM claim_commands WHERE user_id=? AND idem_key=?", actor.userId(), key);
            if (!command.isEmpty()) {
                sameRequest(command.get(0), fingerprint);
                return decode(text(command.get(0), "response_json"), ClaimResult.class);
            }
            var user = one(c, "SELECT * FROM users WHERE id=?", actor.userId());
            var campaign = one(c, "SELECT * FROM campaigns WHERE id=?", request.campaignId());
            var device = rows(c, "SELECT * FROM devices WHERE id=? AND user_id=?", request.deviceId(), actor.userId());
            String reason = null;
            if (!"ACTIVE".equals(text(user, "status")) || !flag(user, "verified")) { reason = "ACCOUNT_INELIGIBLE"; }
            else if (!request.deviceId().equals(actor.sessionDeviceId()) || device.isEmpty()
                    || !flag(device.get(0), "trusted") || flag(device.get(0), "compromised")) {
                reason = "DEVICE_INELIGIBLE";
            } else if (clock.millis() - number(user, "created_at") < 86_400_000) { reason = "ACCOUNT_TOO_NEW"; }
            else if (clock.millis() - number(user, "reset_at") < 3_600_000) { reason = "RECENT_PASSWORD_RESET"; }
            else if (clock.millis() < number(campaign, "starts_at") || clock.millis() >= number(campaign, "expires_at")) {
                reason = "CAMPAIGN_CLOSED";
            } else if (!rows(c, "SELECT id FROM coupons WHERE campaign_id=? AND (user_id=? OR device_id=?)",
                    request.campaignId(), actor.userId(), request.deviceId()).isEmpty()) { reason = "ALREADY_CLAIMED_OR_SHARED_DEVICE"; }
            else if (number(campaign, "remaining") <= 0) { reason = "STOCK_EXHAUSTED"; }
            ClaimResult result;
            if (reason != null) { result = new ClaimResult("BLOCKED", reason, null); }
            else {
                String couponId = newId();
                update(c, "UPDATE campaigns SET remaining=remaining-1 WHERE id=? AND remaining>0", request.campaignId());
                update(c, "INSERT INTO coupons VALUES(?,?,?,?,?,?,?)", couponId, actor.userId(), request.campaignId(),
                        request.deviceId(), "AVAILABLE", null, clock.millis());
                result = new ClaimResult("CLAIMED", "ELIGIBLE", coupon(c, actor, couponId));
            }
            update(c, "INSERT INTO claim_commands VALUES(?,?,?,?)", actor.userId(), key, fingerprint, json(result));
            audit(c, actor, "COUPON_CLAIM", request.campaignId(), result);
            return result;
        });
    }

    public CouponView getCoupon(Principal actor, String couponId) {
        id(couponId, "couponId"); return database.transaction(c -> coupon(c, actor, couponId));
    }

    public PaymentView pay(Principal actor, String key, PaymentRequest request, String peerIp) {
        customer(actor); id(key, "Idempotency-Key"); id(request.orderId(), "orderId"); id(request.deviceId(), "deviceId");
        if (request.couponId() != null) { id(request.couponId(), "couponId"); }
        PaymentView intent = database.transaction(c -> {
            String fingerprint = hash(json(request));
            var previous = rows(c, "SELECT * FROM payments WHERE user_id=? AND idem_key=?", actor.userId(), key);
            if (!previous.isEmpty()) {
                sameRequest(previous.get(0), fingerprint);
                return payment(previous.get(0));
            }
            var order = order(c, actor, request.orderId());
            if (!order.status().equals("OPEN")) { throw conflict("ORDER_NOT_OPEN", "Order already has an active or completed payment"); }
            FeatureSnapshot features = features(c, actor, request.deviceId(), order, peerIp, null);
            Decision decision = policy.evaluate(features);
            long discount = 0;
            if (request.couponId() != null) {
                String failure = couponFailure(c, actor.userId(), request.couponId(), order, null);
                if (failure != null) {
                    var reasons = new java.util.ArrayList<String>(); reasons.add(failure);
                    if (!decision.reasons().equals(List.of("BASELINE_PASSED"))) { reasons.addAll(decision.reasons()); }
                    decision = new Decision("BLOCK", 100, reasons, RiskPolicy.VERSION);
                }
                else { discount = coupon(c, actor, request.couponId()).discountCents(); }
            }
            String paymentId = newId();
            String status = switch (decision.action()) { case "BLOCK" -> "BLOCKED"; case "REVIEW" -> "REVIEW"; default -> "PENDING"; };
            update(c, "INSERT INTO payments VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", paymentId, actor.userId(), key,
                    fingerprint, order.id(), request.deviceId(), request.couponId(), order.amountCents(), discount,
                    Math.subtractExact(order.amountCents(), discount), status, json(decision), json(features), null, clock.millis(), actor.sessionDeviceId());
            if (!status.equals("BLOCKED")) {
                update(c, "UPDATE orders SET status='PAYMENT_IN_PROGRESS',payment_id=? WHERE id=?", paymentId, order.id());
                if (request.couponId() != null) {
                    update(c, "UPDATE coupons SET status='RESERVED',payment_id=? WHERE id=? AND status='AVAILABLE'", paymentId, request.couponId());
                }
            }
            audit(c, actor, "PAYMENT_DECISION", paymentId, Map.of("decision", decision, "features", features));
            return payment(c, actor, paymentId);
        });
        return "PENDING".equals(intent.status()) ? reconcileOne(intent.id()) : intent;
    }

    public PaymentView getPayment(Principal actor, String paymentId) {
        id(paymentId, "paymentId"); return database.transaction(c -> payment(c, actor, paymentId));
    }

    public PaymentView review(Principal actor, String paymentId, ReviewRequest request) {
        admin(actor); id(paymentId, "paymentId");
        if (request.reason() == null || request.reason().isBlank() || request.reason().length() > 300) {
            throw invalid("review reason must be 1..300 characters");
        }
        var reviewed = database.transaction(c -> {
            var row = one(c, "SELECT * FROM payments WHERE id=?", paymentId);
            if (!"REVIEW".equals(text(row, "status"))) { throw conflict("NOT_REVIEWABLE", "Payment is not in review"); }
            Principal owner = new Principal(text(row, "user_id"), "CUSTOMER", text(row, "session_device_id"));
            var order = order(c, owner, text(row, "order_id"));
            // The original peer's facts are snapshotted; re-check account/device/velocity at approval time.
            var oldFeatures = decode(text(row, "features_json"), FeatureSnapshot.class);
            var fresh = features(c, owner, text(row, "device_id"), order, null, paymentId);
            var checked = policy.evaluate(new FeatureSnapshot(fresh.accountBlocked(), fresh.verified(), fresh.accountAgeHours(),
                    fresh.passwordResetAgeMinutes(), fresh.trustedDevice(), fresh.compromisedDevice(), fresh.merchantBlocked(),
                    oldFeatures.networkKnown(), oldFeatures.proxyNetwork(), oldFeatures.countryMismatch(), fresh.amountCents(),
                    fresh.typicalAmountCents(), fresh.userAttempts(), fresh.deviceUsers(), fresh.declinedAttempts(), fresh.availableBalanceCents()));
            String couponId = text(row, "coupon_id");
            String failure = couponId == null ? null : couponFailure(c, owner.userId(), couponId, order, paymentId);
            boolean approved = request.approve() && !checked.action().equals("BLOCK") && failure == null;
            if (approved) { update(c, "UPDATE payments SET status='PENDING' WHERE id=?", paymentId); }
            else {
                update(c, "UPDATE payments SET status='BLOCKED' WHERE id=?", paymentId);
                release(c, paymentId);
            }
            audit(c, actor, approved ? "REVIEW_APPROVED" : "REVIEW_REJECTED", paymentId,
                    Map.of("reason", request.reason(), "recheck", checked, "couponCheck", failure == null ? "PASSED" : failure));
            return payment(c, owner, paymentId);
        });
        return "PENDING".equals(reviewed.status()) ? reconcileOne(paymentId) : reviewed;
    }

    public PaymentView refund(Principal actor, String paymentId) {
        customer(actor); id(paymentId, "paymentId");
        var intent = database.transaction(c -> {
            var payment = payment(c, actor, paymentId);
            if ("REFUNDED".equals(payment.status()) || "REFUND_PENDING".equals(payment.status())) { return payment; }
            if (!"PAID".equals(payment.status())) { throw conflict("NOT_REFUNDABLE", "Only paid transactions can be refunded"); }
            update(c, "UPDATE payments SET status='REFUND_PENDING' WHERE id=?", paymentId);
            audit(c, actor, "REFUND_REQUESTED", paymentId, Map.of("payableCents", payment.payableCents()));
            return payment(c, actor, paymentId);
        });
        return intent.status().equals("REFUND_PENDING") ? reconcileOne(paymentId) : intent;
    }

    public PaymentView cancelReview(Principal actor, String paymentId) {
        customer(actor); id(paymentId, "paymentId");
        return database.transaction(c -> {
            var payment = payment(c, actor, paymentId);
            if (payment.status().equals("CANCELLED")) { return payment; }
            if (!payment.status().equals("REVIEW")) {
                throw conflict("NOT_CANCELLABLE", "Only an uncharged review can be cancelled; uncertain payments require reconciliation");
            }
            update(c, "UPDATE payments SET status='CANCELLED' WHERE id=?", paymentId);
            release(c, paymentId);
            audit(c, actor, "PAYMENT_CANCELLED", paymentId, Map.of("reason", "customer cancelled review"));
            return payment(c, actor, paymentId);
        });
    }

    /** Durable PENDING rows form a small recovery queue, retried on startup, periodically or by an operator. */
    public int reconcile(Principal actor) {
        admin(actor);
        var pending = database.transaction(c -> rows(c,
                "SELECT id FROM payments WHERE status IN ('PENDING','REFUND_PENDING') ORDER BY created_at LIMIT 100"));
        int completed = 0;
        for (var row : pending) {
            try {
                var result = reconcileOne(text(row, "id"));
                if (!result.status().endsWith("PENDING")) { completed++; }
            } catch (RuntimeException failure) {
                // Fail closed: intent stays durable; the periodic worker will try again.
                LOGGER.warn("reconciliation failed for {}; intent retained", text(row, "id"));
            }
        }
        return completed;
    }

    public PaymentView reconcileOne(String paymentId) {
        var before = database.transaction(c -> one(c, "SELECT * FROM payments WHERE id=?", paymentId));
        String state = text(before, "status");
        if (!state.equals("PENDING") && !state.equals("REFUND_PENDING")) { return payment(before); }
        var outcome = state.equals("PENDING") ? provider.charge(paymentId) : provider.refund(paymentId);
        if (outcome.state().equals("UNKNOWN")) { return payment(before); }
        return database.transaction(c -> {
            var current = one(c, "SELECT * FROM payments WHERE id=?", paymentId);
            if (!state.equals(text(current, "status"))) { return payment(current); }
            String next = switch (outcome.state()) {
                case "CAPTURED" -> "PAID"; case "DECLINED" -> "DECLINED";
                case "REFUNDED" -> "REFUNDED"; default -> throw new IllegalStateException("Unexpected PSP outcome");
            };
            update(c, "UPDATE payments SET status=?,provider_reference=? WHERE id=?", next, outcome.reference(), paymentId);
            String orderId = text(current, "order_id");
            if (next.equals("PAID")) {
                update(c, "UPDATE orders SET status='PAID' WHERE id=?", orderId);
                update(c, "UPDATE coupons SET status='USED' WHERE payment_id=? AND status='RESERVED'", paymentId);
            } else if (next.equals("DECLINED")) { release(c, paymentId); }
            else {
                update(c, "UPDATE orders SET status='REFUNDED' WHERE id=?", orderId);
                // Promotion policy: a refunded coupon is void, preventing buy/refund/reuse loops.
                update(c, "UPDATE coupons SET status='VOID' WHERE payment_id=? AND status='USED'", paymentId);
            }
            audit(c, new Principal("system", "ADMIN"), "PSP_" + next, paymentId, Map.of("reference", outcome.reference()));
            return payment(c, new Principal(text(current, "user_id"), "CUSTOMER"), paymentId);
        });
    }

    public List<Map<String,Object>> audit(Principal actor, String paymentId) {
        admin(actor); id(paymentId, "entityId");
        return database.transaction(c -> rows(c, "SELECT * FROM audit_events WHERE entity_id=? ORDER BY sequence_id LIMIT 500", paymentId));
    }

    public long balance(Principal actor) {
        customer(actor);
        return database.transaction(c -> number(one(c, "SELECT balance_cents FROM wallets WHERE user_id=?", actor.userId()), "balance_cents"));
    }

    private FeatureSnapshot features(Connection c, Principal principal, String deviceId, OrderView order,
                                     String peerIp, String excludingPayment) throws Exception {
        String userId = principal.userId();
        var user = one(c, "SELECT * FROM users WHERE id=?", userId);
        var merchant = one(c, "SELECT * FROM merchants WHERE id=?", order.merchant());
        var devices = rows(c, "SELECT * FROM devices WHERE id=? AND user_id=?", deviceId, userId);
        var boundDevices = rows(c, "SELECT * FROM devices WHERE id=? AND user_id=?", principal.sessionDeviceId(), userId);
        var networks = rows(c, "SELECT * FROM networks WHERE ip=?", peerIp == null ? "" : peerIp);
        long now = clock.millis();
        int attempts = (int) number(one(c, "SELECT COUNT(*) AS n FROM payments WHERE user_id=? AND created_at>=? AND id<>?",
                userId, now - 600_000, excludingPayment == null ? "" : excludingPayment), "n");
        int users = (int) number(one(c, "SELECT COUNT(DISTINCT user_id) AS n FROM payments WHERE device_id=? AND created_at>=? AND user_id<>?",
                deviceId, now - 86_400_000, userId), "n");
        int declined = (int) number(one(c, "SELECT COUNT(*) AS n FROM payments WHERE user_id=? AND status IN ('DECLINED','BLOCKED') AND created_at>=? AND id<>?",
                userId, now - 3_600_000, excludingPayment == null ? "" : excludingPayment), "n");
        return new FeatureSnapshot(!"ACTIVE".equals(text(user, "status")), flag(user, "verified"),
                Math.max(0, now - number(user, "created_at")) / 3_600_000,
                Math.max(0, now - number(user, "reset_at")) / 60_000,
                deviceId.equals(principal.sessionDeviceId()) && !devices.isEmpty() && flag(devices.get(0), "trusted"),
                !devices.isEmpty() && flag(devices.get(0), "compromised")
                        || !boundDevices.isEmpty() && flag(boundDevices.get(0), "compromised"),
                flag(merchant, "blocked"), !networks.isEmpty(), !networks.isEmpty() && flag(networks.get(0), "proxy"),
                !networks.isEmpty() && !text(networks.get(0), "country").equals(text(user, "country")),
                order.amountCents(), number(user, "typical_cents"), attempts, users, declined,
                number(one(c, "SELECT balance_cents FROM wallets WHERE user_id=?", userId), "balance_cents"));
    }

    private String couponFailure(Connection c, String userId, String couponId, OrderView order, String reservedBy) throws Exception {
        var rows = rows(c, "SELECT q.*,b.merchant_id,b.currency,b.minimum_cents,b.discount_cents,b.starts_at,b.expires_at "
                + "FROM coupons q JOIN campaigns b ON q.campaign_id=b.id WHERE q.id=?", couponId);
        if (rows.isEmpty() || !userId.equals(text(rows.get(0), "user_id"))) { return "COUPON_NOT_OWNED"; }
        var coupon = rows.get(0);
        boolean correctState = reservedBy == null ? "AVAILABLE".equals(text(coupon, "status"))
                : "RESERVED".equals(text(coupon, "status")) && reservedBy.equals(text(coupon, "payment_id"));
        if (!correctState) { return "COUPON_UNAVAILABLE"; }
        if (clock.millis() < number(coupon, "starts_at") || clock.millis() >= number(coupon, "expires_at")) { return "COUPON_EXPIRED"; }
        if (!order.merchant().equals(text(coupon, "merchant_id")) || !order.currency().equals(text(coupon, "currency"))) {
            return "COUPON_SCOPE_MISMATCH";
        }
        if (order.amountCents() < number(coupon, "minimum_cents") || order.amountCents() < number(coupon, "discount_cents")) {
            return "COUPON_MINIMUM_NOT_MET";
        }
        return null;
    }

    private void release(Connection c, String paymentId) throws Exception {
        update(c, "UPDATE orders SET status='OPEN',payment_id=NULL WHERE payment_id=? AND status='PAYMENT_IN_PROGRESS'", paymentId);
        update(c, "UPDATE coupons SET status='AVAILABLE',payment_id=NULL WHERE payment_id=? AND status='RESERVED'", paymentId);
    }

    private OrderView order(Connection c, Principal actor, String id) throws Exception {
        var row = one(c, "SELECT * FROM orders WHERE id=? AND user_id=?", id, actor.userId());
        return new OrderView(id, text(row, "user_id"), text(row, "merchant_id"), text(row, "currency"),
                number(row, "amount_cents"), text(row, "status"), text(row, "payment_id"));
    }
    private CouponView coupon(Connection c, Principal actor, String id) throws Exception {
        var row = one(c, "SELECT q.*,b.discount_cents,b.expires_at FROM coupons q JOIN campaigns b ON b.id=q.campaign_id WHERE q.id=? AND q.user_id=?", id, actor.userId());
        return new CouponView(id, actor.userId(), text(row, "campaign_id"), text(row, "status"),
                number(row, "discount_cents"), number(row, "expires_at"), text(row, "payment_id"));
    }
    private PaymentView payment(Connection c, Principal actor, String id) throws Exception {
        var row = one(c, "SELECT * FROM payments WHERE id=? AND user_id=?", id, actor.userId());
        return payment(row);
    }
    private static PaymentView payment(Map<String,Object> row) {
        return new PaymentView(text(row, "id"), text(row, "user_id"), text(row, "order_id"), number(row, "gross_cents"),
                number(row, "discount_cents"), number(row, "payable_cents"), text(row, "status"),
                decode(text(row, "decision_json"), Decision.class), text(row, "provider_reference"), number(row, "created_at"));
    }
    private void audit(Connection c, Principal actor, String kind, String entityId, Object detail) throws Exception {
        update(c, "INSERT INTO audit_events(actor,kind,entity_id,detail_json,happened_at) VALUES(?,?,?,?,?)",
                actor.userId(), kind, entityId, json(detail), clock.millis());
    }
    private void activeUser(Connection c, String userId) throws Exception {
        if (!"ACTIVE".equals(text(one(c, "SELECT * FROM users WHERE id=?", userId), "status"))) {
            throw new ApiException(403, "ACCOUNT_BLOCKED", "Account is not active");
        }
    }
    private static void sameRequest(Map<String,Object> previous, String fingerprint) {
        if (!fingerprint.equals(text(previous, "request_hash"))) { throw conflict("IDEMPOTENCY_CONFLICT", "Key already bound to different parameters"); }
    }
    private static String newId() { return UUID.randomUUID().toString(); }
    private static ApiException unauthorized() { return new ApiException(401, "UNAUTHORIZED", "Valid bearer token required"); }
    private static ApiException invalid(String message) { return new ApiException(400, "INVALID_INPUT", message); }
    private static ApiException conflict(String code, String message) { return new ApiException(409, code, message); }
    public static void admin(Principal actor) {
        if (!actor.role().equals("ADMIN")) { throw new ApiException(403, "FORBIDDEN", "Operator credential required"); }
    }
    private static void customer(Principal actor) {
        if (!actor.role().equals("CUSTOMER")) { throw new ApiException(403, "FORBIDDEN", "Customer credential required"); }
    }
}
