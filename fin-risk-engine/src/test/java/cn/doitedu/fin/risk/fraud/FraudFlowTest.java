package cn.doitedu.fin.risk.fraud;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import static cn.doitedu.fin.risk.fraud.Contracts.*;
import static cn.doitedu.fin.risk.fraud.Database.*;
import static org.junit.jupiter.api.Assertions.*;

class FraudFlowTest {
    static final Principal ALICE = new Principal("alice", "CUSTOMER");
    static final Principal BOB = new Principal("bob", "CUSTOMER");
    static final Principal ADMIN = new Principal("operator", "ADMIN");
    static final Map<String,String> TOKENS = Map.of("alice", "alice-" + "a".repeat(40),
            "bob", "bob-" + "b".repeat(40), "newbie", "new-" + "c".repeat(40), "operator", "admin-" + "d".repeat(40));
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-27T00:00:00Z"), ZoneOffset.UTC);
    Database db;
    FraudService service;
    @TempDir Path directory;

    @BeforeEach void setup() {
        db = new Database("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000");
        DemoBootstrap.initialise(db, CLOCK, TOKENS); service = new FraudService(db, CLOCK);
    }
    private String key() { return UUID.randomUUID().toString(); }
    private OrderView order(String product) { return service.createOrder(ALICE, key(), new OrderRequest(product, 1)); }
    private CouponView claim(String campaign) {
        return service.claim(ALICE, key(), new ClaimRequest(campaign, "alice-phone")).coupon();
    }
    private PaymentView pay(OrderView order, String coupon) {
        return service.pay(ALICE, key(), new PaymentRequest(order.id(), "alice-phone", coupon), "127.0.0.1");
    }
    private void sql(String sql, Object... args) { db.transaction(c -> update(c, sql, args)); }

    @Test void happyPathClaimsPaysAndRefundsNetMoneyWithCouponVoided() {
        var coupon = claim("welcome");
        long before = service.balance(ALICE);
        var payment = pay(order("approve"), coupon.id());
        assertEquals("PAID", payment.status());
        assertEquals(10_000, payment.grossCents()); assertEquals(2_000, payment.discountCents());
        assertEquals(8_000, payment.payableCents()); assertEquals(before - 8_000, service.balance(ALICE));
        assertEquals("USED", service.getCoupon(ALICE, coupon.id()).status());
        assertEquals("REFUNDED", service.refund(ALICE, payment.id()).status());
        assertEquals(before, service.balance(ALICE));
        assertEquals("REFUNDED", service.refund(ALICE, payment.id()).status());
        assertEquals(before, service.balance(ALICE));
        assertEquals("VOID", service.getCoupon(ALICE, coupon.id()).status());
        assertEquals("BLOCKED", pay(order("approve"), coupon.id()).status());
        assertTrue(service.audit(ADMIN, payment.id()).size() >= 4);
    }

    @Test void stolenCouponCannotBeUsedByAnotherAccount() {
        var coupon = claim("welcome");
        var order = service.createOrder(BOB, key(), new OrderRequest("approve", 1));
        long balance = service.balance(BOB);
        var attack = service.pay(BOB, key(), new PaymentRequest(order.id(), "bob-phone", coupon.id()), "127.0.0.1");
        assertEquals("BLOCKED", attack.status());
        assertEquals(List.of("COUPON_NOT_OWNED"), attack.decision().reasons());
        assertEquals(balance, service.balance(BOB));
        assertEquals("AVAILABLE", service.getCoupon(ALICE, coupon.id()).status());
    }

    @Test void idempotencyBindsParametersAndOwner() {
        String key = key();
        var request = new OrderRequest("approve", 1);
        var order = service.createOrder(ALICE, key, request);
        assertEquals(order, service.createOrder(ALICE, key, request));
        assertThrows(ApiException.class, () -> service.createOrder(ALICE, key, new OrderRequest("small", 1)));
        var command = new PaymentRequest(order.id(), "alice-phone", null);
        var paid = service.pay(ALICE, key, command, "127.0.0.1");
        assertEquals(paid, service.pay(ALICE, key, command, "127.0.0.1"));
        assertThrows(ApiException.class, () -> service.pay(ALICE, key,
                new PaymentRequest(order.id(), "different-device", null), "127.0.0.1"));
        assertThrows(ApiException.class, () -> service.getOrder(BOB, order.id()));
        assertThrows(ApiException.class, () -> service.getPayment(BOB, paid.id()));
        assertThrows(ApiException.class, () -> service.refund(BOB, paid.id()));
    }

    @Test void hardBlockedAccountMerchantAndDeviceNeverReachProvider() {
        var order = order("approve");
        sql("UPDATE users SET status='BLOCKED' WHERE id='alice'");
        assertEquals("BLOCKED", pay(order, null).status());
        sql("UPDATE users SET status='ACTIVE' WHERE id='alice'");
        sql("UPDATE devices SET compromised=TRUE WHERE user_id='alice'");
        assertEquals("BLOCKED", pay(order, null).status());
        var renamedDevice = service.pay(ALICE, key(), new PaymentRequest(order.id(), "made-up-device", null), "127.0.0.1");
        assertEquals("BLOCKED", renamedDevice.status());
        assertTrue(renamedDevice.decision().reasons().contains("DEVICE_COMPROMISED"));
        sql("UPDATE devices SET compromised=FALSE WHERE user_id='alice'");
        sql("UPDATE merchants SET blocked=TRUE WHERE id='shop-approve'");
        assertEquals("BLOCKED", pay(order, null).status());
        assertEquals(0L, db.<Long>transaction(c -> number(one(c, "SELECT COUNT(*) n FROM provider_ledger"), "n")).longValue());
    }

    @Test void mediumRiskIsUnchargedUntilIndependentOperatorReview() {
        var order = order("large");
        long before = service.balance(ALICE);
        var payment = pay(order, null);
        assertEquals("REVIEW", payment.status()); assertEquals(before, service.balance(ALICE));
        assertThrows(ApiException.class, () -> service.review(ALICE, payment.id(), new ReviewRequest(true, "self approval")));
        assertEquals("PAID", service.review(ADMIN, payment.id(), new ReviewRequest(true, "Verified via independent demo review")).status());
        assertEquals(before - order.amountCents(), service.balance(ALICE));
    }

    @Test void reviewRechecksBlockedUserAndExpiredCoupon() {
        var coupon = claim("welcome");
        var order = order("approve");
        var payment = service.pay(ALICE, key(), new PaymentRequest(order.id(), "unfamiliar", coupon.id()), "127.0.0.1");
        assertEquals("REVIEW", payment.status());
        assertEquals("RESERVED", service.getCoupon(ALICE, coupon.id()).status());
        sql("UPDATE campaigns SET expires_at=? WHERE id='welcome'", CLOCK.millis());
        var reviewed = service.review(ADMIN, payment.id(), new ReviewRequest(true, "Checked customer"));
        assertEquals("BLOCKED", reviewed.status());
        assertEquals("AVAILABLE", service.getCoupon(ALICE, coupon.id()).status());
        assertEquals("OPEN", service.getOrder(ALICE, order.id()).status());
    }

    @Test void unknownNetworkAndRecentResetRequireReview() {
        var order = order("approve");
        var payment = service.pay(ALICE, key(), new PaymentRequest(order.id(), "alice-phone", null), "203.0.113.99");
        assertEquals("REVIEW", payment.status()); assertTrue(payment.decision().reasons().contains("UNKNOWN_NETWORK"));
        service.review(ADMIN, payment.id(), new ReviewRequest(false, "Unverified network"));
        sql("UPDATE users SET reset_at=? WHERE id='alice'", CLOCK.millis());
        var reset = pay(order, null);
        assertEquals("REVIEW", reset.status()); assertTrue(reset.decision().reasons().contains("RECENT_PASSWORD_RESET"));
    }

    @Test void mixedProxyDeviceNewAccountSignalsBlock() {
        var user = new Principal("newbie", "CUSTOMER");
        var order = service.createOrder(user, key(), new OrderRequest("approve", 1));
        var payment = service.pay(user, key(), new PaymentRequest(order.id(), "unknown", null), "198.51.100.10");
        assertEquals("BLOCKED", payment.status()); assertTrue(payment.decision().score() >= 75);
        assertTrue(payment.decision().reasons().contains("COUNTRY_MISMATCH"));
    }

    @Test void velocityUsesAllAttemptsAndCannotBeBypassedWithNewKeys() {
        for (int index = 0; index < 3; index++) { assertEquals("PAID", pay(order("small"), null).status()); }
        var review = pay(order("small"), null); // 25 velocity points is still below review threshold.
        assertEquals("PAID", review.status());
        pay(order("small"), null);
        var blocked = pay(order("small"), null);
        assertEquals("BLOCKED", blocked.status());
        assertTrue(blocked.decision().reasons().contains("ACCOUNT_VELOCITY_10M"));
    }

    @Test void declinesReleaseCouponAndThreeDeclinesBlockFurtherAttempts() {
        var coupon = claim("decline-promo");
        long balance = service.balance(ALICE);
        var first = pay(order("decline"), coupon.id());
        assertEquals("DECLINED", first.status()); assertEquals(balance, service.balance(ALICE));
        assertEquals("AVAILABLE", service.getCoupon(ALICE, coupon.id()).status());
        pay(order("decline"), null); pay(order("decline"), null);
        var fourth = pay(order("approve"), null);
        assertEquals("BLOCKED", fourth.status()); assertTrue(fourth.decision().reasons().contains("REPEATED_DECLINES_1H"));
    }

    @Test void timeoutAfterCaptureKeepsCouponReservedUntilReconciliationAndNeverDoubleDebits() {
        var coupon = claim("timeout-promo"); var order = order("timeout_after_capture");
        long balance = service.balance(ALICE);
        String key = key(); var request = new PaymentRequest(order.id(), "alice-phone", coupon.id());
        var uncertain = service.pay(ALICE, key, request, "127.0.0.1");
        assertEquals("PENDING", uncertain.status()); assertEquals(balance - 8_000, service.balance(ALICE));
        assertEquals("RESERVED", service.getCoupon(ALICE, coupon.id()).status());
        var replay = service.pay(ALICE, key, request, "127.0.0.1");
        assertEquals("PAID", replay.status()); assertEquals(balance - 8_000, service.balance(ALICE));
        assertEquals("USED", service.getCoupon(ALICE, coupon.id()).status());
        assertEquals(1L, db.<Long>transaction(c -> number(one(c, "SELECT COUNT(*) n FROM provider_ledger"), "n")).longValue());
    }

    @Test void providerOutageFailsClosedAndRecovers() {
        var payment = pay(order("unavailable"), null);
        assertEquals("PENDING", payment.status()); assertEquals(10_000_000, service.balance(ALICE));
        assertThrows(ApiException.class, () -> service.refund(ALICE, payment.id()));
        assertEquals(0, service.reconcile(ADMIN));
        sql("UPDATE merchants SET provider_mode='APPROVE' WHERE id='shop-unavailable'");
        assertEquals(1, service.reconcile(ADMIN)); assertEquals("PAID", service.getPayment(ALICE, payment.id()).status());
    }

    @Test void concurrentSameRequestProducesOneCharge() throws Exception {
        var order = order("approve"); String key = key();
        var pool = Executors.newFixedThreadPool(8);
        try {
            List<Callable<PaymentView>> tasks = new ArrayList<>();
            for (int index = 0; index < 16; index++) {
                tasks.add(() -> service.pay(ALICE, key, new PaymentRequest(order.id(), "alice-phone", null), "127.0.0.1"));
            }
            Set<String> ids = new HashSet<>();
            for (var future : pool.invokeAll(tasks)) { ids.add(future.get().id()); }
            assertEquals(1, ids.size()); assertEquals(9_990_000, service.balance(ALICE));
            assertEquals(1L, db.<Long>transaction(c -> number(one(c, "SELECT COUNT(*) n FROM provider_ledger"), "n")).longValue());
        } finally { pool.shutdownNow(); }
    }

    @Test void concurrentCouponUseHasOneWinnerEvenAcrossDifferentOrdersAndKeys() throws Exception {
        var coupon = claim("welcome"); var first = order("approve"); var second = order("approve");
        var pool = Executors.newFixedThreadPool(2);
        try {
            var results = pool.invokeAll(List.<Callable<PaymentView>>of(() -> pay(first, coupon.id()), () -> pay(second, coupon.id())));
            var statuses = Set.of(results.get(0).get().status(), results.get(1).get().status());
            assertEquals(Set.of("PAID", "BLOCKED"), statuses);
            assertEquals(9_992_000, service.balance(ALICE));
        } finally { pool.shutdownNow(); }
    }

    @Test void oneDeviceCannotFarmCampaignAcrossAccountsAndStockCannotGoNegative() throws Exception {
        sql("INSERT INTO devices VALUES('alice-phone','bob',TRUE,FALSE)");
        var pool = Executors.newFixedThreadPool(2);
        try {
            var results = pool.invokeAll(List.<Callable<ClaimResult>>of(
                    () -> service.claim(ALICE, key(), new ClaimRequest("welcome", "alice-phone")),
                    () -> service.claim(new Principal("bob", "CUSTOMER", "alice-phone"), key(), new ClaimRequest("welcome", "alice-phone"))));
            assertEquals(Set.of("CLAIMED", "BLOCKED"), Set.of(results.get(0).get().status(), results.get(1).get().status()));
        } finally { pool.shutdownNow(); }
        sql("UPDATE campaigns SET remaining=0 WHERE id='decline-promo'");
        assertEquals("STOCK_EXHAUSTED", service.claim(ALICE, key(), new ClaimRequest("decline-promo", "alice-phone")).reason());
    }

    @Test void couponThresholdMerchantExpiryAndClaimReplayAreEnforced() {
        String key = key(); var request = new ClaimRequest("welcome", "alice-phone");
        var claimed = service.claim(ALICE, key, request);
        assertEquals(claimed, service.claim(ALICE, key, request));
        assertThrows(ApiException.class, () -> service.claim(ALICE, key, new ClaimRequest("decline-promo", "alice-phone")));
        assertEquals("COUPON_MINIMUM_NOT_MET", pay(order("small"), claimed.coupon().id()).decision().reasons().get(0));
        assertEquals("COUPON_SCOPE_MISMATCH", pay(order("decline"), claimed.coupon().id()).decision().reasons().get(0));
        sql("UPDATE campaigns SET expires_at=? WHERE id='welcome'", CLOCK.millis());
        assertEquals("COUPON_EXPIRED", pay(order("approve"), claimed.coupon().id()).decision().reasons().get(0));
    }

    @Test void restartRecoversCommittedProviderCaptureAndRefund() {
        String url = "jdbc:h2:file:" + directory.resolve("recovery").toString().replace('\\', '/');
        var persistent = new Database(url); DemoBootstrap.initialise(persistent, CLOCK, TOKENS);
        var original = new FraudService(persistent, CLOCK);
        var order = original.createOrder(ALICE, key(), new OrderRequest("timeout_after_capture", 1));
        var pending = original.pay(ALICE, key(), new PaymentRequest(order.id(), "alice-phone", null), "127.0.0.1");
        assertEquals("PENDING", pending.status());
        var restarted = new FraudService(new Database(url), CLOCK);
        assertEquals(1, restarted.reconcile(ADMIN)); assertEquals("PAID", restarted.getPayment(ALICE, pending.id()).status());
        assertEquals(9_990_000, restarted.balance(ALICE));
        assertEquals("REFUNDED", restarted.refund(ALICE, pending.id()).status());
        assertEquals(10_000_000, new FraudService(new Database(url), CLOCK).balance(ALICE));
    }

    @Test void authenticationOnlyAcceptsSeededSecretsAndCustomerCannotReadAudit() {
        assertEquals(ALICE, service.authenticate(TOKENS.get("alice")));
        assertThrows(ApiException.class, () -> service.authenticate("x".repeat(40)));
        assertThrows(ApiException.class, () -> service.reconcile(ALICE));
        var payment = pay(order("approve"), null);
        assertThrows(ApiException.class, () -> service.audit(ALICE, payment.id()));
        assertTrue(service.audit(ADMIN, payment.id()).stream().anyMatch(row -> row.get("kind").equals("PSP_PAID")));
    }

    @Test void cancellationReleasesReviewReservationButCannotCancelUnknownCapture() {
        var coupon = claim("welcome"); var order = order("approve");
        var review = service.pay(ALICE, key(), new PaymentRequest(order.id(), "unknown", coupon.id()), "127.0.0.1");
        assertEquals("CANCELLED", service.cancelReview(ALICE, review.id()).status());
        assertEquals("CANCELLED", service.cancelReview(ALICE, review.id()).status());
        assertEquals("AVAILABLE", service.getCoupon(ALICE, coupon.id()).status());
        var uncertain = pay(order("timeout_after_capture"), null);
        assertThrows(ApiException.class, () -> service.cancelReview(ALICE, uncertain.id()));
    }

    @Test void restartRecoversRefundCommittedBeforeApplicationFinalisation() {
        var payment = pay(order("approve"), null);
        sql("UPDATE payments SET status='REFUND_PENDING' WHERE id=?", payment.id());
        new SandboxProvider(db).refund(payment.id()); // PSP commits, application is assumed to crash here.
        var restarted = new FraudService(db, CLOCK);
        assertEquals(1, restarted.reconcile(ADMIN));
        assertEquals("REFUNDED", restarted.getPayment(ALICE, payment.id()).status());
        assertEquals(10_000_000, restarted.balance(ALICE));
        restarted.refund(ALICE, payment.id()); assertEquals(10_000_000, restarted.balance(ALICE));
    }

    @Test void campaignLastStockIsIssuedOnceUnderConcurrentDifferentDevices() throws Exception {
        sql("UPDATE campaigns SET remaining=1 WHERE id='welcome'");
        var pool = Executors.newFixedThreadPool(2);
        try {
            var results = pool.invokeAll(List.<Callable<ClaimResult>>of(
                    () -> service.claim(ALICE, key(), new ClaimRequest("welcome", "alice-phone")),
                    () -> service.claim(BOB, key(), new ClaimRequest("welcome", "bob-phone"))));
            assertEquals(Set.of("CLAIMED", "BLOCKED"), Set.of(results.get(0).get().status(), results.get(1).get().status()));
            long stock = db.transaction(c -> number(one(c, "SELECT remaining FROM campaigns WHERE id='welcome'"), "remaining"));
            assertEquals(0, stock);
        } finally { pool.shutdownNow(); }
    }

    @Test void drainingWalletTriggersReviewBeforeAnyDebit() {
        sql("UPDATE wallets SET balance_cents=10000 WHERE user_id='alice'");
        var result = pay(order("approve"), null);
        assertEquals("REVIEW", result.status());
        assertTrue(result.decision().reasons().contains("BALANCE_DRAIN"));
        assertEquals(10_000, service.balance(ALICE));
    }
}
