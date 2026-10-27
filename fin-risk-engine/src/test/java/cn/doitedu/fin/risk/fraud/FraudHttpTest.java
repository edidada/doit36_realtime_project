package cn.doitedu.fin.risk.fraud;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.*;
import java.util.UUID;

import static cn.doitedu.fin.risk.fraud.Database.*;
import static org.junit.jupiter.api.Assertions.*;

class FraudHttpTest {
    @Test void realHttpRunsCompleteCouponPaymentRefundAndEnforcesAuthAndOwnership() throws Exception {
        var db = new Database("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        DemoBootstrap.initialise(db, FraudFlowTest.CLOCK, FraudFlowTest.TOKENS);
        try (var server = new FraudHttpServer(new FraudService(db, FraudFlowTest.CLOCK), 0)) {
            server.start();
            var client = HttpClient.newHttpClient();
            String root = "http://127.0.0.1:" + server.port();
            assertEquals(401, send(client, root, "/v1/wallet", "GET", null, null, null).statusCode());
            var order = send(client, root, "/v1/orders", "POST", "alice", "order",
                    "{\"productId\":\"approve\",\"quantity\":1}");
            assertEquals(201, order.statusCode(), order.body());
            String orderId = JSON.readTree(order.body()).get("id").asText();
            assertEquals(404, send(client, root, "/v1/orders/" + orderId, "GET", "bob", null, null).statusCode());
            var coupon = send(client, root, "/v1/coupons/claim", "POST", "alice", "claim",
                    "{\"campaignId\":\"welcome\",\"deviceId\":\"alice-phone\"}");
            assertEquals(200, coupon.statusCode(), coupon.body());
            String couponId = JSON.readTree(coupon.body()).get("coupon").get("id").asText();
            String payload = "{\"orderId\":\"" + orderId + "\",\"deviceId\":\"alice-phone\",\"couponId\":\"" + couponId + "\"}";
            var paid = send(client, root, "/v1/payments", "POST", "alice", "pay", payload);
            assertEquals(200, paid.statusCode(), paid.body());
            assertEquals("PAID", JSON.readTree(paid.body()).get("status").asText());
            String paymentId = JSON.readTree(paid.body()).get("id").asText();
            assertEquals(paid.body(), send(client, root, "/v1/payments", "POST", "alice", "pay", payload).body());
            assertEquals(403, send(client, root, "/v1/payments/" + paymentId + "/audit", "GET", "alice", null, null).statusCode());
            assertEquals(200, send(client, root, "/v1/payments/" + paymentId + "/audit", "GET", "operator", null, null).statusCode());
            var refunded = send(client, root, "/v1/payments/" + paymentId + "/refund", "POST", "alice", null, "{}");
            assertEquals("REFUNDED", JSON.readTree(refunded.body()).get("status").asText());
            assertFalse(paid.body().contains(FraudFlowTest.TOKENS.get("alice")));
        }
    }

    @Test void rejectsForgedRiskFlagsAmountsMalformedJsonAndUntrustedProxyHeaders() throws Exception {
        var db = new Database("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        DemoBootstrap.initialise(db, FraudFlowTest.CLOCK, FraudFlowTest.TOKENS);
        try (var server = new FraudHttpServer(new FraudService(db, FraudFlowTest.CLOCK), 0)) {
            server.start(); var client = HttpClient.newHttpClient(); String root = "http://127.0.0.1:" + server.port();
            for (String body : new String[]{"{\"productId\":\"approve\",\"quantity\":1,\"amountCents\":1}",
                    "{\"productId\":\"approve\",\"quantity\":1,\"verified\":true}",
                    "{\"productId\":\"approve\",\"quantity\":null}",
                    "{\"productId\":\"approve\",\"quantity\":1.5}",
                    "{\"productId\":\"approve\",\"quantity\":1,\"quantity\":2}", "{} {}", "null"}) {
                assertEquals(400, send(client, root, "/v1/orders", "POST", "alice", UUID.randomUUID().toString(), body).statusCode(), body);
            }
            assertEquals(413, send(client, root, "/v1/orders", "POST", "alice", "large-body", " ".repeat(8200)).statusCode());
            assertEquals(400, send(client, root, "/v1/orders", "POST", "alice", null,
                    "{\"productId\":\"approve\",\"quantity\":1}").statusCode());
            assertEquals(403, send(client, root, "/v1/orders", "POST", "operator", "admin-order",
                    "{\"productId\":\"approve\",\"quantity\":1}").statusCode());
            var orderResponse = send(client, root, "/v1/orders", "POST", "alice", "proxy-order",
                    "{\"productId\":\"approve\",\"quantity\":1}");
            String orderId = JSON.readTree(orderResponse.body()).get("id").asText();
            var forgedProxy = HttpRequest.newBuilder(URI.create(root + "/v1/payments"))
                    .header("Authorization", "Bearer " + FraudFlowTest.TOKENS.get("alice"))
                    .header("Idempotency-Key", "proxy-pay").header("Content-Type", "application/json")
                    .header("X-Forwarded-For", "198.51.100.10")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"orderId\":\"" + orderId + "\",\"deviceId\":\"alice-phone\"}"))
                    .build();
            var response = client.send(forgedProxy, HttpResponse.BodyHandlers.ofString());
            assertEquals("PAID", JSON.readTree(response.body()).get("status").asText()); // Actual peer is loopback.
        }
    }

    static HttpResponse<String> send(HttpClient client, String root, String path, String method, String user, String key, String body) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(root + path)).timeout(java.time.Duration.ofSeconds(15));
        if (user != null) { builder.header("Authorization", "Bearer " + FraudFlowTest.TOKENS.get(user)); }
        if (key != null) { builder.header("Idempotency-Key", key); }
        if (body != null) { builder.header("Content-Type", "application/json"); }
        builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
}
