package com.test.bigdata.fin.risk.fraud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static com.test.bigdata.fin.risk.fraud.Contracts.*;
import static com.test.bigdata.fin.risk.fraud.Database.*;

/** Loopback-only sandbox API, bounded executor/body sizes, bearer auth and role/ownership checks. */
public final class FraudHttpServer implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FraudHttpServer.class);
    private final FraudService service;
    private final HttpServer server;
    private final ThreadPoolExecutor executor;
    private final ScheduledExecutorService recovery = Executors.newSingleThreadScheduledExecutor();
    private final RateLimit limiter = new RateLimit();

    public FraudHttpServer(FraudService service, int port) throws IOException {
        this.service = service;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 32);
        executor = new ThreadPoolExecutor(4, 8, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(64), new ThreadPoolExecutor.CallerRunsPolicy());
        server.setExecutor(executor);
        server.createContext("/", this::handle);
    }

    public void start() {
        server.start();
        recovery.scheduleWithFixedDelay(() -> {
            try { service.reconcile(new Principal("recovery-worker", "ADMIN")); }
            catch (RuntimeException failure) { LOGGER.warn("risk recovery deferred; database unavailable"); }
        }, 5, 5, TimeUnit.SECONDS);
    }
    public int port() { return server.getAddress().getPort(); }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            String peer = exchange.getRemoteAddress().getAddress().getHostAddress();
            if (!limiter.allow("ip:" + peer, 240)) { throw new ApiException(429, "RATE_LIMIT", "Too many requests"); }
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            if (exchange.getRequestURI().getQuery() != null) { throw new ApiException(400, "INVALID_INPUT", "Queries are not accepted"); }
            if (path.equals("/health") && method.equals("GET")) {
                service.checkReady(); respond(exchange, 200, Map.of("status", "UP")); return;
            }
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            if (auth == null || !auth.startsWith("Bearer ")) { throw new ApiException(401, "UNAUTHORIZED", "Bearer token required"); }
            Principal principal = service.authenticate(auth.substring(7));
            if (!limiter.allow("user:" + principal.userId(), 120)) { throw new ApiException(429, "RATE_LIMIT", "Too many requests"); }
            String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
            Object result;
            int code = 200;
            if (method.equals("POST") && path.equals("/v1/orders")) {
                result = service.createOrder(principal, key, body(exchange, OrderRequest.class)); code = 201;
            } else if (method.equals("POST") && path.equals("/v1/coupons/claim")) {
                result = service.claim(principal, key, body(exchange, ClaimRequest.class));
            } else if (method.equals("POST") && path.equals("/v1/payments")) {
                result = service.pay(principal, key, body(exchange, PaymentRequest.class), peer);
            } else if (method.equals("POST") && path.equals("/v1/admin/reconcile")) {
                FraudService.admin(principal); body(exchange, Map.class);
                result = Map.of("completed", service.reconcile(principal));
            } else if (method.equals("GET") && path.equals("/v1/wallet")) {
                result = Map.of("balanceCents", service.balance(principal), "currency", "CNY");
            } else {
                String[] segments = path.split("/");
                if (segments.length == 4 && method.equals("GET")) {
                    result = switch (segments[2]) {
                        case "orders" -> service.getOrder(principal, segments[3]);
                        case "coupons" -> service.getCoupon(principal, segments[3]);
                        case "payments" -> service.getPayment(principal, segments[3]);
                        default -> throw notFound();
                    };
                } else if (segments.length == 5 && segments[2].equals("payments")) {
                    if (method.equals("POST") && segments[4].equals("review")) {
                        result = service.review(principal, segments[3], body(exchange, ReviewRequest.class));
                    } else if (method.equals("POST") && segments[4].equals("refund")) {
                        body(exchange, Map.class); result = service.refund(principal, segments[3]);
                    } else if (method.equals("POST") && segments[4].equals("cancel")) {
                        body(exchange, Map.class); result = service.cancelReview(principal, segments[3]);
                    } else if (method.equals("GET") && segments[4].equals("audit")) {
                        result = service.audit(principal, segments[3]);
                    } else { throw notFound(); }
                } else { throw notFound(); }
            }
            respond(exchange, code, result);
        } catch (ApiException failure) {
            respond(exchange, failure.status(), Map.of("code", failure.code(), "message", failure.getMessage()));
        } catch (JsonProcessingException | IllegalArgumentException failure) {
            respond(exchange, 400, Map.of("code", "INVALID_JSON", "message", "Invalid JSON fields or types"));
        } catch (Exception failure) {
            // No approval on errors. Stored intents remain available for recovery.
            LOGGER.error("risk request failed: {}", failure.getClass().getSimpleName());
            respond(exchange, 503, Map.of("code", "SERVICE_UNAVAILABLE", "message", "Retry using the same idempotency key"));
        } finally { exchange.close(); }
    }

    private static <T> T body(HttpExchange exchange, Class<T> type) throws IOException {
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType == null || !contentType.toLowerCase(java.util.Locale.ROOT).startsWith("application/json")) {
            throw new ApiException(415, "CONTENT_TYPE", "application/json required");
        }
        byte[] bytes = exchange.getRequestBody().readNBytes(8193);
        if (bytes.length > 8192) { throw new ApiException(413, "BODY_TOO_LARGE", "Maximum body is 8192 bytes"); }
        T value = JSON.readValue(bytes, type);
        if (value == null) { throw new ApiException(400, "INVALID_JSON", "JSON object required"); }
        return value;
    }

    private static ApiException notFound() { return new ApiException(404, "NOT_FOUND", "Route not found"); }
    private static void respond(HttpExchange exchange, int status, Object value) throws IOException {
        byte[] bytes = json(value).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    @Override public void close() {
        recovery.shutdownNow(); server.stop(0); executor.shutdownNow();
    }

    private static final class RateLimit {
        private final Map<String,long[]> windows = new HashMap<>();
        synchronized boolean allow(String key, int maximum) {
            long minute = Clock.systemUTC().millis() / 60_000;
            windows.entrySet().removeIf(entry -> entry.getValue()[0] < minute);
            if (!windows.containsKey(key) && windows.size() >= 2048) { return false; }
            long[] window = windows.computeIfAbsent(key, ignored -> new long[]{minute, 0});
            return ++window[1] <= maximum;
        }
    }
}
