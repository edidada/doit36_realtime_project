package com.test.bigdata.fin.risk.fraud;

import java.time.Clock;
import java.util.Map;

import static com.test.bigdata.fin.risk.fraud.Database.*;

/** Explicit local synthetic fixtures only. No public registration or client-controlled risk attributes. */
public final class DemoBootstrap {
    private DemoBootstrap() { }
    public static void initialise(Database database, Clock clock, Map<String,String> tokens) {
        if (!tokens.keySet().containsAll(java.util.Set.of("alice", "bob", "newbie", "operator"))) {
            throw new IllegalArgumentException("Demo requires alice, bob, newbie and operator tokens");
        }
        if (tokens.values().stream().anyMatch(token -> token == null || token.length() < 32 || token.length() > 256)
                || tokens.values().stream().distinct().count() != tokens.size()) {
            throw new IllegalArgumentException("Use distinct randomly generated tokens of at least 32 characters");
        }
        database.transaction(c -> {
            if (!rows(c, "SELECT id FROM users LIMIT 1").isEmpty()) {
                for (var entry : tokens.entrySet()) {
                    if (rows(c, "SELECT user_id FROM api_credentials WHERE token_hash=? AND user_id=?",
                            hash(entry.getValue()), entry.getKey()).isEmpty()) {
                        throw new IllegalStateException("Existing demo DB uses different credentials; reuse its original environment");
                    }
                }
                return null;
            }
            long now = clock.millis();
            for (String user : tokens.keySet()) {
                update(c, "INSERT INTO users VALUES(?,?,?,?,?,?,?)", user, "ACTIVE", true,
                        now - (user.equals("newbie") ? 3_600_000L : 90L * 86_400_000), now - 86_400_000, 10_000, "CN");
                update(c, "INSERT INTO api_credentials VALUES(?,?,?,?)", hash(tokens.get(user)), user,
                        user.equals("operator") ? "ADMIN" : "CUSTOMER", user + "-phone");
                update(c, "INSERT INTO devices VALUES(?,?,?,?)", user + "-phone", user, true, false);
                update(c, "INSERT INTO wallets VALUES(?,?)", user, 10_000_000);
            }
            update(c, "INSERT INTO networks VALUES(?,?,?)", "127.0.0.1", "CN", false);
            update(c, "INSERT INTO networks VALUES(?,?,?)", "0:0:0:0:0:0:0:1", "CN", false);
            update(c, "INSERT INTO networks VALUES(?,?,?)", "198.51.100.10", "US", true);
            for (String mode : new String[]{"APPROVE", "DECLINE", "TIMEOUT_AFTER_CAPTURE", "UNAVAILABLE"}) {
                String merchant = "shop-" + mode.toLowerCase(java.util.Locale.ROOT);
                update(c, "INSERT INTO merchants VALUES(?,?,?)", merchant, false, mode);
                update(c, "INSERT INTO products VALUES(?,?,?,?)", mode.toLowerCase(java.util.Locale.ROOT), merchant, "CNY", 10_000);
            }
            update(c, "INSERT INTO products VALUES(?,?,?,?)", "large", "shop-approve", "CNY", 200_000);
            update(c, "INSERT INTO products VALUES(?,?,?,?)", "luxury", "shop-approve", "CNY", 1_100_000);
            update(c, "INSERT INTO products VALUES(?,?,?,?)", "small", "shop-approve", "CNY", 1_000);
            update(c, "INSERT INTO campaigns VALUES(?,?,?,?,?,?,?,?)", "welcome", "shop-approve", "CNY",
                    5_000, 2_000, now - 60_000, now + 7L * 86_400_000, 100);
            update(c, "INSERT INTO campaigns VALUES(?,?,?,?,?,?,?,?)", "timeout-promo", "shop-timeout_after_capture", "CNY",
                    5_000, 2_000, now - 60_000, now + 7L * 86_400_000, 100);
            update(c, "INSERT INTO campaigns VALUES(?,?,?,?,?,?,?,?)", "decline-promo", "shop-decline", "CNY",
                    5_000, 2_000, now - 60_000, now + 7L * 86_400_000, 100);
            return null;
        });
    }
}
