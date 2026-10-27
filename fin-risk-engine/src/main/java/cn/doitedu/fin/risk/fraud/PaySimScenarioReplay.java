package cn.doitedu.fin.risk.fraud;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;

import static cn.doitedu.fin.risk.fraud.Contracts.*;
import static cn.doitedu.fin.risk.fraud.Database.*;

/** Maps the first 20 public rows into the real application workflow with explicitly synthetic profiles. */
public final class PaySimScenarioReplay {
    private PaySimScenarioReplay() { }
    public static Map<String,Object> run(Path input) throws Exception {
        var clock = Clock.systemUTC();
        var db = new Database("jdbc:h2:mem:public-replay-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        var service = new FraudService(db, clock);
        List<Map<String,Object>> outcomes = new ArrayList<>();
        try (var parser = PaySimTool.parse(Files.newBufferedReader(input, StandardCharsets.UTF_8))) {
            for (var row : parser) {
                int index = outcomes.size();
                long amount = PaySimTool.cents(row.get("amount"));
                long balance = PaySimTool.cents(row.get("oldbalanceOrg"));
                if (amount <= 0) { continue; }
                String user = "public-row-" + index;
                db.transaction(c -> {
                    update(c, "INSERT INTO users VALUES(?,?,?,?,?,?,?)", user, "ACTIVE", true,
                            clock.millis() - 90L * 86_400_000, clock.millis() - 86_400_000, 10_000, "CN");
                    update(c, "INSERT INTO devices VALUES(?,?,?,?)", user + "-device", user, true, false);
                    update(c, "INSERT INTO wallets VALUES(?,?)", user, balance);
                    if (index == 0) {
                        update(c, "INSERT INTO networks VALUES('127.0.0.1','CN',FALSE)");
                        update(c, "INSERT INTO merchants VALUES('public-replay-shop',FALSE,'APPROVE')");
                    }
                    update(c, "INSERT INTO products VALUES(?,?,?,?)", "public-product-" + index, "public-replay-shop", "CNY", amount);
                    return null;
                });
                var actor = new Principal(user, "CUSTOMER", user + "-device");
                var order = service.createOrder(actor, "order-" + index, new OrderRequest("public-product-" + index, 1));
                var payment = service.pay(actor, "pay-" + index,
                        new PaymentRequest(order.id(), user + "-device", null), "127.0.0.1");
                Map<String,Object> outcome = new LinkedHashMap<>();
                outcome.put("sourceRow", row.getRecordNumber()); outcome.put("sourceType", row.get("type"));
                outcome.put("sourceAmountMinorUnits", amount); outcome.put("sourceOldBalanceMinorUnits", balance);
                outcome.put("paymentStatus", payment.status()); outcome.put("decision", payment.decision());
                outcome.put("remainingSandboxBalance", service.balance(actor));
                outcomes.add(outcome);
                if (outcomes.size() == 20) { break; }
            }
        }
        return Map.of("sourceSha256", PaySimTool.digest(input), "cases", outcomes,
                "mapping", "Each source row is an independent scenario. Amount/old balance become sandbox order/wallet minor units. "
                        + "CNY, merchant, verified 90-day account, trusted device and loopback network are generated fixtures, not PaySim observations. "
                        + "isFraud is not consulted; no accuracy claim. REVIEW stays uncharged. Real payment flow, fake money only.");
    }
}
