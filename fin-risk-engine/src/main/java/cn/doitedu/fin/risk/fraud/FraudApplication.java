package cn.doitedu.fin.risk.fraud;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public final class FraudApplication {
    private FraudApplication() { }
    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("dataset")) {
            PaySimTool.main(java.util.Arrays.copyOfRange(args, 1, args.length)); return;
        }
        if (args.length > 0 && args[0].equals("pricing")) {
            cn.doitedu.fin.risk.RiskApplication.main(java.util.Arrays.copyOfRange(args, 1, args.length)); return;
        }
        if (args.length != 2 || !args[0].equals("serve-demo")) {
            throw new IllegalArgumentException("Usage: serve-demo <database-directory> | dataset download/replay ... | pricing ...");
        }
        Path directory = Path.of(args[1]).toAbsolutePath().normalize();
        Files.createDirectories(directory);
        var database = new Database("jdbc:h2:file:" + directory.resolve("risk").toString().replace('\\', '/') + ";LOCK_TIMEOUT=10000");
        var clock = Clock.systemUTC();
        DemoBootstrap.initialise(database, clock, Map.of("alice", env("RISK_ALICE_TOKEN"), "bob", env("RISK_BOB_TOKEN"),
                "newbie", env("RISK_NEWBIE_TOKEN"), "operator", env("RISK_ADMIN_TOKEN")));
        var service = new FraudService(database, clock);
        var server = new FraudHttpServer(service, Integer.parseInt(System.getenv().getOrDefault("RISK_PORT", "8088")));
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
        System.out.println("Risk sandbox API http://127.0.0.1:" + server.port()
                + " (CPU only, fake money; use independent operator credential for review)");
        new CountDownLatch(1).await();
    }
    private static String env(String key) {
        String value = System.getenv(key);
        if (value == null || value.length() < 32) { throw new IllegalArgumentException(key + " must contain a random token (>=32 chars)"); }
        return value;
    }
}
