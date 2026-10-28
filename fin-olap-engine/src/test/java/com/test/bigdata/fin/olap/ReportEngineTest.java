package com.test.bigdata.fin.olap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CancellationException;

import static org.junit.jupiter.api.Assertions.*;

class ReportEngineTest {
    @TempDir Path directory;
    private static final String HEADER = String.join(",", CsvTransactionSource.HEADER) + "\n";
    private final ReportEngine engine = new ReportEngine();

    private ReportQuery query(long rows, int groups) {
        return new ReportQuery("snapshot-test", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 31),
                rows, groups, Duration.ofSeconds(30));
    }

    private Path csv(String body) throws IOException {
        return Files.writeString(directory.resolve("transactions.csv"), HEADER + body);
    }

    @Test void filtersInclusiveDatesAndKeepsCurrenciesAndInstitutionsSeparate() throws Exception {
        var result = engine.execute(csv("t1,2026-01-01,a,buy-bank,PAY,CNY,100\n"
                + "t2,2026-01-01,a,buy-bank,PAY,CNY,-25\n"
                + "t3,2026-01-01,a,buy-bank,PAY,USD,90\n"
                + "t4,2026-01-31,a,other-bank,PAY,CNY,500\n"
                + "t5,2026-02-01,a,buy-bank,PAY,CNY,999\n"), query(5, 3));
        assertEquals(5, result.scannedRows());
        assertEquals(4, result.matchedRows());
        assertEquals(3, result.rows().size());
        assertEquals(75, result.rows().get(0).totalAmountCents());
        assertEquals(2, result.rows().get(0).tradeCount());
        assertEquals("CNY", result.rows().get(0).currency());
        assertEquals("USD", result.rows().get(1).currency());
        assertEquals("snapshot-test", result.snapshotId());
        assertEquals(64, result.sourceSha256().length());
        assertThrows(UnsupportedOperationException.class, () -> result.rows().clear());
    }

    @Test void reconcilesRandomAmountsAgainstIndependentBigIntegerReference() throws Exception {
        var random = new Random(20261026);
        var input = new StringBuilder();
        var amounts = new HashMap<String, BigInteger>();
        var counts = new HashMap<String, Long>();
        for (int row = 0; row < 3000; row++) {
            String account = "account-" + random.nextInt(37);
            long amount = random.nextInt(2_000_001) - 1_000_000;
            amounts.merge(account, BigInteger.valueOf(amount), BigInteger::add);
            counts.merge(account, 1L, Long::sum);
            input.append(row).append(",2026-01-20,").append(account)
                    .append(",bank,TRANSFER,CNY,").append(amount).append('\n');
        }
        var result = engine.execute(csv(input.toString()), query(3000, 37));
        assertEquals(37, result.rows().size());
        for (var row : result.rows()) {
            assertEquals(amounts.get(row.accountId()).longValueExact(), row.totalAmountCents());
            assertEquals(counts.get(row.accountId()).longValue(), row.tradeCount());
        }
        assertEquals(result, engine.execute(directory.resolve("transactions.csv"), query(3000, 37)));
    }

    @Test void supportsQuotedCommaAndNewline() throws Exception {
        var result = engine.execute(csv("t,2026-01-10,\"account,\nquoted\",bank,PAY,CNY,5\n"), query(1, 1));
        assertEquals("account,\nquoted", result.rows().get(0).accountId());
        Path output = directory.resolve("report.csv");
        CsvReportWriter.write(output, result);
        assertTrue(Files.readString(output).contains("\"account,\nquoted\""));
    }

    @Test void headerOnlyInputProducesEmptyReport() throws Exception {
        var result = engine.execute(csv(""), query(1, 1));
        assertEquals(0, result.scannedRows());
        assertTrue(result.rows().isEmpty());
        CsvReportWriter.write(directory.resolve("empty.csv"), result);
        assertEquals(1, Files.readAllLines(directory.resolve("empty.csv")).size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"9223372036854775807,1", "-9223372036854775808,-1"})
    void rejectsSignedOverflow(String amounts) throws Exception {
        String[] pair = amounts.split(",");
        Path input = csv("t1,2026-01-01,a,b,PAY,CNY," + pair[0] + "\n"
                + "t2,2026-01-01,a,b,PAY,CNY," + pair[1] + "\n");
        assertThrows(ArithmeticException.class, () -> engine.execute(input, query(2, 1)));
        Files.delete(input); // Confirms the source was closed on the failure path, also on Windows.
    }

    @Test void rejectsGroupAndScanBudgetsWithoutReturningPartialResults() throws Exception {
        Path input = csv("t1,2026-01-01,a,b,PAY,CNY,1\nt2,2026-01-01,z,b,PAY,CNY,2\n");
        assertThrows(IllegalStateException.class, () -> engine.execute(input, query(2, 1)));
        assertThrows(IllegalStateException.class, () -> engine.execute(input, query(1, 2)));
    }

    @Test void honoursCancellationAndTimeout() throws Exception {
        Path input = csv("t,2026-01-01,a,b,PAY,CNY,1\n");
        Thread.currentThread().interrupt();
        try {
            assertThrows(CancellationException.class, () -> engine.execute(input, query(1, 1)));
        } finally {
            Thread.interrupted();
        }
        var expired = new ReportQuery("s", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 31),
                1, 1, Duration.ofNanos(1));
        assertThrows(IllegalStateException.class, () -> engine.execute(input, expired));
    }

    @ParameterizedTest
    @ValueSource(strings = {"t,2026-02-30,a,b,PAY,CNY,1\n", "t,2026-01-01,a,b,PAY,CNY,1.5\n",
            "t,2026-01-01,,b,PAY,CNY,1\n", "t,2026-01-01,a,b,PAY,cny,1\n",
            "t,2026-01-01,a,b,PAY,CNY,9223372036854775808\n", "t,2026-01-01,a\n", "\n"})
    void rejectsInvalidInput(String row) throws Exception {
        Path input = csv(row);
        assertThrows(IOException.class, () -> engine.execute(input, query(1, 1)));
    }

    @Test void rejectsWrongHeader() throws Exception {
        Path input = Files.writeString(directory.resolve("bad.csv"), "trade_id,trade_date\n");
        assertThrows(IOException.class, () -> engine.execute(input, query(1, 1)));
    }

    @Test void doesNotOverwriteAnExistingReport() throws Exception {
        var result = engine.execute(csv(""), query(1, 1));
        Path target = Files.writeString(directory.resolve("existing.csv"), "retained");
        assertThrows(IOException.class, () -> CsvReportWriter.write(target, result));
        assertEquals("retained", Files.readString(target));
        try (var files = Files.list(directory)) {
            assertFalse(files.anyMatch(path -> path.toString().endsWith(".tmp")));
        }
    }

    @Test void rejectsInvalidQuery() {
        assertThrows(IllegalArgumentException.class, () -> new ReportQuery("s", LocalDate.MAX,
                LocalDate.MIN, 1, 1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> query(0, 1));
        assertThrows(IllegalArgumentException.class, () -> query(1, 0));
    }
}
