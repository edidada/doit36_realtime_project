package com.test.bigdata.fin.olap;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CancellationException;

/** Streaming scan with bounded group cardinality and checked fixed-point aggregation. */
public final class ReportEngine {
    public ReportResult execute(Path input, ReportQuery query) throws IOException {
        long started = System.nanoTime();
        Runnable checkBudget = () -> {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("Report interrupted");
            }
            if (System.nanoTime() - started >= query.timeout().toNanos()) {
                throw new IllegalStateException("Report timeout exceeded");
            }
        };
        String before = digest(input, checkBudget);
        Map<Key, Aggregate> groups = new HashMap<>();
        long scanned = 0;
        long matched = 0;
        try (TransactionSource source = open(input)) {
            Transaction trade;
            while ((trade = source.next()) != null) {
                checkBudget.run();
                if (scanned == query.maxScannedRows()) {
                    throw new IllegalStateException("Scanned row limit exceeded");
                }
                scanned++;
                if (trade.tradeDate().isBefore(query.startDate()) || trade.tradeDate().isAfter(query.endDate())) {
                    continue;
                }
                matched++;
                Key key = new Key(trade.tradeDate(), trade.accountId(), trade.institutionId(),
                        trade.transactionType(), trade.currency());
                Aggregate aggregate = groups.get(key);
                if (aggregate == null) {
                    if (groups.size() >= query.maxGroups()) {
                        throw new IllegalStateException("Group limit exceeded; reduce cardinality or increase maxGroups");
                    }
                    aggregate = new Aggregate();
                    groups.put(key, aggregate);
                }
                aggregate.add(trade.amountCents());
            }
        }
        String after = digest(input, checkBudget);
        if (!before.equals(after)) {
            throw new IOException("Input changed during execution; use an immutable snapshot");
        }
        var rows = new ArrayList<ReportRow>(groups.size());
        groups.forEach((key, value) -> rows.add(new ReportRow(key.date, key.account, key.institution,
                key.type, key.currency, value.count, value.sum)));
        rows.sort(Comparator.comparing(ReportRow::tradeDate).thenComparing(ReportRow::accountId)
                .thenComparing(ReportRow::institutionId).thenComparing(ReportRow::transactionType)
                .thenComparing(ReportRow::currency));
        checkBudget.run();
        return new ReportResult(query.snapshotId(), before, scanned, matched, rows);
    }

    private static TransactionSource open(Path input) throws IOException {
        String name = input.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv")) { return new CsvTransactionSource(input); }
        if (name.endsWith(".parquet")) { return new ParquetTransactionSource(input); }
        throw new IllegalArgumentException("Input extension must be .csv or .parquet");
    }

    private static String digest(Path path, Runnable checkBudget) throws IOException {
        MessageDigest hash;
        try {
            hash = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
        try (InputStream stream = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536];
            int read;
            while ((read = stream.read(buffer)) != -1) {
                checkBudget.run();
                hash.update(buffer, 0, read);
            }
        }
        checkBudget.run();
        return HexFormat.of().formatHex(hash.digest());
    }

    private record Key(LocalDate date, String account, String institution, String type, String currency) { }

    private static final class Aggregate {
        private long count;
        private long sum;

        void add(long amount) {
            count = Math.addExact(count, 1);
            sum = Math.addExact(sum, amount);
        }
    }
}
