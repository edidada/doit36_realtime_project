package com.test.bigdata.fin.risk.fraud;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.zip.ZipInputStream;

/** Streaming public-data acquisition/evaluation; labels are read only after prediction. CPU only. */
public final class PaySimTool {
    private static final List<String> HEADER = List.of("step", "type", "amount", "nameOrig", "oldbalanceOrg",
            "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud");
    private static final String URL = "https://www.kaggle.com/api/v1/datasets/download/ealaxi/paysim1?datasetVersionNumber=2";
    private static final String BASELINE = "paysim-pretransaction-baseline-1";
    private PaySimTool() { }

    public static void main(String[] args) throws Exception {
        if (args.length >= 2 && args[0].equals("download")) {
            download(Path.of(args[1]), args.length == 3 ? Integer.parseInt(args[2]) : 50_000);
        } else if (args.length == 3 && args[0].equals("replay")) {
            var metrics = replay(Path.of(args[1]));
            Files.writeString(Path.of(args[2]), Database.json(metrics), StandardOpenOption.CREATE_NEW);
            System.out.println(Database.json(metrics));
        } else if (args.length == 3 && args[0].equals("benchmark")) {
            var result = PaySimBenchmark.run(Path.of(args[1]));
            Files.writeString(Path.of(args[2]), Database.json(result), StandardOpenOption.CREATE_NEW);
            System.out.println(Database.json(result));
        } else if (args.length == 3 && args[0].equals("simulate")) {
            var result = PaySimScenarioReplay.run(Path.of(args[1]));
            Files.writeString(Path.of(args[2]), Database.json(result), StandardOpenOption.CREATE_NEW);
            System.out.println(Database.json(result));
        } else { throw new IllegalArgumentException("dataset download <new-directory> [rows=50000] | replay <csv> <new-report.json>"); }
    }

    public static void download(Path directory, int maxRows) throws Exception {
        if (maxRows < 1 || maxRows > 200_000) { throw new IllegalArgumentException("rows must be 1..200000"); }
        Files.createDirectory(directory.toAbsolutePath());
        Path temporary = directory.resolve("sample.csv.partial");
        int count = 0;
        try (InputStream response = connect(URI.create(URL), 0);
             ZipInputStream zip = new ZipInputStream(response, StandardCharsets.UTF_8)) {
            var entry = zip.getNextEntry();
            if (entry == null || entry.isDirectory() || !entry.getName().endsWith(".csv")) {
                throw new IOException("Expected official PaySim CSV in ZIP; authentication or upstream format may have changed");
            }
            try (var parser = parse(new InputStreamReader(zip, StandardCharsets.UTF_8));
                 var writer = new CSVPrinter(Files.newBufferedWriter(temporary, StandardCharsets.UTF_8),
                         CSVFormat.RFC4180.builder().setHeader(HEADER.toArray(String[]::new)).build())) {
                for (CSVRecord row : parser) {
                    if (row.size() != HEADER.size()) { throw new IOException("Invalid PaySim row"); }
                    writer.printRecord(row);
                    if (++count >= maxRows) { break; }
                }
            }
        } catch (Exception failure) {
            Files.deleteIfExists(temporary);
            throw failure;
        }
        if (count == 0) { throw new IOException("Empty dataset"); }
        Path output = directory.resolve("paysim-prefix.csv");
        Files.move(temporary, output);
        Map<String,Object> provenance = new LinkedHashMap<>();
        provenance.put("source", "https://www.kaggle.com/datasets/ealaxi/paysim1");
        provenance.put("downloadUrl", URL);
        provenance.put("author", "Edgar A. Lopez-Rojas et al.; PaySim");
        provenance.put("license", "CC BY-SA 4.0 (Kaggle dataset metadata)");
        provenance.put("downloadedAt", Instant.now().toString());
        provenance.put("rows", count);
        provenance.put("sampling", "first N source-order rows, not representative or randomly sampled");
        provenance.put("sha256", digest(output));
        provenance.put("synthetic", true);
        Files.writeString(directory.resolve("provenance.json"), Database.json(provenance), StandardOpenOption.CREATE_NEW);
        System.out.println(Database.json(provenance));
    }

    private static InputStream connect(URI uri, int redirects) throws IOException {
        if (redirects > 5 || !uri.getScheme().equals("https")) { throw new IOException("Invalid dataset redirect"); }
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setConnectTimeout(20_000); connection.setReadTimeout(30_000);
        connection.setInstanceFollowRedirects(false); connection.setRequestProperty("User-Agent", "realtime-dw-risk-demo/1");
        int status = connection.getResponseCode();
        if (status >= 300 && status < 400) {
            URI next = uri.resolve(connection.getHeaderField("Location")); connection.disconnect();
            return connect(next, redirects + 1);
        }
        if (status != 200) { connection.disconnect(); throw new IOException("Dataset download HTTP " + status); }
        return new FilterInputStream(connection.getInputStream()) {
            private long readBytes;
            private void count(int count) throws IOException {
                readBytes += Math.max(0, count);
                if (readBytes > 32L * 1024 * 1024) { throw new IOException("Compressed download budget exceeded"); }
            }
            @Override public int read() throws IOException { int value = super.read(); count(value < 0 ? 0 : 1); return value; }
            @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                int count = in.read(bytes, offset, length); count(count); return count;
            }
            @Override public void close() throws IOException { try { super.close(); } finally { connection.disconnect(); } }
        };
    }

    public static Map<String,Object> replay(Path csv) throws Exception {
        long tp = 0, fp = 0, tn = 0, fn = 0;
        long started = System.nanoTime();
        try (var parser = parse(Files.newBufferedReader(csv, StandardCharsets.UTF_8))) {
            for (CSVRecord row : parser) {
                if (row.size() != HEADER.size()) { throw new IOException("Invalid PaySim record " + row.getRecordNumber()); }
                // No post-transaction balances, isFraud or isFlaggedFraud are visible to prediction.
                boolean prediction = historicalBlock(row.get("type"), cents(row.get("amount")), cents(row.get("oldbalanceOrg")));
                String label = row.get("isFraud");
                if (!label.equals("0") && !label.equals("1")) { throw new IOException("Invalid label"); }
                boolean actual = label.equals("1");
                if (prediction && actual) { tp++; } else if (prediction) { fp++; }
                else if (actual) { fn++; } else { tn++; }
            }
        }
        Map<String,Object> report = new LinkedHashMap<>();
        report.put("datasetSha256", digest(csv)); report.put("ruleVersion", BASELINE);
        report.put("rows", tp + fp + tn + fn); report.put("truePositive", tp); report.put("falsePositive", fp);
        report.put("trueNegative", tn); report.put("falseNegative", fn);
        report.put("precision", tp + fp == 0 ? null : (double) tp / (tp + fp));
        report.put("recall", tp + fn == 0 ? null : (double) tp / (tp + fn));
        report.put("falsePositiveRate", fp + tn == 0 ? null : (double) fp / (fp + tn));
        report.put("elapsedMillis", (System.nanoTime() - started) / 1_000_000);
        report.put("inputs", List.of("type", "amount", "oldbalanceOrg"));
        report.put("limitations", "Synthetic prefix, baseline only; no device/IP/coupon labels; no production accuracy claim");
        return report;
    }

    public static boolean historicalBlock(String type, long amount, long balance) {
        if (amount < 0 || balance < 0) { throw new IllegalArgumentException("Negative balance/amount unsupported"); }
        boolean transfer = type.equals("TRANSFER") || type.equals("CASH_OUT");
        boolean drains = balance > 0 && BigDecimal.valueOf(amount).compareTo(BigDecimal.valueOf(balance).multiply(new BigDecimal("0.95"))) >= 0;
        return transfer && (amount >= 20_000_000 || amount >= 100_000 && drains);
    }

    static long cents(String value) { return new BigDecimal(value).movePointRight(2).longValueExact(); }
    static CSVParser parse(Reader reader) throws IOException {
        CSVParser parser = CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader);
        if (!parser.getHeaderNames().equals(HEADER)) { parser.close(); throw new IOException("Expected PaySim schema"); }
        return parser;
    }
    static String digest(Path path) throws Exception {
        MessageDigest hash = MessageDigest.getInstance("SHA-256");
        try (var stream = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536]; int count;
            while ((count = stream.read(buffer)) >= 0) { hash.update(buffer, 0, count); }
        }
        return HexFormat.of().formatHex(hash.digest());
    }
}
