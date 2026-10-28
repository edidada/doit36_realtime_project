package com.test.bigdata.fin.risk.fraud;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** Bounded CPU-only supervised decision tree benchmark. Never used as a production approval oracle. */
public final class PaySimBenchmark {
    private static final int MAX_DEPTH = 4;
    private static final int MIN_LEAF = 20;
    private static final List<String> FEATURES = List.of("log_amount", "log_oldbalance", "relative_balance_gap",
            "is_transfer", "is_cash_out", "zero_oldbalance");
    private PaySimBenchmark() { }
    private record Sample(double[] features, boolean fraud) { }
    public record Node(boolean blocked, int support, int fraudCount, int feature, double threshold, Node left, Node right) { }

    public static Map<String,Object> run(Path csv) throws Exception {
        List<Sample> rows = new ArrayList<>();
        long previousStep = -1;
        try (var parser = PaySimTool.parse(Files.newBufferedReader(csv, StandardCharsets.UTF_8))) {
            for (var row : parser) {
                if (rows.size() >= 200_000) { throw new IllegalArgumentException("Benchmark supports at most 200000 rows in 256MiB"); }
                long step = Long.parseLong(row.get("step"));
                if (step < previousStep) { throw new IllegalArgumentException("Input must be source time ordered"); }
                previousStep = step;
                String label = row.get("isFraud");
                if (!label.equals("0") && !label.equals("1")) { throw new IllegalArgumentException("Invalid label"); }
                rows.add(new Sample(features(row.get("type"), PaySimTool.cents(row.get("amount")),
                        PaySimTool.cents(row.get("oldbalanceOrg"))), label.equals("1")));
            }
        }
        if (rows.size() < 100) { throw new IllegalArgumentException("At least 100 ordered records required"); }
        int trainingSize = rows.size() * 70 / 100;
        var training = rows.subList(0, trainingSize);
        var testing = rows.subList(trainingSize, rows.size());
        int frauds = (int) training.stream().filter(Sample::fraud).count();
        if (frauds == 0 || frauds == trainingSize) { throw new IllegalArgumentException("Training prefix needs both labels"); }
        // Class weighting uses training labels only. All algorithm settings are fixed before test evaluation.
        double fraudWeight = Math.min(100, (double) (trainingSize - frauds) / frauds);
        Node root = train(training, 0, fraudWeight);
        long tp = 0, fp = 0, tn = 0, fn = 0;
        for (var row : testing) {
            boolean blocked = predicts(root, row.features());
            if (blocked && row.fraud()) { tp++; } else if (blocked) { fp++; }
            else if (row.fraud()) { fn++; } else { tn++; }
        }
        Map<String,Object> report = new LinkedHashMap<>();
        report.put("modelVersion", "cpu-cart-fixed-grid-1"); report.put("datasetSha256", PaySimTool.digest(csv));
        report.put("split", "source time order: first 70% training, last 30% test; no shuffling or test tuning");
        report.put("trainingRows", trainingSize); report.put("trainingFrauds", frauds); report.put("testRows", testing.size());
        report.put("fraudTrainingWeight", fraudWeight); report.put("maxDepth", MAX_DEPTH); report.put("minLeaf", MIN_LEAF);
        report.put("features", FEATURES); report.put("truePositive", tp); report.put("falsePositive", fp);
        report.put("trueNegative", tn); report.put("falseNegative", fn);
        report.put("precision", tp + fp == 0 ? null : (double) tp / (tp + fp));
        report.put("recall", tp + fn == 0 ? null : (double) tp / (tp + fn));
        report.put("falsePositiveRate", fp + tn == 0 ? null : (double) fp / (fp + tn));
        report.put("tree", root);
        report.put("limitations", "Synthetic prefix only, no validation search, calibration or drift validation. "
                + "Not loaded by live API; missing device/coupon evidence cannot be fabricated.");
        return report;
    }

    static double[] features(String type, long amount, long balance) {
        if (amount < 0 || balance < 0) { throw new IllegalArgumentException("Negative values unsupported"); }
        return new double[]{Math.log1p(amount), Math.log1p(balance),
                Math.min(1, Math.abs((double) amount - balance) / Math.max(1.0, amount)),
                type.equals("TRANSFER") ? 1 : 0, type.equals("CASH_OUT") ? 1 : 0, balance == 0 ? 1 : 0};
    }

    private static Node train(List<Sample> samples, int depth, double weight) {
        int frauds = (int) samples.stream().filter(Sample::fraud).count();
        boolean blocked = frauds * weight >= samples.size() - frauds;
        if (depth == MAX_DEPTH || samples.size() < MIN_LEAF * 2 || frauds == 0 || frauds == samples.size()) {
            return new Node(blocked, samples.size(), frauds, -1, 0, null, null);
        }
        double best = impurity(samples.size(), frauds, weight);
        int chosen = -1; double cut = 0;
        for (int feature = 0; feature < FEATURES.size(); feature++) {
            int index = feature;
            double min = samples.stream().mapToDouble(row -> row.features()[index]).min().orElseThrow();
            double max = samples.stream().mapToDouble(row -> row.features()[index]).max().orElseThrow();
            if (min == max) { continue; }
            // Include an exact equality split for zero balance gap, then a bounded fixed grid.
            for (int candidate = 0; candidate < 33; candidate++) {
                double threshold = candidate == 0 ? min : min + (max - min) * candidate / 33;
                int leftCount = 0, leftFrauds = 0;
                for (var row : samples) {
                    if (row.features()[feature] <= threshold) { leftCount++; if (row.fraud()) { leftFrauds++; } }
                }
                if (leftCount < MIN_LEAF || samples.size() - leftCount < MIN_LEAF) { continue; }
                double score = impurity(leftCount, leftFrauds, weight)
                        + impurity(samples.size() - leftCount, frauds - leftFrauds, weight);
                if (score < best - 1e-10) { best = score; chosen = feature; cut = threshold; }
            }
        }
        if (chosen < 0) { return new Node(blocked, samples.size(), frauds, -1, 0, null, null); }
        List<Sample> left = new ArrayList<>(), right = new ArrayList<>();
        for (var row : samples) { (row.features()[chosen] <= cut ? left : right).add(row); }
        return new Node(blocked, samples.size(), frauds, chosen, cut,
                train(left, depth + 1, weight), train(right, depth + 1, weight));
    }

    private static double impurity(int count, int frauds, double weight) {
        double positives = frauds * weight, negatives = count - frauds;
        return positives + negatives == 0 ? 0 : 2 * positives * negatives / (positives + negatives);
    }

    static boolean predicts(Node node, double[] features) {
        while (node.feature() >= 0) { node = features[node.feature()] <= node.threshold() ? node.left() : node.right(); }
        return node.blocked();
    }
}
