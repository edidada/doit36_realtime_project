package com.test.bigdata.fin.risk;

import java.util.Arrays;

/** Positive values are losses. VaR uses nearest-rank; ES averages observations >= VaR (all ties). */
public final class HistoricalRisk {
    private HistoricalRisk() { }

    public static Result calculate(double[] losses, double confidence) {
        if (losses == null || losses.length == 0 || !Double.isFinite(confidence)
                || confidence <= 0 || confidence >= 1) {
            throw new IllegalArgumentException("non-empty losses and confidence in (0,1) required");
        }
        double[] sorted = losses.clone();
        for (double loss : sorted) { Numbers.finite(loss, "loss"); }
        Arrays.sort(sorted);
        int rank = (int) Math.ceil(confidence * sorted.length) - 1;
        double var = sorted[rank];
        long tailCount = 0;
        double mean = 0;
        for (double loss : sorted) {
            if (loss >= var) {
                tailCount++;
                mean = Numbers.result(mean + (loss - mean) / tailCount);
            }
        }
        return new Result(var, mean, confidence, sorted.length, tailCount);
    }

    public record Result(double valueAtRisk, double expectedShortfall, double confidence,
                         int scenarioCount, long tailCount) { }
}
