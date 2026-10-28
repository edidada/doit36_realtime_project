package com.test.bigdata.fin.risk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HistoricalRiskTest {
    @Test void nearestRankQuantileAndInclusiveTailHaveExplicitSemantics() {
        double[] losses = {100, -30, 40, 20, 80};
        var result = HistoricalRisk.calculate(losses, 0.8);
        assertEquals(80, result.valueAtRisk());
        assertEquals(90, result.expectedShortfall());
        assertEquals(2, result.tailCount());
        assertArrayEquals(new double[]{100, -30, 40, 20, 80}, losses);
    }

    @Test void keepsGainsNegativeAndIncludesAllBoundaryTies() {
        var gains = HistoricalRisk.calculate(new double[]{-20, -10}, 0.95);
        assertEquals(-10, gains.valueAtRisk());
        var tied = HistoricalRisk.calculate(new double[]{10, 20, 20, 30}, 0.5);
        assertEquals(20, tied.valueAtRisk());
        assertEquals(70.0 / 3, tied.expectedShortfall(), 1e-12);
        assertEquals(3, tied.tailCount());
    }

    @Test void rejectsEmptyNonFiniteAndInvalidConfidence() {
        assertThrows(IllegalArgumentException.class, () -> HistoricalRisk.calculate(new double[0], 0.95));
        assertThrows(IllegalArgumentException.class, () -> HistoricalRisk.calculate(new double[]{Double.NaN}, 0.95));
        assertThrows(IllegalArgumentException.class, () -> HistoricalRisk.calculate(new double[]{1}, 1));
    }
}
