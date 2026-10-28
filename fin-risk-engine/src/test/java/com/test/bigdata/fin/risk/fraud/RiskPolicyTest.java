package com.test.bigdata.fin.risk.fraud;

import org.junit.jupiter.api.Test;
import static com.test.bigdata.fin.risk.fraud.Contracts.*;
import static org.junit.jupiter.api.Assertions.*;

class RiskPolicyTest {
    @Test void thresholdBoundariesAndHardBlocksAreExplicit() {
        var policy = new RiskPolicy();
        assertEquals("ALLOW", policy.evaluate(base(true, 4, 2, 2, 10_000)).action());
        assertEquals("BLOCK", policy.evaluate(base(true, 5, 0, 0, 10_000)).action());
        assertEquals("BLOCK", policy.evaluate(base(true, 0, 3, 0, 10_000)).action());
        assertEquals("BLOCK", policy.evaluate(base(true, 0, 0, 3, 10_000)).action());
        assertEquals("BLOCK", policy.evaluate(base(true, 0, 0, 0, 1_000_001)).action());
        assertEquals("REVIEW", policy.evaluate(base(false, 0, 0, 0, 10_000)).action());
    }
    private FeatureSnapshot base(boolean trusted, int attempts, int devices, int declines, long amount) {
        return new FeatureSnapshot(false, true, 100, 100, trusted, false, false, true,
                false, false, amount, 10_000, attempts, devices, declines, 10_000_000);
    }
}
