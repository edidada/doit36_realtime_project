package com.test.bigdata.fin.risk;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class PublicOptionDatasetTest {
    @TestFactory Stream<DynamicTest> matchesPinnedUpstreamPrices() throws Exception {
        return PublicOptionDataset.load().stream().map(sample -> DynamicTest.dynamicTest(sample.id(), () -> {
            PricingResult result = new BlackScholesPricer().price(sample.option(), sample.market());
            assertEquals(sample.expectedValue(), result.presentValue(), sample.tolerance());
        }));
    }

    @Test void ideaCanRunWithoutProgramArguments() {
        assertDoesNotThrow(() -> RiskApplication.main(new String[0]));
    }

    @Test void partialExplicitArgumentsStillFailRatherThanSilentlyRunningDemo() {
        assertThrows(IllegalArgumentException.class, () -> RiskApplication.main(new String[]{"incomplete"}));
    }
}
