package cn.doitedu.fin.risk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MonteCarloEngineTest {
    @Test void convergesToAnalyticCallAndPutWithDividend() throws Exception {
        for (OptionType type : OptionType.values()) {
            var option = BlackScholesPricerTest.option(type);
            var market = BlackScholesPricerTest.market(100, 0.04, 0.25, 0.015);
            var result = new MonteCarloEngine().simulate(new SimulationRequest("convergence", option,
                    market, 250_000, 42, 3));
            double expected = new BlackScholesPricer().price(option, market).presentValue();
            assertEquals(expected, result.presentValue(), 4 * result.standardError());
            assertTrue(result.standardError() > 0);
            assertEquals(250_000, result.simulationCount());
            assertTrue(result.confidenceLow() < result.presentValue());
            assertTrue(result.confidenceHigh() > result.presentValue());
        }
    }

    @Test void exactlyReproducesResultAcrossRunsAndWorkerCounts() throws Exception {
        var option = BlackScholesPricerTest.option(OptionType.CALL);
        var market = BlackScholesPricerTest.market(100, 0.05, 0.2, 0);
        var engine = new MonteCarloEngine();
        var serial = engine.simulate(new SimulationRequest("repro", option, market, 50_003, 789, 1));
        var parallel = engine.simulate(new SimulationRequest("repro", option, market, 50_003, 789, 4));
        assertEquals(serial, parallel);
        assertEquals(serial, engine.simulate(new SimulationRequest("repro", option, market, 50_003, 789, 2)));
        assertNotEquals(serial.presentValue(), engine.simulate(new SimulationRequest("repro", option,
                market, 50_003, 790, 2)).presentValue());
    }

    @Test void interruptionDoesNotReturnAPartialSuccess() {
        var request = new SimulationRequest("cancel", BlackScholesPricerTest.option(OptionType.CALL),
                BlackScholesPricerTest.market(100, 0.05, 0.2, 0), 200_000, 42, 2);
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedException.class, () -> new MonteCarloEngine().simulate(request));
        } finally {
            Thread.interrupted();
        }
    }

    @Test void rejectsInvalidResourceLimits() {
        var option = BlackScholesPricerTest.option(OptionType.CALL);
        var market = BlackScholesPricerTest.market(100, 0.05, 0.2, 0);
        assertThrows(IllegalArgumentException.class, () -> new SimulationRequest("j", option, market, 1, 42, 1));
        assertThrows(IllegalArgumentException.class, () -> new SimulationRequest("j", option, market, 10_000_001, 42, 1));
        assertThrows(IllegalArgumentException.class, () -> new SimulationRequest("j", option, market, 20, 42, 0));
    }
}
