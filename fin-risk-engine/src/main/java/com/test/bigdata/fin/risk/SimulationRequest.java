package com.test.bigdata.fin.risk;

public record SimulationRequest(String jobId, EuropeanOption option, MarketData market,
                                long simulationCount, long randomSeed, int workerCount) {
    public SimulationRequest {
        Numbers.text(jobId, "jobId");
        java.util.Objects.requireNonNull(option, "option");
        java.util.Objects.requireNonNull(market, "market");
        if (simulationCount < 2 || simulationCount > 10_000_000 || workerCount < 1 || workerCount > 32) {
            throw new IllegalArgumentException("simulationCount must be 2..10000000 and workerCount 1..32");
        }
    }
}
