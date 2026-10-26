package cn.doitedu.fin.risk;

public record SimulationResult(String jobId, double presentValue, double standardError,
                               double confidenceLow, double confidenceHigh, long simulationCount,
                               long randomSeed, String modelVersion, String marketDataSnapshot) { }
