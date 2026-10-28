package com.test.bigdata.fin.risk;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/** GBM terminal pricing; fixed chunks and path-indexed RNG make worker count immaterial. */
public final class MonteCarloEngine {
    public static final String MODEL_VERSION = "gbm-box-muller-splitmix64-1";
    private static final int CHUNK_SIZE = 4096;

    public SimulationResult simulate(SimulationRequest request) throws InterruptedException {
        var executor = Executors.newFixedThreadPool(request.workerCount());
        try {
            List<Callable<Moments>> tasks = new ArrayList<>();
            for (long first = 0; first < request.simulationCount(); first += CHUNK_SIZE) {
                long start = first;
                long end = Math.min(first + CHUNK_SIZE, request.simulationCount());
                tasks.add(() -> sample(request, start, end));
            }
            Moments aggregate = new Moments();
            // invokeAll and ordered merging preserve the floating-point reduction order.
            for (var result : executor.invokeAll(tasks)) {
                try {
                    aggregate.merge(result.get());
                } catch (ExecutionException failure) {
                    if (failure.getCause() instanceof RuntimeException cause) { throw cause; }
                    throw new IllegalStateException("Simulation worker failed", failure.getCause());
                }
            }
            double standardError = Numbers.result(StrictMath.sqrt(aggregate.m2
                    / (aggregate.count - 1) / aggregate.count));
            double margin = 1.959963984540054 * standardError;
            return new SimulationResult(request.jobId(), Numbers.result(aggregate.mean), standardError,
                    Numbers.result(aggregate.mean - margin), Numbers.result(aggregate.mean + margin),
                    aggregate.count, request.randomSeed(), MODEL_VERSION, request.market().snapshotId());
        } finally {
            executor.shutdownNow();
        }
    }

    private static Moments sample(SimulationRequest request, long first, long end) {
        var option = request.option();
        var market = request.market();
        double time = option.maturityYears();
        double drift = Numbers.result((market.riskFreeRate() - market.dividendYield()
                - 0.5 * market.volatility() * market.volatility()) * time);
        double diffusion = Numbers.result(market.volatility() * StrictMath.sqrt(time));
        double discount = Numbers.result(StrictMath.exp(-market.riskFreeRate() * time));
        Moments moments = new Moments();
        for (long path = first; path < end; path++) {
            if (Thread.currentThread().isInterrupted()) { throw new CancellationException("Simulation interrupted"); }
            double terminal = Numbers.result(market.spot()
                    * StrictMath.exp(drift + diffusion * normal(request.randomSeed(), path)));
            double payoff = option.optionType() == OptionType.CALL
                    ? Math.max(terminal - option.strike(), 0) : Math.max(option.strike() - terminal, 0);
            moments.add(Numbers.result(discount * payoff));
        }
        return moments;
    }

    private static double normal(long seed, long path) {
        // Overflow in the hash is intentional 64-bit modular arithmetic, unlike financial totals.
        long counter = seed + (2 * path) * 0x9e3779b97f4a7c15L;
        double uniform = ((mix(counter) >>> 12) + 0.5) * 0x1.0p-52;
        double angle = (mix(counter + 0x9e3779b97f4a7c15L) >>> 11) * 0x1.0p-53;
        return StrictMath.sqrt(-2 * StrictMath.log(uniform)) * StrictMath.cos(2 * StrictMath.PI * angle);
    }

    private static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private static final class Moments {
        long count;
        double mean;
        double m2;

        void add(double value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            m2 = Numbers.result(m2 + delta * (value - mean));
        }

        void merge(Moments other) {
            if (count == 0) {
                count = other.count; mean = other.mean; m2 = other.m2;
                return;
            }
            double delta = other.mean - mean;
            long combined = count + other.count;
            m2 = Numbers.result(m2 + other.m2 + delta * delta * count * other.count / combined);
            mean = Numbers.result(mean + delta * other.count / combined);
            count = combined;
        }
    }
}
