package com.test.bigdata.fin.risk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Locale;

/** Deterministic command-line valuation for one explicitly versioned market snapshot. */
public final class RiskApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(RiskApplication.class);
    private RiskApplication() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            LOGGER.info("No arguments: running the bundled QuantLib 1.40 public benchmark (not live market data)");
            for (var sample : PublicOptionDataset.load()) {
                var analytic = new BlackScholesPricer().price(sample.option(), sample.market());
                double error = Math.abs(analytic.presentValue() - sample.expectedValue());
                if (error > sample.tolerance()) {
                    throw new IllegalStateException("Public benchmark mismatch: " + sample.id());
                }
                var simulation = new MonteCarloEngine().simulate(new SimulationRequest(sample.id(),
                        sample.option(), sample.market(), 100_000, 42, 1));
                LOGGER.info("benchmark={} expected={} analytic={} absoluteError={} tolerance={} simulation={}",
                        sample.id(), sample.expectedValue(), analytic.presentValue(), error, sample.tolerance(), simulation);
            }
            LOGGER.info("Public benchmark complete: 8/8 analytic prices passed; pathsPerCase=100000 seed=42 workers=1");
            return;
        }
        if (args.length < 10 || args.length > 13) {
            throw new IllegalArgumentException("Usage: snapshotId valuationDate CALL|PUT spot strike "
                    + "rate dividendYield volatility maturityYears paths [seed=42] [workers=1] [instrumentId=option]");
        }
        var market = new MarketData(args[0], LocalDate.parse(args[1]), Double.parseDouble(args[3]),
                Double.parseDouble(args[5]), Double.parseDouble(args[7]), Double.parseDouble(args[6]));
        var option = new EuropeanOption(args.length == 13 ? args[12] : "option",
                OptionType.valueOf(args[2].toUpperCase(Locale.ROOT)), Double.parseDouble(args[4]),
                Double.parseDouble(args[8]));
        var request = new SimulationRequest("cli-valuation", option, market, Long.parseLong(args[9]),
                args.length >= 11 ? Long.parseLong(args[10]) : 42,
                args.length >= 12 ? Integer.parseInt(args[11]) : 1);
        LOGGER.info("analytic={}", new BlackScholesPricer().price(option, market));
        LOGGER.info("simulation={}", new MonteCarloEngine().simulate(request));
    }
}
