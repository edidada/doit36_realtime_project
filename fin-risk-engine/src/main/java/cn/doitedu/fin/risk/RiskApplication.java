package cn.doitedu.fin.risk;

import java.time.LocalDate;
import java.util.Locale;

/** Deterministic command-line valuation for one explicitly versioned market snapshot. */
public final class RiskApplication {
    private RiskApplication() { }

    public static void main(String[] args) throws InterruptedException {
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
        System.out.println(new BlackScholesPricer().price(option, market));
        System.out.println(new MonteCarloEngine().simulate(request));
    }
}
