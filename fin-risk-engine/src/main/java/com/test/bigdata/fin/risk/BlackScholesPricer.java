package com.test.bigdata.fin.risk;

import org.apache.commons.math3.distribution.NormalDistribution;

/** Analytic European option price and spot Greeks. Vega is per 1.0 volatility, not per 1%. */
public final class BlackScholesPricer {
    public static final String MODEL_VERSION = "black-scholes-1";
    private static final NormalDistribution NORMAL = new NormalDistribution(0, 1);

    public PricingResult price(EuropeanOption option, MarketData market) {
        double sqrtTime = StrictMath.sqrt(option.maturityYears());
        double sigmaTime = Numbers.result(market.volatility() * sqrtTime);
        double d1 = Numbers.result((StrictMath.log(market.spot()) - StrictMath.log(option.strike())
                + (market.riskFreeRate() - market.dividendYield()
                + 0.5 * market.volatility() * market.volatility()) * option.maturityYears()) / sigmaTime);
        double d2 = Numbers.result(d1 - sigmaTime);
        double discountedSpot = Numbers.result(market.spot() * StrictMath.exp(-market.dividendYield() * option.maturityYears()));
        double discountedStrike = Numbers.result(option.strike() * StrictMath.exp(-market.riskFreeRate() * option.maturityYears()));
        boolean call = option.optionType() == OptionType.CALL;
        double value = call ? discountedSpot * cdf(d1) - discountedStrike * cdf(d2)
                : discountedStrike * cdf(-d2) - discountedSpot * cdf(-d1);
        double discount = StrictMath.exp(-market.dividendYield() * option.maturityYears());
        double delta = discount * (call ? cdf(d1) : -cdf(-d1));
        double density = NORMAL.density(d1);
        double gamma = discount * density / (market.spot() * sigmaTime);
        double vega = discountedSpot * density * sqrtTime;
        return new PricingResult(Numbers.result(Math.max(0, value)), Numbers.result(delta),
                Numbers.result(gamma), Numbers.result(vega), MODEL_VERSION, market.snapshotId());
    }

    private static double cdf(double value) { return NORMAL.cumulativeProbability(value); }
}
