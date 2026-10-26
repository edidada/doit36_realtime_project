package cn.doitedu.fin.risk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class BlackScholesPricerTest {
    private final BlackScholesPricer pricer = new BlackScholesPricer();
    private static final LocalDate DATE = LocalDate.of(2026, 10, 26);

    static MarketData market(double spot, double rate, double volatility, double dividend) {
        return new MarketData("market-test", DATE, spot, rate, volatility, dividend);
    }

    static EuropeanOption option(OptionType type) {
        return new EuropeanOption("european-test", type, 100, 1);
    }

    @Test void matchesPublishedTextbookValuesAndGreeks() {
        var call = pricer.price(option(OptionType.CALL), market(100, 0.05, 0.2, 0));
        var put = pricer.price(option(OptionType.PUT), market(100, 0.05, 0.2, 0));
        assertEquals(10.450583572185565, call.presentValue(), 1e-10);
        assertEquals(5.573526022256971, put.presentValue(), 1e-10);
        assertEquals(0.6368306511756191, call.delta(), 1e-10);
        assertEquals(0.0187620173458469, call.gamma(), 1e-12);
        assertEquals(37.52403469169379, call.vega(), 1e-9);
        assertEquals("market-test", call.marketDataSnapshot());
    }

    @Test void satisfiesDividendAdjustedPutCallParityWithNegativeRates() {
        var market = market(115, -0.01, 0.35, 0.025);
        var call = pricer.price(option(OptionType.CALL), market);
        var put = pricer.price(option(OptionType.PUT), market);
        assertEquals(115 * Math.exp(-0.025) - 100 * Math.exp(0.01),
                call.presentValue() - put.presentValue(), 1e-10);
        assertEquals(Math.exp(-0.025), call.delta() - put.delta(), 1e-10);
        assertEquals(call.gamma(), put.gamma(), 1e-12);
        assertEquals(call.vega(), put.vega(), 1e-12);
    }

    @Test void greeksMatchIndependentFiniteDifferences() {
        var option = option(OptionType.PUT);
        var base = pricer.price(option, market(95, 0.03, 0.28, 0.01));
        double step = 0.01;
        double up = pricer.price(option, market(95 + step, 0.03, 0.28, 0.01)).presentValue();
        double down = pricer.price(option, market(95 - step, 0.03, 0.28, 0.01)).presentValue();
        assertEquals((up - down) / (2 * step), base.delta(), 1e-7);
        assertEquals((up - 2 * base.presentValue() + down) / (step * step), base.gamma(), 1e-7);
        double volStep = 1e-5;
        double volUp = pricer.price(option, market(95, 0.03, 0.28 + volStep, 0.01)).presentValue();
        double volDown = pricer.price(option, market(95, 0.03, 0.28 - volStep, 0.01)).presentValue();
        assertEquals((volUp - volDown) / (2 * volStep), base.vega(), 1e-6);
    }

    @Test void tailProbabilitiesStayFiniteAndNonNegative() {
        for (double spot : new double[]{1e-6, 1e6}) {
            for (OptionType type : OptionType.values()) {
                var price = pricer.price(option(type), market(spot, 0.05, 0.2, 0));
                assertTrue(price.presentValue() >= 0 && Double.isFinite(price.presentValue()));
                assertTrue(Double.isFinite(price.delta()));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(doubles = {0, -1, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY})
    void rejectsInvalidPositiveInputs(double value) {
        assertThrows(IllegalArgumentException.class, () -> market(value, 0, 0.2, 0));
        assertThrows(IllegalArgumentException.class, () -> market(100, 0, value, 0));
        assertThrows(IllegalArgumentException.class, () -> new EuropeanOption("i", OptionType.CALL, value, 1));
        assertThrows(IllegalArgumentException.class, () -> new EuropeanOption("i", OptionType.CALL, 100, value));
    }

    @Test void detectsNumericalOverflow() {
        assertThrows(ArithmeticException.class,
                () -> pricer.price(option(OptionType.CALL), market(100, -1000, 0.2, 0)));
    }
}
