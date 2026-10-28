package com.test.bigdata.fin.risk;

import java.util.Objects;

/** Year fraction is supplied explicitly; calendar and day-count conversion is outside this model. */
public record EuropeanOption(String instrumentId, OptionType optionType, double strike, double maturityYears) {
    public EuropeanOption {
        Numbers.text(instrumentId, "instrumentId");
        Objects.requireNonNull(optionType, "optionType");
        Numbers.positive(strike, "strike");
        Numbers.positive(maturityYears, "maturityYears");
    }
}
