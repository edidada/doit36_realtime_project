package cn.doitedu.fin.risk;

import java.time.LocalDate;
import java.util.Objects;

/** Annualised continuously compounded rates and decimal volatility, e.g. 0.20 = 20%. */
public record MarketData(String snapshotId, LocalDate valuationDate, double spot,
                         double riskFreeRate, double volatility, double dividendYield) {
    public MarketData {
        Numbers.text(snapshotId, "snapshotId");
        Objects.requireNonNull(valuationDate, "valuationDate");
        Numbers.positive(spot, "spot");
        Numbers.finite(riskFreeRate, "riskFreeRate");
        Numbers.positive(volatility, "volatility");
        Numbers.finite(dividendYield, "dividendYield");
    }
}
