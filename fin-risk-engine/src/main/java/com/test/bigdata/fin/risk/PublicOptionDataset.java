package com.test.bigdata.fin.risk;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/** Fixed upstream numerical regression vectors, not observed market quotes. */
public final class PublicOptionDataset {
    public static final String SNAPSHOT = "quantlib-1.40-test-values";
    private PublicOptionDataset() { }

    public record Sample(String id, EuropeanOption option, MarketData market,
                         double expectedValue, double tolerance) { }

    public static List<Sample> load() throws IOException {
        var input = PublicOptionDataset.class.getResourceAsStream("/datasets/quantlib-european-options.csv");
        if (input == null) { throw new IOException("Bundled public option dataset is missing"); }
        var samples = new ArrayList<Sample>();
        try (var reader = new InputStreamReader(input, StandardCharsets.UTF_8);
             var parser = new CSVParser(reader, CSVFormat.RFC4180.builder()
                     .setHeader().setSkipHeaderRecord(true).build())) {
            for (var row : parser) {
                String id = row.get("case_id");
                var option = new EuropeanOption(id, OptionType.valueOf(row.get("option_type")),
                        Double.parseDouble(row.get("strike")), Double.parseDouble(row.get("maturity_years")));
                // The fixed date is local metadata only; upstream vectors specify a year fraction, not a quote date.
                var market = new MarketData(SNAPSHOT, LocalDate.of(2026, 1, 1), Double.parseDouble(row.get("spot")),
                        Double.parseDouble(row.get("risk_free_rate")), Double.parseDouble(row.get("volatility")),
                        Double.parseDouble(row.get("dividend_yield")));
                samples.add(new Sample(id, option, market, Double.parseDouble(row.get("expected_value")),
                        Double.parseDouble(row.get("tolerance"))));
            }
        }
        if (samples.size() != 8) { throw new IOException("Expected exactly 8 pinned public benchmark cases"); }
        return List.copyOf(samples);
    }
}
