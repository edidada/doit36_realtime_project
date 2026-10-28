package com.test.bigdata.fin.risk;

public record PricingResult(double presentValue, double delta, double gamma, double vega,
                            String modelVersion, String marketDataSnapshot) { }
