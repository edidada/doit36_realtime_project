package cn.doitedu.fin.risk;

public record PricingResult(double presentValue, double delta, double gamma, double vega,
                            String modelVersion, String marketDataSnapshot) { }
