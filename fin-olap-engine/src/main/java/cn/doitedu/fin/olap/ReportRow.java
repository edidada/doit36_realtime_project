package cn.doitedu.fin.olap;

import java.time.LocalDate;

/** Currency is always part of the grouping key: CNY and USD are never added together. */
public record ReportRow(LocalDate tradeDate, String accountId, String institutionId,
                        String transactionType, String currency, long tradeCount, long totalAmountCents) {
}
