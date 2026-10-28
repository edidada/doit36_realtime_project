package com.test.bigdata.fin.olap;

import java.time.LocalDate;
import java.time.Duration;
import java.util.Objects;

/** Inclusive date bounds and explicit execution budgets. */
public record ReportQuery(String snapshotId, LocalDate startDate, LocalDate endDate,
                          long maxScannedRows, int maxGroups, Duration timeout) {
    public ReportQuery {
        if (snapshotId == null || snapshotId.isBlank()) {
            throw new IllegalArgumentException("snapshotId must not be blank");
        }
        Objects.requireNonNull(startDate, "startDate");
        Objects.requireNonNull(endDate, "endDate");
        Objects.requireNonNull(timeout, "timeout");
        if (endDate.isBefore(startDate) || maxScannedRows < 1 || maxGroups < 1
                || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofDays(1)) > 0) {
            throw new IllegalArgumentException("invalid date range or execution budget (timeout <= 1 day)");
        }
    }
}
