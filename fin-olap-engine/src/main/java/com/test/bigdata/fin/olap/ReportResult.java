package com.test.bigdata.fin.olap;

import java.util.List;

/** Source digest and caller-supplied snapshot ID make a report traceable to its input. */
public record ReportResult(String snapshotId, String sourceSha256, long scannedRows,
                           long matchedRows, List<ReportRow> rows) {
    public ReportResult {
        rows = List.copyOf(rows);
    }
}
