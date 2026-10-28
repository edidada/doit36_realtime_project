package com.test.bigdata.fin.olap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;

public final class OlapApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlapApplication.class);

    private OlapApplication() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 5 || args.length > 8) {
            throw new IllegalArgumentException("Usage: input.csv|input.parquet output.csv snapshotId "
                    + "startDate endDate [maxGroups=100000] [maxScannedRows=10000000] [timeoutSeconds=60]");
        }
        ReportQuery query = new ReportQuery(args[2], LocalDate.parse(args[3]), LocalDate.parse(args[4]),
                args.length >= 7 ? Long.parseLong(args[6]) : 10_000_000,
                args.length >= 6 ? Integer.parseInt(args[5]) : 100_000,
                Duration.ofSeconds(args.length >= 8 ? Long.parseLong(args[7]) : 60));
        ReportResult result = new ReportEngine().execute(Path.of(args[0]), query);
        CsvReportWriter.write(Path.of(args[1]), result);
        LOGGER.info("snapshot={} sha256={} scanned={} matched={} groups={} output={}",
                result.snapshotId(), result.sourceSha256(), result.scannedRows(), result.matchedRows(),
                result.rows().size(), args[1]);
    }
}
