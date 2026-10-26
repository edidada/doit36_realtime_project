package cn.doitedu.fin.olap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Publish only a completed report, refusing an existing output path. */
public final class CsvReportWriter {
    private CsvReportWriter() { }

    public static void write(Path output, ReportResult result) throws IOException {
        Path target = output.toAbsolutePath();
        Path temporary = Files.createTempFile(target.getParent(), ".fin-olap-", ".csv.tmp");
        try {
            try (CSVPrinter printer = new CSVPrinter(Files.newBufferedWriter(temporary, StandardCharsets.UTF_8),
                    CSVFormat.RFC4180.builder().setHeader("trade_date", "account_id", "institution_id",
                            "transaction_type", "currency", "trade_count", "total_amount_cents").build())) {
                for (ReportRow row : result.rows()) {
                    printer.printRecord(row.tradeDate(), row.accountId(), row.institutionId(),
                            row.transactionType(), row.currency(), row.tradeCount(), row.totalAmountCents());
                }
            }
            Files.move(temporary, target);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }
}
