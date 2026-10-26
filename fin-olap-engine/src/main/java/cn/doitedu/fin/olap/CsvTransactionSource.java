package cn.doitedu.fin.olap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;

/** Strict UTF-8 RFC 4180 input, including quoted commas/newlines. */
public final class CsvTransactionSource implements TransactionSource {
    public static final List<String> HEADER = List.of("trade_id", "trade_date", "account_id",
            "institution_id", "transaction_type", "currency", "amount_cents");
    private final CSVParser parser;
    private final Iterator<CSVRecord> records;

    public CsvTransactionSource(Path path) throws IOException {
        Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
        CSVParser opened;
        try {
            opened = CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true)
                    .setIgnoreEmptyLines(false).build().parse(reader);
        } catch (IOException | RuntimeException failure) {
            reader.close();
            throw failure;
        }
        parser = opened;
        if (!parser.getHeaderNames().equals(HEADER)) {
            parser.close();
            throw new IOException("CSV header must be exactly " + HEADER);
        }
        records = parser.iterator();
    }

    @Override
    public Transaction next() throws IOException {
        try {
            if (!records.hasNext()) {
                return null;
            }
            CSVRecord row = records.next();
            try {
                if (row.size() != HEADER.size()) {
                    throw new IllegalArgumentException("expected seven columns");
                }
                return new Transaction(row.get(0), LocalDate.parse(row.get(1)), row.get(2), row.get(3),
                        row.get(4), row.get(5), Long.parseLong(row.get(6)));
            } catch (RuntimeException failure) {
                throw new IOException("Invalid transaction at CSV record " + row.getRecordNumber(), failure);
            }
        } catch (UncheckedIOException failure) {
            throw failure.getCause();
        }
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }
}
