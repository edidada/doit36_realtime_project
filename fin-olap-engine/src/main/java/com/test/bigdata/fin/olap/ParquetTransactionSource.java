package com.test.bigdata.fin.olap;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;

/** Same logical schema as the CSV source; date is ISO string, amount_cents is Avro long. */
public final class ParquetTransactionSource implements TransactionSource {
    private final ParquetReader<GenericRecord> reader;

    public ParquetTransactionSource(Path path) throws IOException {
        reader = AvroParquetReader.<GenericRecord>builder(new LocalParquetFile(path))
                .withConf(new PlainParquetConfiguration()).build();
    }

    @Override
    public Transaction next() throws IOException {
        try {
            GenericRecord row = reader.read();
            if (row == null) { return null; }
            Object amount = row.get("amount_cents");
            if (!(amount instanceof Long)) {
                throw new IllegalArgumentException("amount_cents must be a non-null Avro long");
            }
            return new Transaction(text(row, "trade_id"), LocalDate.parse(text(row, "trade_date")),
                    text(row, "account_id"), text(row, "institution_id"), text(row, "transaction_type"),
                    text(row, "currency"), (Long) amount);
        } catch (RuntimeException failure) {
            throw new IOException("Invalid Parquet file or transaction", failure);
        }
    }

    private static String text(GenericRecord row, String field) {
        Object value = row.get(field);
        if (!(value instanceof CharSequence)) {
            throw new IllegalArgumentException(field + " must be a non-null string");
        }
        return value.toString();
    }

    @Override
    public void close() throws IOException { reader.close(); }
}
