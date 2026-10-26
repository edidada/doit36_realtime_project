package cn.doitedu.fin.olap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class ParquetTransactionSourceTest {
    @TempDir Path directory;
    private static final Schema SCHEMA = new Schema.Parser().parse("""
            {"type":"record","name":"Transaction","fields":[
              {"name":"trade_id","type":"string"},
              {"name":"trade_date","type":"string"},
              {"name":"account_id","type":"string"},
              {"name":"institution_id","type":"string"},
              {"name":"transaction_type","type":"string"},
              {"name":"currency","type":"string"},
              {"name":"amount_cents","type":"long"}]}
            """);

    @Test void parquetAndCsvReconcileWithoutHadoopFilesystemOrWinutils() throws Exception {
        Path parquet = directory.resolve("input.parquet");
        try (var writer = AvroParquetWriter.<GenericRecord>builder(output(parquet))
                .withConf(new PlainParquetConfiguration()).withSchema(SCHEMA).build()) {
            writer.write(trade("t1", 150));
            writer.write(trade("t2", -20));
        }
        Path csv = Files.writeString(directory.resolve("input.csv"), String.join(",", CsvTransactionSource.HEADER)
                + "\nt1,2026-01-01,acc,bank,PAY,CNY,150\nt2,2026-01-01,acc,bank,PAY,CNY,-20\n");
        var query = new ReportQuery("s", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 31),
                10, 10, Duration.ofSeconds(30));
        var engine = new ReportEngine();
        var actual = engine.execute(parquet, query);
        assertEquals(engine.execute(csv, query).rows(), actual.rows());
        assertEquals(2, actual.scannedRows());
        assertEquals(130, actual.rows().get(0).totalAmountCents());
        assertEquals(actual, engine.execute(parquet, query));
        Files.delete(parquet);
    }

    @Test void emptyParquetProducesNoRows() throws Exception {
        Path path = directory.resolve("empty.parquet");
        try (var writer = AvroParquetWriter.<GenericRecord>builder(output(path))
                .withConf(new PlainParquetConfiguration()).withSchema(SCHEMA).build()) {
            // Deliberately leave a valid empty file.
        }
        try (var source = new ParquetTransactionSource(path)) {
            assertNull(source.next());
        }
    }

    @Test void corruptParquetFailsRatherThanReturningAnEmptyResult() throws Exception {
        Path path = Files.writeString(directory.resolve("corrupt.parquet"), "not parquet");
        assertThrows(IOException.class, () -> {
            try (var source = new ParquetTransactionSource(path)) { source.next(); }
        });
    }

    private static GenericRecord trade(String id, long amount) {
        var row = new GenericData.Record(SCHEMA);
        row.put("trade_id", id); row.put("trade_date", "2026-01-01"); row.put("account_id", "acc");
        row.put("institution_id", "bank"); row.put("transaction_type", "PAY"); row.put("currency", "CNY");
        row.put("amount_cents", amount);
        return row;
    }

    private static OutputFile output(Path path) {
        return new OutputFile() {
            @Override public PositionOutputStream create(long hint) throws IOException {
                return stream(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
            }
            @Override public PositionOutputStream createOrOverwrite(long hint) throws IOException {
                return stream(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING));
            }
            @Override public boolean supportsBlockSize() { return false; }
            @Override public long defaultBlockSize() { return 0; }
        };
    }

    private static PositionOutputStream stream(FileChannel channel) {
        return new PositionOutputStream() {
            @Override public long getPos() throws IOException { return channel.position(); }
            @Override public void write(int value) throws IOException { write(new byte[]{(byte) value}); }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
                while (buffer.hasRemaining()) { channel.write(buffer); }
            }
            @Override public void close() throws IOException { channel.close(); }
        };
    }
}
