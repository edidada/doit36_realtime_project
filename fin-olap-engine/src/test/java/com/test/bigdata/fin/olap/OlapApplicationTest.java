package com.test.bigdata.fin.olap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import static org.junit.jupiter.api.Assertions.*;

class OlapApplicationTest {
    @TempDir Path directory;

    @Test void logsCompletedReportThroughFacadeToLog4j2() throws Exception {
        Path output = directory.resolve("report.csv");
        try (LogCapture capture = new LogCapture()) {
            OlapApplication.main(arguments(output));
            assertEquals(1, capture.records.size());
            LogEvent record = capture.records.get(0);
            assertEquals(Level.INFO, record.getLevel());
            assertEquals(OlapApplication.class.getName(), record.getLoggerName());
            String message = record.getMessage().getFormattedMessage();
            assertTrue(message.matches("snapshot=log-test sha256=[0-9a-f]{64} scanned=1 matched=1 groups=1 output=.*"));
            assertTrue(message.endsWith(output.toString()));
            assertTrue(Files.readString(output).contains("2026-01-01,account-a,bank-a,PAY,CNY,1,12500"));
        }
    }

    @Test void doesNotLogSuccessWhenOutputCannotBePublished() throws Exception {
        Path output = Files.writeString(directory.resolve("existing.csv"), "keep original");
        String[] args = arguments(output);
        try (LogCapture capture = new LogCapture()) {
            assertThrows(IOException.class, () -> OlapApplication.main(args));
            assertTrue(capture.records.isEmpty());
            assertEquals("keep original", Files.readString(output));
        }
    }

    private String[] arguments(Path output) throws IOException {
        Path input = Files.writeString(directory.resolve("input.csv"),
                String.join(",", CsvTransactionSource.HEADER) + "\n"
                        + "t001,2026-01-01,account-a,bank-a,PAY,CNY,12500\n");
        return new String[]{input.toString(), output.toString(), "log-test", "2026-01-01", "2026-01-31"};
    }

    private static final class LogCapture extends AbstractAppender implements AutoCloseable {
        private final Logger logger = (Logger) LogManager.getLogger(OlapApplication.class);
        private final Level previousLevel = logger.getLevel();
        private final boolean previousAdditivity = logger.isAdditive();
        private final List<LogEvent> records = new ArrayList<>();

        private LogCapture() {
            super("test-capture", null, null, false, Property.EMPTY_ARRAY);
            start();
            logger.setLevel(Level.INFO);
            logger.setAdditive(false);
            logger.addAppender(this);
        }

        @Override public void append(LogEvent record) { records.add(record.toImmutable()); }
        @Override public void close() {
            logger.removeAppender(this);
            logger.setLevel(previousLevel);
            logger.setAdditive(previousAdditivity);
            stop();
        }
    }
}
