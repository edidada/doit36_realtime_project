package cn.doitedu.fin.olap;

import java.io.IOException;

/** One row at a time; the engine owns and closes the source. */
public interface TransactionSource extends AutoCloseable {
    Transaction next() throws IOException;

    @Override
    void close() throws IOException;
}
