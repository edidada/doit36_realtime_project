package cn.doitedu.fin.olap;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Local NIO adapter: no Hadoop FileSystem or Windows winutils.exe is needed. */
public final class LocalParquetFile implements InputFile {
    private final Path path;

    public LocalParquetFile(Path path) {
        this.path = path;
    }

    @Override
    public long getLength() throws IOException {
        return java.nio.file.Files.size(path);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        return new SeekableInputStream() {
            @Override public long getPos() throws IOException { return channel.position(); }
            @Override public void seek(long position) throws IOException { channel.position(position); }
            @Override public int read() throws IOException {
                ByteBuffer value = ByteBuffer.allocate(1);
                return channel.read(value) == -1 ? -1 : Byte.toUnsignedInt(value.array()[0]);
            }
            @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                return channel.read(ByteBuffer.wrap(bytes, offset, length));
            }
            @Override public int read(ByteBuffer buffer) throws IOException { return channel.read(buffer); }
            @Override public void readFully(byte[] bytes) throws IOException {
                readFully(ByteBuffer.wrap(bytes));
            }
            @Override public void readFully(byte[] bytes, int offset, int length) throws IOException {
                readFully(ByteBuffer.wrap(bytes, offset, length));
            }
            @Override public void readFully(ByteBuffer buffer) throws IOException {
                while (buffer.hasRemaining()) {
                    if (channel.read(buffer) < 0) { throw new EOFException("Truncated Parquet input"); }
                }
            }
            @Override public void close() throws IOException { channel.close(); }
        };
    }
}
