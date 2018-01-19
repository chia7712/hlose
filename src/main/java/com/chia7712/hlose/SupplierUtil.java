package com.chia7712.hlose;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public final class SupplierUtil {
  public static Supplier<Table> toTable(Connection conn, TableName name) {
    return new Supplier<Table>() {
      @Override
      public Table generate() throws IOException {
        return conn.getTable(name);
      }
    };
  }
  public static Supplier<ResultScanner> toResultScanner(Table table, Scan scan) {
    return new Supplier<ResultScanner>() {
      @Override
      public ResultScanner generate() throws IOException {
        return table.getScanner(scan);
      }
    };
  }

  public static Supplier<RowConsumer<byte[]>> toNoneConsumer() {
    return new Supplier<RowConsumer<byte[]>>() {
      @Override
      public RowConsumer<byte[]> generate() throws IOException {
        return new RowConsumer<byte[]>() {
          @Override
          public void close() throws Exception {
          }

          @Override
          public void apply(byte[] data) throws IOException {
          }
        };
      }
    };
  }

  public static Supplier<RowConsumer<Delete>> toDeleteConsumer(Connection conn, TableName name, int batch) {
    return new Supplier<RowConsumer<Delete>>() {
      @Override
      public RowConsumer<Delete> generate() throws IOException {
        return new RowConsumer<Delete>() {
          private final Table t = toTable(conn, name).generate();
          private final List<Delete> deletes = new ArrayList<>(batch);
          @Override
          public void close() throws Exception {
            try {
              flush();
            } finally {
              t.close();
            }
          }

          @Override
          public void apply(Delete Delete) throws IOException {
            deletes.add(Delete);
            if (deletes.size() >= batch) {
              flush();
            }
          }

          private void flush() throws IOException {
            if (!deletes.isEmpty()) {
              t.delete(deletes);
              deletes.clear();
            }
          }
        };
      }
    };
  }

  public static Supplier<RowConsumer<Put>> toPutConsumer(Connection conn, TableName name, int batch) {
    return new Supplier<RowConsumer<Put>>() {
      @Override
      public RowConsumer<Put> generate() throws IOException {
        return new RowConsumer<Put>() {
          private final Table t = toTable(conn, name).generate();
          private final List<Put> puts = new ArrayList<>(batch);
          @Override
          public void close() throws Exception {
            try {
              flush();
            } finally {
              t.close();
            }
          }

          @Override
          public void apply(Put put) throws IOException {
            puts.add(put);
            if (puts.size() >= batch) {
              flush();
            }
          }

          private void flush() throws IOException {
            if (!puts.isEmpty()) {
              t.put(puts);
              puts.clear();
            }
          }
        };
      }
    };
  }

  public static Supplier<RowLoader> toRowLoader(Table table, Scan scan) {
    return new Supplier<RowLoader>() {
      @Override
      public RowLoader generate() throws IOException {
        return new RowLoader() {
          private final ResultScanner scanner = toResultScanner(table, scan).generate();
          private final Iterator<Result> iter = scanner.iterator();
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public byte[] next() {
            return iter.next().getRow();
          }

          @Override
          public void close() throws Exception {
            scanner.close();
          }
        };
      }
    };

  }

  public static Supplier<RowLoader> toRowLoader(File file) {
    return new Supplier<RowLoader>() {
      @Override
      public RowLoader generate() throws IOException {
        return new RowLoader() {
          private final BufferedReader r = new BufferedReader(new FileReader(file));
          private String line = null;
          @Override
          public void close() throws Exception {
            r.close();
          }

          @Override
          public boolean hasNext() {
            tryLoad();
            return line != null;
          }

          @Override
          public byte[] next() {
            tryLoad();
            try {
              if (line == null) {
                throw new NoSuchElementException();
              }
              return Bytes.toBytes(line);
            } finally {
              line = null;
            }

          }

          private void tryLoad() {
            if (line == null) {
              try {
                line = r.readLine();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }
        };
      }
    };
  }
}
