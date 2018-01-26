package com.chia7712.hlose;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.util.Bytes;

public final class SupplierUtil {
  private static final Log LOG = LogFactory.getLog(SupplierUtil.class);
  public static TableSupplier toTableSupplier(final Connection conn, final TableName name) {
    return new TableSupplier() {
      @Override
      public Table generate() throws IOException {
        return conn.getTable(name);
      }
      @Override
      public TableName getTableName() {
        return name;
      }
    };
  }

  public static Supplier<RowConsumer<Delete>> toDeleteConsumer(final TableSupplier supplier, final int batch) {
    return new Supplier<RowConsumer<Delete>>() {
      @Override
      public RowConsumer<Delete> generate() throws IOException {
        return new RowConsumer<Delete>() {
          private final Table t = supplier.generate();
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

  public static Supplier<RowConsumer<Put>> toPutConsumer(final Supplier<Table> supplier, final int batch) {
    return new Supplier<RowConsumer<Put>>() {
      @Override
      public RowConsumer<Put> generate() throws IOException {
        return new RowConsumer<Put>() {
          private final Table t = supplier.generate();
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

  public static Supplier<ResultScanner> toResultScannerSupplier(final Table table,
    final Scan scan) {
    return new Supplier<ResultScanner>() {
      @Override
      public ResultScanner generate() throws IOException {
        return new ResultScannerImpl(table, scan);
      }
    };
  }

  public static Supplier<RowLoader> toRowLoader(final Supplier<ResultScanner> supplier) {
    return new Supplier<RowLoader>() {
      @Override
      public RowLoader generate() throws IOException {
        return new RowLoader() {
          @Override
          public Map<String, Long> getMetrics() {
            ScanMetrics metrics = scanner.getScan().getScanMetrics();
            if (metrics == null) {
              return Collections.emptyMap();
            }
            return new TreeMap<>(metrics.getMetricsMap());
          }
          private final ResultScanner scanner = supplier.generate();
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
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close() throws Exception {
            scanner.close();
          }
        };
      }
    };

  }

  public static Supplier<RowLoader> toRowLoader(final File file) {
    return new Supplier<RowLoader>() {
      @Override
      public RowLoader generate() throws IOException {
        return new RowLoader() {
          private final BufferedReader r = new BufferedReader(new FileReader(file));
          private String line = null;
          private long count = 0;
          @Override
          public Map<String, Long> getMetrics() {
            Map<String, Long> m = new TreeMap<>();
            m.put("ROWS", count);
            return m;
          }
          @Override
          public void close() throws Exception {
            r.close();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
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
                ++count;
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }
        };
      }
    };
  }

  private static class ResultScannerImpl implements ResultScanner {
    private final org.apache.hadoop.hbase.client.ResultScanner scanner;
    private final Scan scan;
    private final TableName name;
    ResultScannerImpl(Table table, final Scan scan) throws IOException {
      this.scan = new Scan(scan);
      this.scanner = table.getScanner(this.scan);
      this.name = table.getName();
    }
    @Override
    public Scan getScan() {
      return scan;
    }

    @Override
    public TableName getTableName() {
      return name;
    }

    @Override
    public Result next() throws IOException {
      return scanner.next();
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
      return scanner.next(nbRows);
    }

    @Override
    public void close() {
      scanner.close();
    }

    @Override
    public boolean renewLease() {
      return scanner.renewLease();
    }

    @Override
    public ScanMetrics getScanMetrics() {
      return scanner.getScanMetrics();
    }

    @Override
    public Iterator<Result> iterator() {
      return scanner.iterator();
    }
  }
}
