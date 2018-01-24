package com.chia7712.hlose.tool;

import com.chia7712.hlose.ResultScanner;
import com.chia7712.hlose.RowConsumer;
import com.chia7712.hlose.RowQueue;
import com.chia7712.hlose.RowQueueBuilder;
import com.chia7712.hlose.Supplier;
import com.chia7712.hlose.SupplierUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class CountTable {
  public static CountTable newJob() {
    return new CountTable();
  }
  private Supplier<ResultScanner> resultScannerSupplier;
  private String prefix = "";

  CountTable setTableSupplier(Supplier<ResultScanner> resultScannerSupplier) {
    this.resultScannerSupplier = resultScannerSupplier;
    return this;
  }

  CountTable setPrefix(String prefix) {
    this.prefix = prefix;
    return this;
  }


  private void check() {
    Objects.requireNonNull(resultScannerSupplier);
  }
  Counter run() throws Exception {
    check();

    final List<byte[]> rowCollector = new ArrayList<>(2);
    Supplier<RowConsumer<byte[]>> rowConsumerSupplier = new Supplier<RowConsumer<byte[]>>() {
      @Override
      public RowConsumer<byte[]> generate() throws IOException {
        return new RowConsumer<byte[]>() {

          @Override
          public void close() throws Exception {
          }

          @Override
          public void apply(byte[] bytes) throws IOException {
            switch (rowCollector.size()) {
              case 0:
              case 1:
                rowCollector.add(bytes);
                break;
              default:
                rowCollector.set(1, bytes);
                break;
            }
          }
        };
      }
    };

    try (RowQueue<byte[]> queue = RowQueueBuilder.newBuilder()
      .setPrefix(prefix)
      .setRowLoader(SupplierUtil.toRowLoader(resultScannerSupplier))
      .addConsumer(rowConsumerSupplier)
      .build();) {
      queue.await();
      return new Counter() {

        @Override
        public long get() {
          return queue.getAcceptedRowCount();
        }

        @Override
        public byte[] getStartRow() {
          return rowCollector.isEmpty() ? null : rowCollector.get(0);
        }

        @Override
        public byte[] getEndRow() {
          return rowCollector.isEmpty() ? null : rowCollector.get(rowCollector.size() - 1);
        }

        @Override
        public Map<String, Long> getMetrics() {
          return queue.getRowLoader().getMetrics();

        }

        @Override
        public String toString() {
          return prefix + ":" + get()
            + "[" + (getStartRow() == null ? "null" : Bytes.toStringBinary(getStartRow())) + "]"
            + "[" + (getEndRow() == null ? "null" : Bytes.toStringBinary(getEndRow())) + "]"
            + getMetrics().toString();
        }
      };
    }
  }

  private static final Log LOG = LogFactory.getLog(LoadKeyFromFile.class);
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new RuntimeException("Give me the table name, please?");
    }

    final TableName name = TableName.valueOf(args[0]);
    try (Connection conn = ConnectionFactory.createConnection();
      Admin admin = conn.getAdmin();
      Table table = conn.getTable(name)) {
      if (!admin.tableExists(name)) {
        throw new RuntimeException("Where is the table?");
      }
      Counter counter = CountTable.newJob()
        .setTableSupplier(SupplierUtil.toResultScannerSupplier(table,
          new Scan().setScanMetricsEnabled(true)))
        .setPrefix(Alter.NONE.name())
        .run();
      LOG.info("[CHIA] " + counter);
    }
  }

  private CountTable() {
  }
}
