package com.chia7712.hlose.tool;

import com.chia7712.hlose.ResultScannerSupplier;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CountTable {
  public static CountTable newJob() {
    return new CountTable();
  }
  private Supplier<Table> tableSupplier;
  private Supplier<Admin> adminSupplier;
  private Alter alter = Alter.NONE;
  private boolean enableScanLog = false;
  private boolean enableScanMetrics = false;

  CountTable setTableSupplier(Supplier<Table> tableSupplier) {
    this.tableSupplier = tableSupplier;
    return this;
  }

  CountTable setAlter(Alter alter) {
    this.alter = alter;
    return this;
  }
  CountTable setScanLog(boolean enableScanLog) {
    this.enableScanLog = enableScanLog;
    return this;
  }
  CountTable setScanMetrics(boolean enableScanMetrics) {
    this.enableScanMetrics = enableScanMetrics;
    return this;
  }
  CountTable setAdminSupplier(Supplier<Admin> adminSupplier) {
    this.adminSupplier = adminSupplier;
    return this;
  }

  private void check() {
    Objects.requireNonNull(tableSupplier);
    Objects.requireNonNull(adminSupplier);
  }
  Counter run() throws Exception {
    check();
    final Scan scan = new Scan()
      .setFilter(new FirstKeyOnlyFilter())
      .setScanMetricsEnabled(enableScanMetrics);
    if (enableScanLog) {
      scan.setAttribute("chia7712.log", Bytes.toBytes(alter.name()));
    }

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

    try (Table table = tableSupplier.generate();
      RowQueue<byte[]> queue = RowQueueBuilder.newBuilder()
      .setPrefix(alter.name())
      .setRowLoader(SupplierUtil.toRowLoader(new ResultScannerSupplier() {
        @Override
        public Scan getScan() {
          return scan;
        }

        @Override
          public ResultScanner generate() throws IOException {
            return table.getScanner(scan);
          }
        }))
      .addConsumer(rowConsumerSupplier)
      .build();
      Admin admin = adminSupplier.generate()) {
      LOG.info("[CHIA] alter:" + alter);
      switch (alter) {
        case SPLIT:
          admin.split(table.getName());
          break;
        case FLUSH:
          admin.flush(table.getName());
          break;
        default:
          break;
      }
      queue.await();
      return new Counter() {
        @Override
        public Alter getAlter() {
          return alter;
        }

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
          return alter.name() + ":" + get()
            + "[" + (getStartRow() == null ? "null" : Bytes.toStringBinary(getStartRow())) + "]"
            + "[" + (getEndRow() == null ? "null" : Bytes.toStringBinary(getEndRow())) + "]"
            + getMetrics().toString();
        }
      };
    }
  }
  private static final Log LOG = LogFactory.getLog(LoadKeyFromFile.class);
  public static void main(String[] args) throws Exception {
    if (args.length <= 0) {
      throw new RuntimeException("Where is the alters(none, flush, split)? <alter>");
    }
    List<Alter> alters = new ArrayList<>(args.length);
    for (String arg : args) {
      alters.add(Alter.valueOf(arg.toUpperCase()));
    }
    final TableName name = TableName.valueOf("testLoadLargeData");
    try (Connection conn = ConnectionFactory.createConnection();
      Admin admin = conn.getAdmin();) {
      if (!admin.tableExists(name)) {
        throw new RuntimeException("Where is the table?");
      }
      Supplier<Table> supplier = new Supplier<Table> () {

        @Override
        public Table generate() throws IOException {
          return conn.getTable(name);
        }
      };

      List<Counter> counters = new ArrayList<>(alters.size());
      for (Alter alter : alters) {
        Counter counter = CountTable.newJob()
          .setTableSupplier(supplier)
          .setAlter(alter)
          .run();
        LOG.info("[CHIA] " + counter);
        counters.add(counter);
      }
      LOG.info("[CHIA] " + counters);
    }
  }

  private CountTable() {
  }
}
