package com.chia7712.hlose.tool;

import com.chia7712.hlose.RowConsumer;
import com.chia7712.hlose.RowFunction;
import com.chia7712.hlose.RowQueue;
import com.chia7712.hlose.RowQueueBuilder;
import com.chia7712.hlose.Supplier;
import com.chia7712.hlose.SupplierUtil;
import com.chia7712.hlose.TableSupplier;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public final class LoadKeyFromFile {
  private static final Log LOG = LogFactory.getLog(LoadKeyFromFile.class);
  public static LoadKeyFromFile newJob(byte[] family, Collection<byte[]> quals) {
    return new LoadKeyFromFile(family, quals);
  }
  private final List<byte[]> qualifiers;
  private final byte[] family;
  private byte[] value = new byte[15];
  private long putRowStart = 0;
  private long putRowEnd = Long.MAX_VALUE;
  private int putBatch = 30;
  private int putThreads = 5;
  private int deleteBatch = 30;
  private int deleteThreads = 5;
  private long deleteRowStart = 0;
  private long deleteRowEnd = 100;
  private boolean enablePUt = true;
  private boolean enableDelete = true;
  private TableSupplier tableSupplier;
  private File keyFile;
  LoadKeyFromFile(byte[] family, Collection<byte[]> quals) {
    this.family = family;
    this.qualifiers = new ArrayList<>(quals);
  }
  LoadKeyFromFile disablePut() {
    this.enablePUt = false;
    return this;
  }
  LoadKeyFromFile disableDelete() {
    this.enableDelete = false;
    return this;
  }
  LoadKeyFromFile setValue(byte[] value) {
    this.value = value;
    return this;
  }
  LoadKeyFromFile setPutRowRange(long rowStart, long rowEnd) {
    this.putRowStart = Math.min(rowStart, rowEnd);
    this.putRowEnd = Math.max(rowStart, rowEnd);
    return this;
  }

  LoadKeyFromFile setDeleteRowRange(long rowStart, long rowEnd) {
    this.deleteRowStart = Math.min(rowStart, rowEnd);
    this.deleteRowEnd = Math.max(rowStart, rowEnd);
    return this;
  }

  LoadKeyFromFile setPutBatch(int batch) {
    this.putBatch = batch;
    return this;
  }
  LoadKeyFromFile setPutThread(int threads) {
    this.putThreads = threads;
    return this;
  }
  LoadKeyFromFile setDeleteBatch(int batch) {
    this.deleteBatch = batch;
    return this;
  }
  LoadKeyFromFile setDeleteThread(int threads) {
    this.deleteThreads = threads;
    return this;
  }
  LoadKeyFromFile setTableSupplier(TableSupplier tableSupplier) {
    this.tableSupplier= tableSupplier;
    return this;
  }

  LoadKeyFromFile setKeyFile(File keyFile) {
    this.keyFile = keyFile;
    return this;
  }

  private void check() {
    Objects.requireNonNull(tableSupplier);
    Objects.requireNonNull(keyFile);
    Objects.requireNonNull(family);
    Objects.requireNonNull(value);
    Objects.requireNonNull(qualifiers);
  }

  Result run() throws Exception {
    check();
    final long putCount = enablePUt ? runPut() : -1;
    final long deleteCount = enableDelete ? runDelete() : -1;
    return new Result() {

      @Override
      public long getPutCount() {
        return putCount;
      }

      @Override
      public long getDeleteCount() {
        return deleteCount;
      }

      @Override
      public String toString() {
        return "putCount:" + (putCount < 0 ? "N/A" : putCount)
            + " deleteCount:" + (deleteCount < 0 ? "N/A" : deleteCount);
      }
    };
  }

  private long runDelete() throws Exception {
    RowFunction<Delete> f = new RowFunction<Delete>() {
      @Override
      public Delete apply(byte[] data) {
        return new Delete(data);
      }
    };
    List<Supplier<RowConsumer<Delete>>> consumers = new ArrayList<>(deleteThreads);
    for (int thread = 0; thread != deleteThreads; ++thread) {
      consumers.add(SupplierUtil.toDeleteConsumer(tableSupplier, deleteBatch));
    }
    try (RowQueue<Delete> queue = RowQueueBuilder.newBuilder(f)
      .setRowRange(deleteRowStart, deleteRowEnd)
      .setPrefix("DELETE")
      .setRowLoader(SupplierUtil.toRowLoader(keyFile))
      .addConsumer(consumers)
      .build()) {
      queue.await();
      return queue.getAcceptedRowCount();
    }
  }

  private long runPut() throws Exception {
    RowFunction<Put> f = new RowFunction<Put>() {
      @Override
      public Put apply(byte[] data) {
        Put put = new Put(data);
        for (byte[] q : qualifiers) {
          put.addColumn(family, q, value);
        }
        return put;
      }
    };
    List<Supplier<RowConsumer<Put>>> consumers = new ArrayList<>(putThreads);
    for (int thread = 0; thread != putThreads; ++thread) {
      consumers.add(SupplierUtil.toPutConsumer(tableSupplier, putBatch));
    }
    try (RowQueue<Put> queue = RowQueueBuilder.newBuilder(f)
      .setRowRange(putRowStart, putRowEnd)
      .setPrefix("PUT")
      .setRowLoader(SupplierUtil.toRowLoader((keyFile)))
      .addConsumer(consumers)
      .build()) {
      queue.await();
      return queue.getAcceptedRowCount();
    }
  }
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new RuntimeException("Where is the row file? <table name> <file> (hbase.zookeeper.quorum)");
    }
    final TableName name = TableName.valueOf(args[0]);
    File rowKeyFile = new File(args[1]);
    final List<byte[]> qualifiers =
      Arrays.asList(Bytes.toBytes("at"), Bytes.toBytes("ct"), Bytes.toBytes("gu"));
    Configuration config = HBaseConfiguration.create();
    if (args.length > 2) {
      config.set("hbase.zookeeper.quorum", args[2]);
    }
    try (Connection conn = ConnectionFactory.createConnection(config);
      Admin admin = conn.getAdmin();
      Table table = conn.getTable(name)) {
      byte[] family;
      if (admin.tableExists(name)) {
        family = admin.getTableDescriptor(name).getColumnFamilies()[0].getName();
      } else {
        HTableDescriptor desc = new HTableDescriptor(name);
        desc.setRegionReplication(1).addFamily(
          new HColumnDescriptor(Bytes.toBytes("fm"))
            .setDataBlockEncoding(DataBlockEncoding.NONE)
            .setBloomFilterType(BloomType.NONE)
            .setMaxVersions(3)
            .setCompressionType(Compression.Algorithm.GZ)
            .setMinVersions(0)
            .setTimeToLive(HConstants.FOREVER)
            .setKeepDeletedCells(KeepDeletedCells.FALSE)
            .setBlocksize(65536)
            .setInMemory(false)
            .setBlockCacheEnabled(true));
        family = desc.getColumnFamilies()[0].getName();
        admin.createTable(desc);
      }
      Result result = LoadKeyFromFile.newJob(family, qualifiers)
        .setPutRowRange(0, Long.MAX_VALUE)
        .setDeleteRowRange(671655L, 20582714L)
        .setPutBatch(30)
        .setDeleteBatch(30)
        .setKeyFile(rowKeyFile)
        .setPutThread(5)
        .setDeleteBatch(5)
        .setValue(new byte[15])
        .setTableSupplier(SupplierUtil.toTableSupplier(conn, name))
        .run();
      LOG.info("[CHIA] " + result);
      for (Alter alter : Alter.values()) {
        switch (alter) {
          case SPLIT: admin.split(name);break;
          case FLUSH: admin.flush(name);break;
          default:break;
        }
        Counter counter = CountTable.newJob()
          .setTableSupplier(SupplierUtil.toResultScannerSupplier(table,
            new Scan().setScanMetricsEnabled(true)))
          .setPrefix(alter.name())
          .run();
        LOG.info("[CHIA] " + counter);
      }
    }
  }
}
