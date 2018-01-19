package com.chia712.hlose;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.chia7712.hlose.RowConsumer;
import com.chia7712.hlose.RowFunction;
import com.chia7712.hlose.RowQueue;
import com.chia7712.hlose.RowQueueBuilder;
import com.chia7712.hlose.Supplier;
import com.chia7712.hlose.SupplierUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKeyFromFile {
  private static final Log LOG = LogFactory.getLog(TestKeyFromFile.class);
  private static final File ROW_KEY_FILE = new File("/home/chia7712/rowkey.log");
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("fm");
  private static final List<byte[]> QUALIFIERS =
    Arrays.asList(Bytes.toBytes("at"), Bytes.toBytes("ct"), Bytes.toBytes("gu"));
  private static final byte[] VALUE = new byte[15];
  private static final long PUT_ROWS_START = 0;
  private static final long PUT_ROWS_END = 50L * 1000L;
  private static final int BATCH = 30;
  private static final int WRITE_THREAD = 5;
  private static final long DELETE_ROW_START = 0;
  private static final long DELETE_ROW_END = 100;
//  private static final long DELETE_ROW_START = 671655L;
//  private static final long DELETE_ROW_END = 20582714L;
  private static final int READ_THREAD = 1;
  private static final boolean PRE_SPLIT = false;
  private static final int RETRY_LIMIT = 2;
  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 2000000)
  public void testLoadLargeData() throws Exception {
    TableName name = TableName.valueOf("testLoadLargeData");
    HTableDescriptor desc = new HTableDescriptor(name);
    desc.setRegionReplication(1).addFamily(
      new HColumnDescriptor(FAMILY).setDataBlockEncoding(DataBlockEncoding.NONE)
        .setBloomFilterType(BloomType.NONE).setMaxVersions(3)
        .setCompressionType(Compression.Algorithm.GZ).setMinVersions(0)
        .setTimeToLive(HConstants.FOREVER).setKeepDeletedCells(KeepDeletedCells.FALSE)
        .setBlocksize(65536).setInMemory(false).setBlockCacheEnabled(true));
    if (PRE_SPLIT) {
      byte[][] split = new byte[][] {
        Bytes.toBytes("G000007301O_T00000Ox01t_0_63a79701-59ec-437d-8534-fa8bfe90fbbb_M00000Eg") };
      UTIL.getHBaseAdmin().createTable(desc, split);
    } else {
      UTIL.getHBaseAdmin().createTable(desc);
    }

    try (Admin admin = UTIL.getHBaseAdmin();
      Table table = UTIL.getConnection().getTable(name)) {
      final long totalPuts = runPut(name);
      assertNotEquals(0, totalPuts);
      assertEquals(totalPuts, count(table, "v0 check"));
      final long deletedDeletes = runDelete(name);
      assertNotEquals("total row:" + totalPuts, 0, deletedDeletes);
      assertNotEquals(totalPuts, deletedDeletes);

      assertEquals(totalPuts - deletedDeletes, count(table, "v1 check"));

      doSplit(admin, table, totalPuts - deletedDeletes, "v2 check");
      doFlush(admin, table, totalPuts - deletedDeletes, "v3 check");
      doSplit(admin, table, totalPuts - deletedDeletes, "v4 check");
    }
  }

  private static void doFlush(Admin admin, Table table, long expectedRows, String prefix) throws Exception {
    admin.flush(table.getName());
    doCount(table, expectedRows, prefix);
  }

  private static void doSplit(Admin admin, Table table, long expectedRows, String prefix) throws Exception {
    admin.split(table.getName());
    doCount(table, expectedRows, prefix);
  }

  private static void doCount(Table table, long expectedRows, String prefix) throws Exception {
    List<Long> countHistory = new ArrayList<>(RETRY_LIMIT);
    for (int i = 0; i != RETRY_LIMIT; ++i) {
      long actual = count(table, prefix);
      if (actual == expectedRows) {
        if (!countHistory.isEmpty()) {
          LOG.error(countHistory);
        }
        return;
      }
      countHistory.add(actual);
    }
    throw new RuntimeException("Expected:" + expectedRows + ", actual:" + countHistory);
  }

  private static long runPut(final TableName name) throws Exception {
    RowFunction<Put> f = new RowFunction<Put>() {
      @Override
      public Put apply(byte[] data) {
        Put put = new Put(data);
        for (byte[] q : QUALIFIERS) {
          put.addColumn(FAMILY, q, VALUE);
        }
        return put;
      }
    };
    List<Supplier<RowConsumer<Put>>> consumers = new ArrayList<>(WRITE_THREAD);
    for (int thread = 0; thread != WRITE_THREAD; ++thread) {
      consumers.add(SupplierUtil.toPutConsumer(UTIL.getConnection(), name, BATCH));
    }
    try (RowQueue<Put> queue = RowQueueBuilder.newBuilder(f)
      .setRowStart(PUT_ROWS_START)
      .setRowEnd(PUT_ROWS_END)
      .setPrefix("PUT")
      .setRowLoader(SupplierUtil.toRowLoader((ROW_KEY_FILE)))
      .addConsumer(consumers)
      .build()) {
      queue.await();
      return queue.getAcceptedRowCount();
    }
  }

  private static long runDelete(final TableName name) throws Exception {
    RowFunction<Delete> f = new RowFunction<Delete>() {
      @Override
      public Delete apply(byte[] data) {
        return new Delete(data);
      }
    };
    List<Supplier<RowConsumer<Delete>>> consumers = new ArrayList<>(READ_THREAD);
    for (int thread = 0; thread != READ_THREAD; ++thread) {
      consumers.add(SupplierUtil.toDeleteConsumer(UTIL.getConnection(), name, BATCH));
    }
    try (RowQueue<Delete> queue = RowQueueBuilder.newBuilder(f)
      .setRowStart(DELETE_ROW_START)
      .setRowEnd(DELETE_ROW_END)
      .setPrefix("DELETE")
      .setRowLoader(SupplierUtil.toRowLoader(ROW_KEY_FILE))
      .addConsumer(consumers)
      .build()) {
      queue.await();
      return queue.getAcceptedRowCount();
    }
  }

  private static Scan getScan() {
    Scan scan = new Scan();
    //    scan.setCacheBlocks(false);
    scan.setFilter(new FirstKeyOnlyFilter());
    return scan;
  }

  private static long count(final Table table, final String prefix) throws Exception{
    try (RowQueue<byte[]> queue = RowQueueBuilder.newBuilder()
      .setPrefix(prefix)
      .setRowLoader(SupplierUtil.toRowLoader(table, getScan()))
      .addConsumer(SupplierUtil.toNoneConsumer())
      .build()) {
      queue.await();
      return queue.getAcceptedRowCount();
    }
  }
}
