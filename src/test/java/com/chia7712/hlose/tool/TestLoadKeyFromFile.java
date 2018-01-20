package com.chia7712.hlose.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.chia7712.hlose.Supplier;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLoadKeyFromFile {
  private static final Log LOG = LogFactory.getLog(TestLoadKeyFromFile.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("fm");
  private static final List<byte[]> QUALIFIERS =
    Arrays.asList(Bytes.toBytes("at"), Bytes.toBytes("ct"), Bytes.toBytes("gu"));
  private static final boolean PRE_SPLIT = false;
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
      new HColumnDescriptor(FAMILY)
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
    if (PRE_SPLIT) {
      byte[][] split = new byte[][] {
        Bytes.toBytes("G000007301O_T00000Ox01t_0_63a79701-59ec-437d-8534-fa8bfe90fbbb_M00000Eg") };
      UTIL.getHBaseAdmin().createTable(desc, split);
    } else {
      UTIL.getHBaseAdmin().createTable(desc);
    }
    Supplier<Table> supplier = new Supplier<Table> () {

      @Override
      public Table generate() throws IOException {
        return UTIL.getConnection().getTable(name);
      }
    };
    List<Alter> alters = Arrays.asList(Alter.values());

    Result result = LoadKeyFromFile.newJob(FAMILY, QUALIFIERS)
      .setPutRowRange(0, Long.MAX_VALUE)
      .setDeleteRowRange(671655L, 20582714L)
      .setPutBatch(30)
      .setDeleteBatch(30)
      .setKeyFile(new File("/home/chia7712/rowkey.log"))
      .setPutThread(5)
      .setDeleteBatch(5)
      .setValue(new byte[15])
      .setTableSupplier(supplier)
      .run(alters);
    LOG.info("[CHIA] result:" + result);
    assertNotEquals(0, result.getPutCount());
    assertNotEquals(0, result.getDeleteCount());
    for (Counter counter : result.getCounters()) {
      assertEquals("counter:" + counter, result.getPutCount() - result.getDeleteCount()
        , counter.get());
    }
  }
}