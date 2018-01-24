package com.chia7712.hlose.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLoadHFile {
  private static final Log LOG = LogFactory.getLog(TestLoadHFile.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("fm");
  private static final List<byte[]> QUALIFIERS =
    Arrays.asList(Bytes.toBytes("at"), Bytes.toBytes("ct"), Bytes.toBytes("gu"));
  private static final Path HDFS_FOLDER = new Path("/tmp/hfile/" + FAMILY);
  private static final List<Pair<byte[], String>> HFILES = new ArrayList<>();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster(1);

    UTIL.getDFSCluster().getFileSystem().mkdirs(HDFS_FOLDER);
    int localCount = 0;
    long localSize = 0;
    for (File f : new File("/home/chia7712/hfile").listFiles()) {
      if (f.isFile()) {
        localSize += f.length();
        Path localPath = new Path(f.getAbsolutePath());
        UTIL.getDFSCluster().getFileSystem().copyFromLocalFile(localPath, HDFS_FOLDER);
        ++localCount;
      }
    }
    assertNotEquals(0, localCount);
    int hdfsCount = 0;
    long hdfsSize = 0;
    for (FileStatus fs : UTIL.getDFSCluster().getFileSystem().listStatus(HDFS_FOLDER)) {
      LOG.info("[CHIA] fs:" + fs.toString());
      HFILES.add(Pair.newPair(FAMILY, fs.getPath().toString()));
      ++hdfsCount;
      hdfsSize += fs.getLen();
    }
    assertEquals(localCount, hdfsCount);
    assertEquals(localSize, hdfsSize);
    LOG.info("[CHIA] file count:" + localCount + ", file size:" + localSize);
    LOG.info("[CHIA] java.library.path=" + System.getProperty("java.library.path"));
    LOG.info("[CHIA] native:" + NativeCodeLoader.isNativeCodeLoaded());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 40000)
  public void testLoadHFile() throws Exception {
    final TableName name = TableName.valueOf("testLoadHFile");
    HTableDescriptor desc = new HTableDescriptor(name);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    try (Table table = UTIL.createTable(name, FAMILY)) {
      ProtobufUtil.bulkLoadHFile(((HConnection)UTIL.getConnection()).getClient(
        UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName()),
        HFILES, UTIL.getMiniHBaseCluster().getRegions(name).get(0).getRegionInfo().getRegionName(),
        true);
      long count = UTIL.countRows(table);
      assertNotEquals(0, count);
      LOG.info("[CHIA] count:" + count);
    }
  }
}
