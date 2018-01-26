package com.chia7712.hlose.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.chia7712.hlose.SupplierUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestLoadHFile {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(6000);
  private static final File ROW_KEY_FILE = new File("/home/chia7712/rowkey.log");
  private static final Log LOG = LogFactory.getLog(TestLoadHFile.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String FAMILY_STR = "cf";
  private static final byte[] FAMILY = Bytes.toBytes(FAMILY_STR);
  private static final Set<byte[]> QUALIFIERS;
  static {
    QUALIFIERS = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    QUALIFIERS.addAll(Arrays.asList(Bytes.toBytes("at"), Bytes.toBytes("ct"), Bytes.toBytes("gu")));
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster(1);
  }

  private static long copyFromLocalDirectly(File f, Path hdfsFolder) throws IOException {
    Path localPath = new Path(f.getAbsolutePath());
    UTIL.getTestFileSystem().copyFromLocalFile(localPath, hdfsFolder);
    return 0;
  }

  private static long copyFromLocalByCell(File f, Path hdfsFolder) throws IOException {
    Path localPath = new Path(f.getAbsolutePath());
    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(UTIL.getConfiguration());
    Path testFile = new Path(hdfsFolder, UUID.randomUUID().toString());
    try (HFile.Reader reader  = HFile.createReader(FileSystem.getLocal(UTIL.getConfiguration()),
      localPath, new CacheConfig(UTIL.getConfiguration()), UTIL.getConfiguration());
      FSDataOutputStream out = UTIL.getTestFileSystem().create(testFile)) {
      HFileScanner scanner = reader.getScanner(false, false);
      assertTrue(scanner.seekTo());
      HFileContext context = new HFileContext();
      context.setCompression(Compression.Algorithm.LZ4);
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(context);
      try (HFile.Writer writer = hFileFactory.create()){
        long count = 0;
        Cell last = null;
        while (scanner.next()) {
          ++count;
          Cell cell = scanner.getKeyValue();
          assertFalse(CellUtil.isDelete(cell));
          assertTrue(Bytes.equals(FAMILY, CellUtil.cloneFamily(cell)));
          if (last != null) {
            int cmp = CellComparator.compare(last, cell, false);
            assertTrue("cmp:" + cmp, cmp < 0);
          }
          writer.append(cell);
          last = cell;
        }
        return count;
      }
    }
  }

  private static List<Pair<byte[], String>> createHFile(File localFolder) throws IOException {
    Path hdfsFolder = new Path("/tmp/hfile/" + FAMILY_STR);
    List<Pair<byte[], String>> familyPaths = new ArrayList<Pair<byte[], String>>();
    UTIL.getTestFileSystem().mkdirs(hdfsFolder);
    int localCount = 0;
    long localSize = 0;
    long cellCount = 0;
    for (File f : localFolder.listFiles()) {
      if (f.isFile()) {
        cellCount += copyFromLocalDirectly(f, hdfsFolder);
        localSize += f.length();
        ++localCount;
      }
    }
    LOG.info("[CHIA] cellCount:" + cellCount);
    assertNotEquals(0, localCount);
    int hdfsCount = 0;
    long hdfsSize = 0;
    for (FileStatus fs : UTIL.getTestFileSystem().listStatus(hdfsFolder)) {
      familyPaths.add(Pair.newPair(FAMILY, fs.getPath().toString()));
      ++hdfsCount;
      hdfsSize += fs.getLen();
    }
    assertEquals(localCount, hdfsCount);
//    assertEquals(localSize, hdfsSize);
    return familyPaths;
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static long countCell(TableName name) throws Exception {
    try (Table table = UTIL.getConnection().getTable(name);
      org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(new Scan()
        .setRaw(true))) {
      long cellCount = 0;
      long deleteCellCount = 0;
      long deleteRowCount = 0;
      long rowCount = 0;
      for (org.apache.hadoop.hbase.client.Result r : scanner) {
        ++rowCount;
        cellCount += r.size();
        boolean hasDelete = false;
        for (Cell cell : r.rawCells()) {
          if (CellUtil.isDelete(cell)) {
            ++deleteCellCount;
            hasDelete = true;
          }
        }
        if (hasDelete) {
          ++deleteRowCount;
        }
      }
      LOG.info("[CHIA] cellCount:" + cellCount
        + ", rowCount:" + rowCount
        + ", deleteCellCount:" + deleteCellCount
        + ", deleteRowCount:" + deleteRowCount);
      return rowCount;
    }
  }

  @Test
  public void testLoadHFile() throws Exception {
    File localFolder = new File("/home/chia7712/hfile");
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("testLoadHFile-bulkload"));
    tableDesc.addFamily(new HColumnDescriptor(FAMILY)
      .setCompressionType(Compression.Algorithm.LZ4)
      .setMaxVersions(1000));
    UTIL.getHBaseAdmin().createTable(tableDesc);

    List<Pair<byte[], String>> hfiles = createHFile(localFolder);
    UTIL.getMiniHBaseCluster().getRegions(tableDesc.getTableName()).get(0)
      .bulkLoadHFiles(hfiles, true, null);

    countCell(tableDesc.getTableName());
  }

  private void doDelete(long putCount, TableName name) throws Exception {
    Result result = LoadKeyFromFile.newJob(FAMILY, QUALIFIERS)
      .disablePut()
      .setDeleteRowRange(671655L, 20582714L)
      .setDeleteBatch(30)
      .setKeyFile(ROW_KEY_FILE)
      .setDeleteBatch(5)
      .setValue(new byte[15])
      .setTableSupplier(SupplierUtil.toTableSupplier(UTIL.getConnection(), name))
      .run();
    LOG.info("[CHIA] " + result);
    assertNotEquals(0, result.getDeleteCount());

    try (Table table = UTIL.getConnection().getTable(name)) {
      Counter counter = CountTable.newJob()
        .setTableSupplier(SupplierUtil.toResultScannerSupplier(table,
          new Scan().setScanMetricsEnabled(true)))
        .setPrefix(Alter.NONE.name())
        .run();
      LOG.info("[CHIA] " + counter);
      assertEquals(putCount - result.getDeleteCount(), counter.get());
    }
  }
}
