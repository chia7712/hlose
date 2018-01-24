package com.chia7712.hlose;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public interface ResultScannerSupplier extends Supplier<ResultScanner> {
  Scan getScan();
  TableName getTableName();
}
