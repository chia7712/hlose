package com.chia7712.hlose;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;

public interface ResultScanner extends org.apache.hadoop.hbase.client.ResultScanner {
  Scan getScan();
  TableName getTableName();
}
