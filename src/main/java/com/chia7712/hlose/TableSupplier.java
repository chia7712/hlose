package com.chia7712.hlose;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

public interface TableSupplier extends Supplier<Table> {
  TableName getTableName();
}
