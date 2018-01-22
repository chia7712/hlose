package com.chia7712.hlose;

import java.io.IOException;

public interface RowQueue<T> extends AutoCloseable {
  void await() throws InterruptedException;
  boolean isClosed();
  long getAcceptedRowCount();
  RowLoader getRowLoader();
}
