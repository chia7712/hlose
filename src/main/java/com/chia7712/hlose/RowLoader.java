package com.chia7712.hlose;

import java.util.Iterator;
import java.util.Map;

public interface RowLoader extends Iterator<byte[]>, AutoCloseable {
  Map<String, Long> getMetrics();
}
