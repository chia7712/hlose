package com.chia7712.hlose.tool;

import java.util.Map;

public interface Counter {
  Alter getAlter();
  long get();
  byte[] getStartRow();
  byte[] getEndRow();
  Map<String, Long> getMetrics();
}
