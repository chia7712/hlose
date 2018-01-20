package com.chia7712.hlose.tool;

import java.util.List;

public interface Result {
  long getPutCount();
  long getDeleteCount();
  List<Counter> getCounters();
}
