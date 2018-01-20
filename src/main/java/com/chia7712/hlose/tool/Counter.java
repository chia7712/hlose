package com.chia7712.hlose.tool;

public interface Counter {
  Alter getAlter();
  long get();
  byte[] getStartRow();
  byte[] getEndRow();
}
