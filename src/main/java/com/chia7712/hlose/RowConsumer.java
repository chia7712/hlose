package com.chia7712.hlose;

import java.io.IOException;

public interface RowConsumer<T> extends AutoCloseable {
  void apply(T t) throws IOException;
}
