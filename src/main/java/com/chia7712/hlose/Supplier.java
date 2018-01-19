package com.chia7712.hlose;

import java.io.IOException;

public interface Supplier<T> {
  T generate() throws IOException;
}
