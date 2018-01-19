package com.chia7712.hlose;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

public final class RowQueueBuilder<T> {
  public static RowQueueBuilder<byte[]> newBuilder() {
    RowFunction<byte[]> idle = new RowFunction<byte[]>() {
      @Override
      public byte[] apply(byte[] data) {
        return data;
      }
    };
    return new RowQueueBuilder(idle);
  }

  public static <T> RowQueueBuilder<T> newBuilder(RowFunction<T> function) {
    return new RowQueueBuilder(function);
  }

  private static final Log LOG = LogFactory.getLog(RowQueueBuilder.class);

  private final RowFunction<T> function;
  private int capacity = 500; //ROWS
  private long timeToLog = 10 * 1000; //ms
  private long rowStart = 0;
  private long rowEnd = Long.MAX_VALUE;
  private String prefix = "";
  private Supplier<RowLoader> supplier;
  private final List<Supplier<RowConsumer<T>>> consumers = new ArrayList<>();

  private RowQueueBuilder(RowFunction<T> function) {
    this.function = Objects.requireNonNull(function);
  }

  public RowQueueBuilder<T> setCapacity(int capacity) {
    this.capacity = capacity;
    return this;
  }

  public RowQueueBuilder<T> setTimeToLog(int timeToLog) {
    this.timeToLog = timeToLog;
    return this;
  }

  public RowQueueBuilder<T> setRowStart(long rowStart) {
    this.rowStart = rowStart;
    return this;
  }

  public RowQueueBuilder<T> setRowEnd(long rowEnd) {
    this.rowEnd = rowEnd;
    return this;
  }

  public RowQueueBuilder<T> setPrefix(String prefix) {
    this.prefix = prefix;
    return this;
  }

  public RowQueueBuilder<T> setRowLoader(Supplier<RowLoader> supplier) {
    this.supplier = supplier;
    return this;
  }

  public RowQueueBuilder<T> addConsumer(List<Supplier<RowConsumer<T>>> consumers) {
    this.consumers.addAll(consumers);
    return this;
  }

  public RowQueueBuilder<T> addConsumer(Supplier<RowConsumer<T>> consumer) {
    this.consumers.add(consumer);
    return this;
  }

  private CountDownLatch invokeConsumer(Executor executor, RowTaker<T> rows) {
    final CountDownLatch close = new CountDownLatch(consumers.size());
    for (Supplier<RowConsumer<T>> consumerSupplier : consumers) {
      try {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            try (RowConsumer<T> consumer = consumerSupplier.generate()) {
              while (true) {
                T row = rows.take();
                if (row == null) {
                  return;
                }
                consumer.apply(row);
              }
            } catch (Exception e) {
              LOG.error(e);
            } finally {
              close.countDown();
            }
          }
        });
      } catch (Throwable e) {
        close.countDown();
        throw e;
      }
    }
    return close;
  }

  public RowQueue<T> build() {
    return build(Executors.newFixedThreadPool(1 + consumers.size()));
  }

  private RowQueue<T> build(Executor executor) {
    final Supplier<RowLoader> supplier = this.supplier;
    final RowFunction<T> function = this.function;
    final BlockingQueue<T> rows = new ArrayBlockingQueue<>(capacity);
    final AtomicBoolean stop = new AtomicBoolean(false);
    final CountDownLatch close = new CountDownLatch(1);
    final AtomicLong acceptedRows = new AtomicLong(0);
    final long rowStart = this.rowStart;
    final long rowEnd = this.rowEnd;
    final String prefix = this.prefix;
    final long timeToLog = this.timeToLog;
    executor.execute(new Runnable() {
      private long rowIndex = 0;
      private long lastLog = System.currentTimeMillis();

      @Override
      public void run() {
        try (RowLoader loader = supplier.generate()) {
          while (!stop.get() && loader.hasNext()) {
            byte[] row = loader.next();
            ++rowIndex;
            if (rowIndex >= rowStart && rowIndex <= rowEnd) {
              acceptedRows.incrementAndGet();
              rows.put(function.apply(row));
            } else if (rowIndex > rowEnd) {
              break;
            }
            if (System.currentTimeMillis() - lastLog > timeToLog) {
              String msg =
                "rowIndex:" + rowIndex + ", acceptedRows:" + acceptedRows + ", rowStart:" + rowStart
                  + ", rowEnd:" + rowEnd + ", currentRow:" + (row == null ?
                  "null" :
                  Bytes.toStringBinary(row));
              LOG.info("[-------------------" + prefix + "-------------------] " + msg);
              lastLog = System.currentTimeMillis();
            }
          }
        } catch (Exception e) {
          LOG.error("interrupt the loader", e);
        } finally {
          stop.set(true);
          close.countDown();
          LOG.info("[-------------------" + prefix + "-------------------] " + "DONE:" + acceptedRows);
        }
      }
    });

    RowTaker<T> taker = new RowTaker<T>() {
      @Override
      public T take() throws InterruptedException {
        while (true) {
          T rval = rows.poll(1, TimeUnit.SECONDS);
          if (rval != null) {
            return rval;
          }
          if (rows.isEmpty() && close.getCount() <= 0) {
            return null;
          }
        }
      }
    };

    CountDownLatch consumerLatch = invokeConsumer(executor, taker);
    return new RowQueue<T>() {
      @Override
      public void await() throws InterruptedException {
        close.await();
        consumerLatch.await();
      }

      @Override
      public boolean isClosed() {
        return close.getCount() <= 0 && consumerLatch.getCount() <= 0;
      }

      @Override
      public void close() throws IOException, InterruptedException {
        stop.set(true);
        await();
      }

      @Override
      public long getAcceptedRowCount() {
        return acceptedRows.get();
      }

      @Override
      public long getRowStart() {
        return rowStart;
      }

      @Override
      public long getRowEnd() {
        return rowEnd;
      }
    };

  }

  private interface RowTaker<T> {
    T take() throws InterruptedException;
  }
}
