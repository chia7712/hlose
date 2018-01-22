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
import org.apache.commons.collections.CollectionUtils;
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
    return new RowQueueBuilder<byte[]>(idle);
  }

  public static <T> RowQueueBuilder<T> newBuilder(RowFunction<T> function) {
    return new RowQueueBuilder<T>(function);
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

  public RowQueueBuilder<T> setRowRange(long start, long end) {
    this.rowStart = Math.min(start, end);
    this.rowEnd = Math.max(start, end);
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

  private CountDownLatch invokeConsumer(Executor executor, final RowTaker<T> rows) {
    final CountDownLatch close = new CountDownLatch(consumers.size());
    for (final Supplier<RowConsumer<T>> consumerSupplier : consumers) {
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

  private void check() {
    if (CollectionUtils.isEmpty(consumers)) {
      throw new RuntimeException("Empty consumers");
    }
    Objects.requireNonNull(supplier);
    Objects.requireNonNull(function);
  }

  public RowQueue<T> build() throws Exception {
    return build(Executors.newFixedThreadPool(1 + consumers.size()));
  }

  private RowQueue<T> build(final ExecutorService executor) throws Exception {
    check();

    final RowLoadWorker<T> worker = new RowLoadWorker<T>(supplier, rowStart, rowEnd, capacity,
      function, timeToLog, prefix);


    executor.execute(worker);

    final CountDownLatch consumerLatch = invokeConsumer(executor, worker);
    return new RowQueue<T>() {
      @Override
      public void await() throws InterruptedException {
        worker.await();
        consumerLatch.await();
        executor.shutdown();
      }

      @Override
      public boolean isClosed() {
        return worker.isClosed() && consumerLatch.getCount() <= 0;
      }

      @Override
      public void close() throws IOException, InterruptedException {
        worker.stop();
        await();
      }

      @Override
      public long getAcceptedRowCount() {
        return worker.getAcceptedRowCount();
      }

      @Override
      public RowLoader getRowLoader() {
        return worker.getLoader();
      }
    };

  }

  private static class RowLoadWorker<T> implements Runnable, RowTaker<T> {
    private long rowIndex = 0;
    private long lastLog = System.currentTimeMillis();
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final RowLoader loader;
    private final long rowStart;
    private final long rowEnd;
    private final AtomicLong acceptedRows = new AtomicLong(0);
    private final BlockingQueue<T> rows;
    private final RowFunction<T> function;
    private final String prefix;
    private final long timeToLog;
    private final CountDownLatch close = new CountDownLatch(1);
    RowLoadWorker(Supplier<RowLoader> rowLoaderSupplier, long rowStart, long rowEnd,
      int capacity, final RowFunction<T> function, long timeToLog, final String prefix) throws Exception {
      loader = rowLoaderSupplier.generate();
      this.rowEnd = rowEnd;
      this.rowStart = rowStart;
      this.rows = new ArrayBlockingQueue<>(capacity);
      this.function = function;
      this.timeToLog = timeToLog;
      this.prefix = prefix;
    }

    public boolean isClosed() {
      return close.getCount() <= 0;
    }
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
    public RowLoader getLoader() {
      return loader;
    }

    public void stop() {
      stop.set(true);
    }

    public void await() throws InterruptedException {
      close.await();
    }

    public long getAcceptedRowCount() {
      return acceptedRows.get();
    }
    @Override
    public void run() {
      try (RowLoader loader = getLoader()) {
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
  }

  private interface RowTaker<T> {
    T take() throws InterruptedException;
  }
}
