package com.example.flink.source;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class IncrementSource extends RichParallelSourceFunction<Long> {

  private volatile boolean stop = false;
  private static final AtomicLong counter = new AtomicLong(100);

  public void run(SourceContext<Long> ctx) throws Exception {
    long localValue;
    while(!stop && (localValue = counter.incrementAndGet()) <= 200) {
      Thread.sleep(1000);
      synchronized (ctx.getCheckpointLock()) {
        ctx.collect(localValue);
      }
    }
    System.out.println("IncrementSource finished");

  }

  public void cancel() {
    stop = true;
  }
}
