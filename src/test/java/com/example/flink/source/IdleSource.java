package com.example.flink.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class IdleSource extends RichSourceFunction<Long> {

  private volatile boolean stop = false;
  private long counter = 0;

  public void run(SourceContext<Long> ctx) throws Exception {
    synchronized (ctx.getCheckpointLock()) {
      ctx.collect(counter++);
    }
    Thread.sleep(10000);
    System.out.println("IdleSource finished");
  }

  public void cancel() {
    stop = true;
  }
}
