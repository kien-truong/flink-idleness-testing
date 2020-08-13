package com.example.flink;

import com.example.flink.source.IdleSource;
import com.example.flink.source.IncrementSource;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class IdlenessTest {
  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  @Test
  public void testIdleDetectionConnectedStream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(2);

    WatermarkStrategy<Long> wm2 = WatermarkStrategy.<Long>forBoundedOutOfOrderness(Duration.ofMillis(10))
        .withTimestampAssigner((SerializableTimestampAssigner<Long>) (aLong, l) -> aLong);
    DataStream<Long> s2 = env.addSource(new IncrementSource())
        .assignTimestampsAndWatermarks(wm2);

    WatermarkStrategy<Long> wm1 = WatermarkStrategy.<Long>forBoundedOutOfOrderness(Duration.ofMillis(10))
        .withTimestampAssigner((SerializableTimestampAssigner<Long>) (aLong, l) -> 1)
        .withIdleness(Duration.ofSeconds(3));
    DataStream<Long> s1 = env.addSource(new IdleSource())
        .assignTimestampsAndWatermarks(wm1);

    s2.connect(s1).process(new CollectProcess()).addSink(new CollectSink());

    env.execute();

    Assert.assertTrue(CollectSink.watermarks.contains(100L));
  }

  private static class CollectProcess extends CoProcessFunction<Long, Long, Long> {
    @Override
    public void processElement1(Long value, Context ctx, Collector<Long> out) throws Exception {
      out.collect(value);
    }

    @Override
    public void processElement2(Long value, Context ctx, Collector<Long> out) throws Exception {
      out.collect(value);
    }
  }

  private static class CollectSink implements SinkFunction<Long> {
    public static Set<Long> watermarks = new HashSet<Long>();
    @Override
    public void invoke(Long value, Context context) throws Exception {
      long wm = context.currentWatermark();
      System.out.println("Sink wm: " + wm + "; item: " + value);
    }
  }

}

