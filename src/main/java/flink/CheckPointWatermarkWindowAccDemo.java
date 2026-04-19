package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;

public class CheckPointWatermarkWindowAccDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(3000L);
        // 两个检查点最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        // 失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(15000L);
        // 精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // checkpoin 指定保留位置
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/flink-checkpoints");
        // 作业取消时保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        SingleOutputStreamOperator<WaterSensor> addSensor = env.addSource(new ContinuousSensorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
        );

        addSensor.keyBy(WaterSensor::getId).window(TumblingEventTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

            private transient ValueState<Long> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        //.cleanupInRocksdbCompactFilter(1000) // HashMapStateBackend + cleanupInRocksdbCompactFilter 不兼容
                        .cleanupIncrementally(10, false)
                        .build();

                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("totalCount", Types.LONG);
                descriptor.enableTimeToLive(ttlConfig);

                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long currCount = valueState.value() == null ? 0L : valueState.value();
                WaterSensor max = null;

                for (WaterSensor e : elements) {
                    currCount++;

                    max = max == null || e.getVc() >= max.getVc() ? e : max;

                }

                valueState.update(currCount);

                out.collect("累计: " + currCount + ", 窗口最大: " + max);

            }

        }).print("窗口数据");

        env.execute();

    }

    public static class ContinuousSensorSource implements SourceFunction<WaterSensor> {

        private volatile boolean isRunning = true;
        long currTimes = System.currentTimeMillis() / 1000;
        Random rand = new Random();

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning) {
                ctx.collect(new WaterSensor("Qi_" + (rand.nextInt(3) + 1), currTimes, 20 + rand.nextInt(30)));
                currTimes += rand.nextInt(5);
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
