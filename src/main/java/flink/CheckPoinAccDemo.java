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

public class CheckPoinAccDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String restorePath = "file:///D:/flink-local-cp/4a7f76c57c9cdc00b5dfa21d6ef83683/chk-39";

        // 从指定的检查点 / 保存点路径恢复状态
        if (restorePath != null) {
            conf.setString("execution.savepoint.path", restorePath);
            // conf.setString("execution.savepoint.ignore-unclaimed-state", "true"); // 允许跳过 JobID 不一致的状态
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度
        env.setParallelism(1);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 指定新 Checkpoint 的写入基目录
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/flink-local-cp");

        // 设置检查点
        env.enableCheckpointing(3000L);

        // 设置 Checkpoint 模式（精确一次 / 至少一次）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置两次 Checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);

        // 设置 Checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(15000L);

        // 设置可容忍的连续 Checkpoint 失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // 设置外部化checkpoint
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.addSource(new ContinuousSensorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
        );

        sensorDS.keyBy(WaterSensor::getId).window(TumblingEventTimeWindows.of(Time.seconds(5))).process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                    private transient ValueState<Long> totalCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 创建 StateTtlConfig
                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.hours(24))  // 根据业务需求设置
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .cleanupInRocksdbCompactFilter(1000)  // 如果使用 RocksDB
                                .build();

                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                                "totalCount", Types.LONG);
                        descriptor.enableTimeToLive(ttlConfig);  // 启用TTL

                        totalCount = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        // 获取当前统计量状态值
                        long currentState = totalCount.value() == null ? 0L : totalCount.value();

                        // 累积统计
                        for (WaterSensor e : elements) {
                            currentState++;
                        }

                        totalCount.update(currentState);


                        // 窗口内求最大值
                        WaterSensor max = null;

                        for (WaterSensor e : elements) {
                            max = max == null || e.getVc() >= max.getVc() ? e : max;
                        }

                        out.collect("累计: " + currentState + ", 窗口最大: " + max);

                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }

                }).print("窗口结果");

        env.execute("SensorSlidingWindowCheckpointDemo");

    }


}


class ContinuousSensorSource implements SourceFunction<WaterSensor> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        long currentTs = System.currentTimeMillis() / 1000;
        Random rand = new Random();

        while (isRunning) {
            ctx.collect(new WaterSensor("sensor_" + (rand.nextInt(3) + 1), currentTs, 20 + rand.nextInt(30)));
            currentTs += rand.nextInt(3) + 1;
            Thread.sleep(500);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
