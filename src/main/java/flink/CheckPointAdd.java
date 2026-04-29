package flink;

import entity.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.Random;

public class CheckPointAdd {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String restorePath = "file:///D:/flink-local-cp/ac58aa457201911fb3b549e9229c34c3/chk-35";
        if (restorePath != null) {
            conf.setString("execution.savepoint.path", restorePath);
            conf.setString("execution.savepoint.ignore-unclaimed-state", "true"); // 允许跳过 JobID 不一致的状态
        }

        // 使用该配置创建本地环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);

        // 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 指定新 Checkpoint 的写入基目录
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/flink-local-cp");

        // 1. 开启检查点
        env.enableCheckpointing(3000);
        // 2. 设置 Checkpoint 模式（精确一次 / 至少一次）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 3. 设置两次 Checkpoint 之间的最小间隔（防止 Checkpoint 过于密集）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // 4. 设置 Checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(15000);
        // 6. 设置可容忍的连续 Checkpoint 失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // 7. 设置外部化 Checkpoint（作业取消后是否保留）
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 设置水位线
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.addSource(new ContinuousSensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        // 3. KeyBy + 窗口 + 绑定窗口计算函数
        sensorDS.keyBy(WaterSensor::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    private transient ValueState<Long> totalCount;

                    @Override
                    public void open(Configuration parameters) {
                        totalCount = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("total_count", Types.LONG)
                        );
                    }

                    @Override
                    public void process(String key, Context context,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        // 累计统计
                        long current = totalCount.value() == null ? 0L : totalCount.value();
                        for (WaterSensor e : elements) {
                            current++;
                        }
                        totalCount.update(current);

                        // 窗口内求最大
                        WaterSensor max = null;
                        for (WaterSensor e : elements) {
                            if (max == null || e.getVc() > max.getVc()) {
                                max = e;
                            }
                        }

                        out.collect("累计: " + current + ", 窗口最大: " + max);
                    }
                })
                .print("窗口结果");
        // 4. 触发执行
        env.execute("SensorSlidingWindowCheckpointDemo");


    }

    // 内置连续源（同上，保持事件时间推进）
    public static class ContinuousSensorSource implements SourceFunction<WaterSensor> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            long baseTs = System.currentTimeMillis() / 1000;
            long currentTs = baseTs;
            Random rand = new Random();
            while (isRunning) {
                ctx.collect(new WaterSensor("sensor_" + (rand.nextInt(2) + 1), currentTs, 20 + rand.nextInt(30)));
                currentTs += rand.nextInt(3) + 1;
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
