package flink;

import entity.AlertEvent;
import entity.DataEventEntity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import source.DataEventSource;
import utils.TimeUtil;

import java.time.Duration;

public class DataQualityMonitor {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        // 设置并行度
        env.setParallelism(4);

        // 状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 开启检查点
        env.enableCheckpointing(10000L);
        // 两个检查点最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
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

        SingleOutputStreamOperator<DataEventEntity> eventSource = env.addSource(new DataEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DataEventEntity>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));

        OutputTag<DataEventEntity> LATE_DATA_TAG = new OutputTag<>("late_data") {
        };

        SingleOutputStreamOperator<String> windowEvent = eventSource.keyBy(DataEventEntity::getChannel)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(LATE_DATA_TAG)
                .aggregate(
                        new AggregateFunction<DataEventEntity, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return new Tuple2(0, 0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(DataEventEntity event, Tuple2<Integer, Integer> accumulator) {

                                if ("PASS".equalsIgnoreCase(event.getStatus())) {
                                    accumulator.f0++;
                                }

                                accumulator.f1++;

                                return accumulator;
                            }

                            @Override
                            public Tuple2<Integer, Integer> getResult(Tuple2<Integer, Integer> accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                return new Tuple2(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        },

                        new WindowFunction<Tuple2<Integer, Integer>, String, String, TimeWindow>() {
                            @Override
                            public void apply(String channel, TimeWindow window, Iterable<Tuple2<Integer, Integer>> input, Collector<String> out) throws Exception {
                                Tuple2<Integer, Integer> r = input.iterator().next();

                                int pass = r.f0;
                                int total = r.f1;

                                String windowStart = TimeUtil.tsToDate(window.getStart());
                                String windowEnd = TimeUtil.tsToDate(window.getEnd());

                                double rate = total == 0 ? 0 : (double) pass / total * 100;

                                out.collect("通道[" + channel + "]" + " | 窗口：[" + windowStart + ", " + windowEnd + "] | " + "成功率：" + rate + " (" + pass + "/" + total + ")");
                            }
                        }

                );


        windowEvent.print("通道成功率");

        windowEvent.getSideOutput(LATE_DATA_TAG).print("延迟数据");


        eventSource.keyBy(DataEventEntity::getSouceId)
                .process(new ConsecutiveFailAlertFunction())
                .print("连续异常告警");

        env.execute("DataQualityMonitor");
    }

    public static class ConsecutiveFailAlertFunction extends KeyedProcessFunction<String, DataEventEntity, DataEventEntity> {

        OutputTag<DataEventEntity> ALART_FIAL_TAG = new OutputTag<>("ALART_FIAL_TAG") {
        };

        private transient ValueState<Integer> failValueState;
        private transient ValueState<Long> timeState;
        private static final Long Thread = 3000L;
        private static volatile long ts = System.currentTimeMillis();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(24))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Integer> valueDescriptor = new ValueStateDescriptor<>("failValueState", Types.INT);
            valueDescriptor.enableTimeToLive(ttlConfig);
            failValueState = getRuntimeContext().getState(valueDescriptor);

            ValueStateDescriptor<Long> timeDescriptor = new ValueStateDescriptor<>("timeState", Types.LONG);
            timeDescriptor.enableTimeToLive(ttlConfig);
            timeState = getRuntimeContext().getState(timeDescriptor);
        }

        @Override
        public void processElement(DataEventEntity event, KeyedProcessFunction<String, DataEventEntity, DataEventEntity>.Context ctx, Collector<DataEventEntity> out) throws Exception {
            Integer countObj = failValueState.value();
            int count = countObj == null ? 0 : countObj;

            Long timeObj = timeState.value();
            long time = timeObj == null ? 0L : timeObj;

            long now = ctx.timerService().currentProcessingTime();

            if ("FAIL".equalsIgnoreCase(event.getStatus())) {

                count++;
                failValueState.update(count);

                if ((count >= 3) && (now - time > Thread)) {
                    ctx.output(ALART_FIAL_TAG, new DataEventEntity(
                            event.getSouceId(),
                            event.getStatus(),
                            now,
                            event.getChannel()
                    ));

                }

            } else {
                failValueState.update(0);
            }

        }
    }

}
