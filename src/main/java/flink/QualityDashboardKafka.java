package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

import java.time.Duration;


public class QualityDashboardKafka {
    private static final ObjectMapper mapper = new ObjectMapper();

    public QualityDashboardKafka() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 1. 开启 Checkpoint（Exactly-Once 语义必需）
        env.enableCheckpointing(3000L); // 30秒一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // Kafka配置
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test-cases")
                .setGroupId("QualityDashboard")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // 读取Kafka数据
        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<TestCaseEvent> events = kafkaStream.map(json -> {
            try {
                return mapper.readValue(json, TestCaseEvent.class);
            } catch (Exception e) {
                System.err.println("解析失败，跳过脏数据: " + json + " | 原因: " + e.getMessage());
                return null; // 或返回默认对象
            }
        }).returns(TestCaseEvent.class).filter(e -> e != null)
                .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TestCaseEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))  // 允许 3s 乱序
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()) // 从事件提取时间
        );

        // 统计10秒内的通过率
//        events.keyBy(TestCaseEvent::getModule)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .process(new ProcessWindowFunction<TestCaseEvent, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String module, ProcessWindowFunction<TestCaseEvent, String, String, TimeWindow>.Context context, Iterable<TestCaseEvent> elements, Collector<String> out) throws Exception {
//                        int suc = 0;
//                        int total = 0;
//
//                        for (TestCaseEvent e : elements) {
//                            if ("PASS".equalsIgnoreCase(e.getStatus())) {
//                                suc++;
//                            }
//                            total++;
//                        }
//
//                        if (suc == 0) {
//                            return;
//                        }
//
//                        double rate = (double) suc / total * 100;
//
//                        out.collect(String.format("模块: %s | 总数: %d | 通过: %d | 通过率: %.2f%%",
//                                module, total, suc, rate));
//                    }
//                }).returns(Types.STRING).print("pass方式");

//        events.keyBy(TestCaseEvent::getModule)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .aggregate(new PassRateAggregate())
//                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
//                .map(r -> String.format("PASS: %s | TOTAL: %d | RATE: %.2f%%",
//                        r.f0, r.f1, r.f2 * 100))
//                .print("agg方式");

        events.keyBy(TestCaseEvent::getModule)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new PassRateAggregate(),new MyWindowFunction())
                .print("agg方式");

        // 连续失败告警
        events.keyBy(TestCaseEvent::getModule)
                .process(new ConsecutiveFailAlertFunction())
                .getSideOutput(ConsecutiveFailAlertFunction.ALERT_TAG)
                .print("连续失败告警");

        env.execute("QualityDashboardKafka");

    }

  public static class PassRateAggregate implements AggregateFunction<TestCaseEvent, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> {

        @Override
        public Tuple3<String, Integer, Integer> createAccumulator() {
            return Tuple3.of("", 0, 0);
        }

        @Override
        public Tuple3<String, Integer, Integer> add(TestCaseEvent value, Tuple3<String, Integer, Integer> accumulator) {
            if ("PASS".equalsIgnoreCase(value.getStatus())) {
                accumulator.f1++;
            }

            accumulator.f0 = value.getModule();
            accumulator.f2++;
            return accumulator;
        }

        @Override
        public Tuple3<String, Integer, Integer> getResult(Tuple3<String, Integer, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }

    public static class MyWindowFunction implements WindowFunction<Tuple3<String,Integer, Integer>, String, String, TimeWindow> {
        @Override
        public void apply(String module, TimeWindow window, Iterable<Tuple3<String,Integer, Integer>> input, Collector<String> out) {
            Tuple3<String,Integer, Integer> r = input.iterator().next();
            int pass = r.f1;
            int total = r.f2;
            double rate = total == 0 ? 0 : (double) pass / total * 100;
            out.collect("模块：" + module + " | 通过：" + pass + " | 总数：" + total + " | 通过率：" + String.format("%.2f", rate) + "%");
        }
    }

    public static class ConsecutiveFailAlertFunction extends KeyedProcessFunction<String, TestCaseEvent, TestCaseEvent> {

        public static final OutputTag<AlertEvent> ALERT_TAG = new OutputTag<>("consecutive-fail-alert") {};

        // 业务可配置参数
        private static final int THRESHOLD = 3;               // 连续失败阈值
        private static final long COOLDOWN_MS = 300_000L;     // 告警冷却时间（5分钟防刷屏）

        private ValueState<Integer> failCountState;
        private ValueState<Long> lastAlertTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttl = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(24))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("fail_count", Types.INT);
            countDesc.enableTimeToLive(ttl);
            failCountState = getRuntimeContext().getState(countDesc);

            ValueStateDescriptor<Long> timeDesc = new ValueStateDescriptor<>("last_alert_time", Types.LONG);
            timeDesc.enableTimeToLive(ttl);
            lastAlertTimeState = getRuntimeContext().getState(timeDesc);
        }

        @Override
        public void processElement(TestCaseEvent event, Context ctx, Collector<TestCaseEvent> out) throws Exception {
            Integer count = failCountState.value() == null ? 0 : failCountState.value();
            Long lastAlert = lastAlertTimeState.value() == null ? 0L : lastAlertTimeState.value();
            long now = ctx.timerService().currentProcessingTime(); // 统一使用处理时间

            String status = event.getStatus() == null ? "" : event.getStatus();

            if ("FAIL".equalsIgnoreCase(status)) {
                count++;
                failCountState.update(count);

                // 达到阈值 & 不在冷却期内 → 触发告警
                if (count >= THRESHOLD && (now - lastAlert > COOLDOWN_MS)) {
                    ctx.output(ALERT_TAG, new AlertEvent(
                            event.getCaseId(), event.getModule(), count, now));
                    lastAlertTimeState.update(now); // 更新冷却时间
                    // failCountState.update(0); // 可选：告警后重置计数，或保留继续累加
                }
            } else {
                count = 0;
                // PASS / SKIP / 其他状态 → 立即重置连续失败计数
                failCountState.update(0);
            }

            out.collect(event); // 主数据流继续向下游传递
        }
    }
}

