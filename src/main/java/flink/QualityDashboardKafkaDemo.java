package flink;

import entity.TestCaseEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class QualityDashboardKafkaDemo {

    public static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 开启状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 开启检查点
        env.enableCheckpointing(5000L);
        // 设置两个检查点最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        // 设置检查点模式
        env.getCheckpointConfig().setCheckpointingMode(
                CheckpointingMode.EXACTLY_ONCE
        );
        // 设置检查点超时时间
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(15));
        // 设置检查点任务取消保留
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

        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<TestCaseEvent> events = kafkaStream.map(new MapFunction<String, TestCaseEvent>() {
                    @Override
                    public TestCaseEvent map(String value) throws Exception {
                        try {
                            return mapper.readValue(value, TestCaseEvent.class);
                        } catch (Exception e) {
                            return null;
                        }

                    }
                }).returns(TestCaseEvent.class)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TestCaseEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));


        // 统计通过率
        events.keyBy(TestCaseEvent::getModule)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<TestCaseEvent, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                               @Override
                               public Tuple2<Integer, Integer> createAccumulator() {
                                   return Tuple2.of(0, 0);
                               }

                               @Override
                               public Tuple2<Integer, Integer> add(TestCaseEvent event, Tuple2<Integer, Integer> accumulator) {

                                   accumulator.f1++;

                                   if ("PASS".equalsIgnoreCase(event.getStatus())) {
                                       accumulator.f0++;
                                   }

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
                            public void apply(String module, TimeWindow window, Iterable<Tuple2<Integer, Integer>> input, Collector<String> out) throws Exception {
                                Tuple2<Integer, Integer> r = input.iterator().next();
                                int pass = r.f0;
                                int total = r.f1;

                                double rate = total == 0 ? 0 : (double) pass / total * 100;

                                out.collect("模块：" + module + " | 通过：" + pass + " | 总数：" + total + " | 通过率：" + String.format("%.2f", rate) + "%");

                            }
                        })
                .print("agg方式");

        // 连续失败告警
        events.keyBy(TestCaseEvent::getModule).process(
                        new ConsecutiveFailAlertFunction()
                ).getSideOutput(ConsecutiveFailAlertFunction.ALERT_TAG)
                .print("连续失败告警");

        env.execute("QualityDashboardKafkaDemoTrain");

    }
}

