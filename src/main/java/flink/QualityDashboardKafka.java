package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class QualityDashboardKafka {
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
            String[] parts = json.replaceAll("[{}\"]", "").split(",");
            String caseId = parts[0].split(":")[1];
            String status = parts[1].split(":")[1];
            String module = parts[3].split(":")[1];
            return new TestCaseEvent(caseId, status, System.currentTimeMillis(), module);
        });

        // 实时统计 状态计数
        events.map(e -> Tuple2.of(e.status, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print("实时统计");

        env.execute("QualityDashboardKafka");

    }
}
