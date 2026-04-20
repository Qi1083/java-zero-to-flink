package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class SaltingExample {

    // 热点 key 集合（实际中可通过历史数据或实时监控识别）
    private static final Set<String> HOT_KEYS = new HashSet<>();
    static {
        HOT_KEYS.add("user_1001"); // 假设这是大V用户
        HOT_KEYS.add("user_1002");
    }

    // 加盐数量（将热点分散到10个桶）
    private static final int SALT_COUNT = 10;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 模拟数据源：用户点击流 (userId, clickTime, count)
        DataStream<ClickEvent> source = env.fromElements(
                new ClickEvent("user_1001", System.currentTimeMillis(), 1), // 热点
                new ClickEvent("user_1001", System.currentTimeMillis(), 1),
                new ClickEvent("user_1001", System.currentTimeMillis(), 1),
                new ClickEvent("user_1002", System.currentTimeMillis(), 1),
                new ClickEvent("user_2001", System.currentTimeMillis(), 1), // 普通用户
                new ClickEvent("user_2002", System.currentTimeMillis(), 1)
        );

        // ========== 加盐预处理阶段 ==========
        DataStream<SaltedEvent> saltedStream = source.map(new MapFunction<ClickEvent, SaltedEvent>() {
            private Random random = new Random();

            @Override
            public SaltedEvent map(ClickEvent event) throws Exception {
                String userId = event.getUserId();
                int salt = 0;

                // 如果是热点 key，添加随机盐值
                if (HOT_KEYS.contains(userId)) {
                    salt = random.nextInt(SALT_COUNT); // 0-9
                }

                // 格式：原始key_盐值，如 user_1001_3
                String saltedKey = userId + "_" + salt;

                return new SaltedEvent(userId, saltedKey, event.getClickTime(), event.getCount());
            }
        });

        // ========== 第一阶段：局部聚合（按加盐后的key分组） ==========
        DataStream<Tuple3<String, String, Long>> partialAgg = saltedStream
                .keyBy(SaltedEvent::getSaltedKey)  // 按加盐key分组，分散热点
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(
                        new AggregateFunction<SaltedEvent, Long, Long>() {
                            @Override
                            public Long createAccumulator() { return 0L; }

                            @Override
                            public Long add(SaltedEvent event, Long acc) {
                                return acc + event.getCount();
                            }

                            @Override
                            public Long getResult(Long acc) { return acc; }

                            @Override
                            public Long merge(Long a, Long b) { return a + b; }
                        },
                        new ProcessWindowFunction<Long, Tuple3<String, String, Long>, String, TimeWindow>() {
                            @Override
                            public void process(String saltedKey, Context ctx, Iterable<Long> elements,
                                                Collector<Tuple3<String, String, Long>> out) {
                                Long count = elements.iterator().next();
                                // 提取原始 userId（去掉盐值后缀）
                                String originalKey = saltedKey.split("_")[0] + "_" + saltedKey.split("_")[1];
                                out.collect(Tuple3.of(originalKey, saltedKey, count));
                            }
                        }
                );

        // ========== 第二阶段：全局聚合（按原始key分组） ==========
        DataStream<Tuple2<String, Long>> finalResult = partialAgg
                .keyBy(t -> t.f0)  // 按原始 userId 分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new AggregateFunction<Tuple3<String, String, Long>, Long, Long>() {
                    @Override
                    public Long createAccumulator() { return 0L; }

                    @Override
                    public Long add(Tuple3<String, String, Long> value, Long acc) {
                        return acc + value.f2;
                    }

                    @Override
                    public Long getResult(Long acc) { return acc; }

                    @Override
                    public Long merge(Long a, Long b) { return a + b; }
                }, new ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String userId, Context ctx, Iterable<Long> elements,
                                        Collector<Tuple2<String, Long>> out) {
                        out.collect(Tuple2.of(userId, elements.iterator().next()));
                    }
                });

        // 输出结果
        finalResult.print("Final Result");

        env.execute("Salting Example");
    }

    // 数据类
    public static class ClickEvent {
        private String userId;
        private long clickTime;
        private int count;

        public ClickEvent(String userId, long clickTime, int count) {
            this.userId = userId;
            this.clickTime = clickTime;
            this.count = count;
        }
        // getters...
        public String getUserId() { return userId; }
        public long getClickTime() { return clickTime; }
        public int getCount() { return count; }
    }

    public static class SaltedEvent {
        private String originalKey;  // 原始key
        private String saltedKey;    // 加盐后的key
        private long clickTime;
        private int count;

        public SaltedEvent(String originalKey, String saltedKey, long clickTime, int count) {
            this.originalKey = originalKey;
            this.saltedKey = saltedKey;
            this.clickTime = clickTime;
            this.count = count;
        }
        // getters...
        public String getOriginalKey() { return originalKey; }
        public String getSaltedKey() { return saltedKey; }
        public long getClickTime() { return clickTime; }
        public int getCount() { return count; }
    }
}