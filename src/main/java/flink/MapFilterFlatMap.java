package flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapFilterFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // map
        DataStreamSource<String> lineWords = env.fromElements("hello world", "hell0 java", "hello flink");
        SingleOutputStreamOperator<String> mapWords = lineWords.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        }).returns(Types.STRING);
        // mapWords.print();

        // flatmap
        env.fromElements("hello world", "hell0 java", "hello flink");
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupeWords = lineWords.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (value == null || value.isEmpty()) {
                    return;
                }

                String[] words = value.trim().split("\\s+");

                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //tupeWords.print();

        // filter
        env.fromElements("hello world", "hell0 java", "hello flink");
        SingleOutputStreamOperator<String> filterWords = lineWords.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (value == null || value.isEmpty()) {
                    return false;
                }

                return value.trim().split("\\s+")[1].length()>=5;
            }
        });

        filterWords.print();

        env.execute();
    }
}
