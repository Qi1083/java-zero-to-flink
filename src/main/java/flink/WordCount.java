package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.file.src.FileSource;

import java.time.Duration;
import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // 构建环境
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据
        // DataStreamSource<String> lineWords = env.readTextFile("input/word.txt");

        // 优化
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 必须开启 Checkpoint，否则无法断点续传 & 精确一次语义
        env.enableCheckpointing(5000);

        // 1. 构建文件源

        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input/"))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();

        // 2. 转为 DataStream
        DataStream<String> lineWords = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "FileSource");

        // 数据拆分
        //SingleOutputStreamOperator<Tuple2<String, Integer>> tupleWords = lineWords.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
        //    @Override
        //    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        //        String[] words = s.split(" ");
        //        for (String word : words) {
        //            Tuple2<String, Integer> result = Tuple2.of(word, 1);
        //            collector.collect(result);
        //        }
        //
        //    }
        //});

        // 优化
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleWords = lineWords.flatMap(
                (String line, Collector<Tuple2<String, Integer>> out) -> {
                    if (line == null || line.trim().isEmpty()) {  // 跳过空行
                        return;
                    }

                    String[] words = line.trim().split("\\s+"); // 兼容多空格、制表符
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }

                }

        ).returns(Types.TUPLE(Types.STRING, Types.INT));  // 避免类型擦除报错


        // 分组
        // KeyedStream<Tuple2<String, Integer>, String> key = tupleWords.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
        //    @Override
        //    public String getKey(Tuple2<String, Integer> value) throws Exception {
        //        return value.f0;
        //    }
        //});

        // 聚合
        // key.sum(1).print();

        // 优化 分组、聚合
        tupleWords.keyBy(0).sum(1).print();

        // 执行
        env.execute();

    }
}
