package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCountStreamDemo {
    public static void main(String[] args) throws Exception {
        // TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 读取数据
        DataStreamSource<String> lineWords = env.readTextFile("input/word.txt");
        // TODO 切分 压平 分组 聚合
        // flatMap括号中是一个匿名内部类，实现了flatMap方法
        SingleOutputStreamOperator<Tuple2<String, Integer>> addWordOne = lineWords.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : s.split(" ")) {
                    Tuple2<String, Integer> addWordOne = Tuple2.of(word, 1);
                    out.collect(addWordOne);
                }
            }
        });

        // 这处也用到了匿名内部类 实现了getKey方法
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = addWordOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1); // 位置1

        sumDS.print();
        // TODO 启动
        env.execute("WorldCountStreamDemo");

    }
}
