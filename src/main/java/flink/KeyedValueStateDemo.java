package flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Duration;

public class KeyedValueStateDemo {
    public static void main(String[] args) throws Exception {
        /*检测每种传感器水位值，水位值差超过10，则进行告警*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        // 设置水位线
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 8090).map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                        withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        sensorDS.keyBy(key -> key.getId()).process(new KeyedProcessFunction<String, WaterSensor, String>() {

            // 定义状态
            ValueState lastVcState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 在open方法中初始化状态
                // 状态描述器两个参数：第一个是名字，第二是存储的类型
                lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVcState", Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                // 取出上一个水位值
                Integer lastlVc = lastVcState.value() == null ? 0 : (Integer) lastVcState.value();

                // 求差值绝对值是否超过10
                Integer vc = value.getVc();
                if (Math.abs(vc - lastlVc) > 10) {
                    out.collect("传感器：" + value.getId() + "===>当前水位值：" + vc + ",与上一条水位值：" + lastlVc + ",相差超过10！！！");
                }
                lastVcState.update(vc);
            }
        }).print();


        env.execute();
    }
}



