package flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class ValueSateTrain extends RichFlatMapFunction<String, String> {

    private ValueState<Integer> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("Count", Types.INT));
    }

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {

        Integer currentState = valueState.value();

        if (currentState == null) {
            currentState = 0;
        }

        currentState++;

        valueState.update(currentState);

        out.collect(value + ":" + currentState);

    }
}