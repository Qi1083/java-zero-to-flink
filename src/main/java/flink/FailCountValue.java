package flink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FailCountValue extends KeyedProcessFunction<String, TestCaseEvent, TestCaseEvent> {


    OutputTag<AlertEvent> ALART_TAG = new OutputTag<>("alart_tag") {
    };

    private transient ValueState<Integer> valueState;
    private transient ValueState<Long> eventTimeState;

    private static final Long Thread = 3000L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();

        ValueStateDescriptor<Integer> valueDescriptor = new ValueStateDescriptor<>("ValueState", Types.INT);
        valueDescriptor.enableTimeToLive(ttlConfig);

        ValueStateDescriptor<Long> eventTimeDescriptor = new ValueStateDescriptor<>("eventTimeState", Types.LONG);
        eventTimeDescriptor.enableTimeToLive(ttlConfig);

        valueState = getRuntimeContext().getState(valueDescriptor);
        eventTimeState = getRuntimeContext().getState(eventTimeDescriptor);
    }

    @Override
    public void processElement(TestCaseEvent event, KeyedProcessFunction<String, TestCaseEvent, TestCaseEvent>.Context ctx, Collector<TestCaseEvent> out) throws Exception {
        Integer countObj = valueState.value();
        int count = countObj == null ? 0 : countObj;

        Long timeObj = eventTimeState.value();
        long time = timeObj == null ? 0L : timeObj;
        long now = event.getTimestamp();

        if ("FAIL".equalsIgnoreCase(event.getStatus())) {

            count++;
            valueState.update(count);

            if ((count >= 3) && (now - time >= Thread)) {

                ctx.output(ALART_TAG, new AlertEvent(
                        event.getCaseId(),
                        event.getModule(),
                        count,
                        now
                ));

                eventTimeState.update(now);

            }

        } else {
            out.collect(event);
        }
    }

}
