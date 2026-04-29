package flink;

import entity.AlertEvent;
import entity.TestCaseEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class ConsecutiveFailAlertFunction extends KeyedProcessFunction<String, TestCaseEvent, TestCaseEvent> {

    public static final OutputTag<AlertEvent> ALERT_TAG = new OutputTag<>("alart_fail_tag") {
    };

    private transient ValueState<Integer> valueCountState;
    private transient ValueState<Long> processTime;

    private static final int thread = 3;
    private static final long interTime = 3000L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 待补充完整
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Integer> valueDescriptor = new ValueStateDescriptor<>("FailCount", Types.INT);
        valueDescriptor.enableTimeToLive(ttlConfig);
        valueCountState = getRuntimeContext().getState(valueDescriptor);

        ValueStateDescriptor<Long> processDescriptor = new ValueStateDescriptor<>("processTime", Types.LONG);
        processDescriptor.enableTimeToLive(ttlConfig);
        processTime = getRuntimeContext().getState(processDescriptor);
    }

    @Override
    public void processElement(TestCaseEvent event, KeyedProcessFunction<String, TestCaseEvent, TestCaseEvent>.Context ctx, Collector<TestCaseEvent> out) throws Exception {

        long now = ctx.timerService().currentProcessingTime();
        Integer countObj = valueCountState.value();
        int count = countObj == null ? 0 : countObj;
        Long processTimeObj = processTime.value();
        long pT = processTimeObj == null ? 0L : processTimeObj;

        if ("Fail".equalsIgnoreCase(event.getStatus())) {
            count++;
            valueCountState.update(count);

            if ((count >= thread) && (now - pT > interTime)) {

                ctx.output(ALERT_TAG, new AlertEvent(event.caseId, event.module, count, now));
                processTime.update(now);
            }

        } else {
            valueCountState.update(0);
        }
        out.collect(event);

    }
}