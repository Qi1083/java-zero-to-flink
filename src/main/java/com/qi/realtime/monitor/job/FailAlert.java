package com.qi.realtime.monitor.job;

import com.qi.realtime.monitor.entity.AlertEvent;
import com.qi.realtime.monitor.entity.DataEventEntity;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FailAlert extends KeyedProcessFunction<String, com.qi.realtime.monitor.entity.DataEventEntity, com.qi.realtime.monitor.entity.DataEventEntity> {

    OutputTag<AlertEvent> ALERT_TAG = new OutputTag<>("fail_alert_tag") {
    };

    private transient ValueState<Integer> failValueState;
    private transient ValueState<Long> timeValueState;

    private static final Long THREAD = 3000L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Integer> valueDescriptor = new ValueStateDescriptor<>("failValueState", Types.INT);
        valueDescriptor.enableTimeToLive(ttlConfig);
        failValueState = getRuntimeContext().getState(valueDescriptor);

        ValueStateDescriptor<Long> timeDescriptor = new ValueStateDescriptor<>("timeValueState", Types.LONG);
        timeDescriptor.enableTimeToLive(ttlConfig);
        timeValueState = getRuntimeContext().getState(timeDescriptor);
    }

    @Override
    public void processElement(com.qi.realtime.monitor.entity.DataEventEntity event, KeyedProcessFunction<String, com.qi.realtime.monitor.entity.DataEventEntity, DataEventEntity>.Context ctx, Collector<com.qi.realtime.monitor.entity.DataEventEntity> out) throws Exception {
        Integer countObj = failValueState.value();
        int count = countObj == null ? 0 : countObj;

        Long timeObj = timeValueState.value();
        long time = timeObj == null ? 0L : timeObj;

        long now = ctx.timerService().currentProcessingTime();

        if ("FAIL".equalsIgnoreCase(event.getStatus())) {
            count++;
            failValueState.update(count);

            if ((count >= 3) && (now - time > THREAD)) {

                ctx.output(ALERT_TAG,new AlertEvent(
                        event.getSouceId(),
                        event.getChannel(),
                        count,
                        now
                ));

                timeValueState.update(now);
            }
        }else {
            failValueState.update(0);
        }

        out.collect(event);
    }
}
