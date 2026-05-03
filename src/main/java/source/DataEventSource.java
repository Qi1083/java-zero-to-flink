package source;

import entity.DataEventEntity;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class DataEventSource implements SourceFunction<DataEventEntity> {
    private volatile boolean isRunning = true;
    private final Random rand = new Random(32);
    long currTimes = System.currentTimeMillis();
    String[] status = {"SUCCESS", "FAIL", "TIMEOUT"};
    String[] channels = {"SMS", "PUSH", "EMAIL"};

    @Override
    public void run(SourceContext<DataEventEntity> ctx) throws Exception {

        while (isRunning) {

            int num = rand.nextInt(100);  // 0 ~ 999
            String code = String.format("%03d", num);

            String state = "";
            String channel = "";
            long ts;

            if (num <= 74) {
                state = status[0];
                ts = currTimes;
                channel = channels[1];
            } else if (num <= 94) {
                state = rand.nextBoolean() ? status[1] : status[2];
                channel = rand.nextBoolean() ? channels[0] : channels[2];
                ts = currTimes;
            } else {
                state = rand.nextBoolean() ? status[1] : status[2];
                channel = rand.nextBoolean() ? channels[0] : channels[2];
                ts = currTimes - 15000L;
            }

            ctx.collect(new DataEventEntity(
                    "SRC" + code,
                    state,
                    ts,
                    channel
            ));

            currTimes += rand.nextInt(5000);
            Thread.sleep(10);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
