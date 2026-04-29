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
            long ts;

            if (num <= 74) {
                state = status[0];
                ts = currTimes;
            } else if (num <= 94) {
                state = rand.nextBoolean() ? status[1] : status[2];
                ts = currTimes;
            } else {
                ts = currTimes - 3000L;
            }

            ctx.collect(new DataEventEntity(
                    "SRC" + code,
                    state,
                    ts,
                    channels[rand.nextInt(3)]
            ));

            currTimes += rand.nextInt(5);
            Thread.sleep(10);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
