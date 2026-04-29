package flink;

import entity.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

// WaterSensorMapFunction.java
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        // 格式: sensor1,1713123456,100 (id,ts,vc)
        String[] parts = value.trim().split(",");

        if (parts.length != 3) {
            System.err.println("Invalid format: " + value);
            return null;
        }

        try {
            return new WaterSensor(
                    parts[0].trim(),
                    Long.parseLong(parts[1].trim()),
                    Integer.parseInt(parts[2].trim())

            );
        } catch (NumberFormatException e) {
            System.err.println("Parse error: " + value);
            return null;
        }
    }
}