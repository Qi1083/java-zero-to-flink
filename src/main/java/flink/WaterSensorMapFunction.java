package flink;

import org.apache.flink.api.common.functions.MapFunction;

// WaterSensorMapFunction.java
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        // 格式: sensor1,100,1713123456 (id,vc,ts)
        String[] parts = value.trim().split(",");

        if (parts.length != 3) {
            System.err.println("Invalid format: " + value);
            return null;
        }

        try {
            return new WaterSensor(
                    parts[0].trim(),
                    Integer.parseInt(parts[1].trim()),
                    Long.parseLong(parts[2].trim())
            );
        } catch (NumberFormatException e) {
            System.err.println("Parse error: " + value);
            return null;
        }
    }
}