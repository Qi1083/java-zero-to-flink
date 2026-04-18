package flink;

public class WaterSensor {
    private String id;      // 传感器ID
    private Long ts;        // 时间戳（秒）
    private Integer vc;     // 水位值


    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;

    }

    // Getters
    public String getId() {
        return id;
    }

    public Integer getVc() {
        return vc;
    }

    public Long getTs() {
        return ts;
    }

    @Override
    public String toString() {
        return String.format("WaterSensor{id='%s', ts=%d, vc=%d}", id, ts, vc);
    }
}