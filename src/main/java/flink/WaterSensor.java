package flink;

public class WaterSensor {
    private String id;      // 传感器ID
    private Integer vc;     // 水位值
    private Long ts;        // 时间戳（秒）

    public WaterSensor() {
    }

    public WaterSensor(String id, Integer vc, Long ts) {
        this.id = id;
        this.vc = vc;
        this.ts = ts;
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
        return String.format("WaterSensor{id='%s', vc=%d, ts=%d}", id, vc, ts);
    }
}