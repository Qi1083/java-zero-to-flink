package entity;

public class DataEventEntity {
    private String souceId;
    private String status;
    private Long timestamp;
    private String channel;

    public DataEventEntity() {
    }

    public DataEventEntity(String souceId, String status, Long timestamp, String channel) {
        this.souceId = souceId;
        this.status = status;
        this.timestamp = timestamp;
        this.channel = channel;
    }

    public String getSouceId() {
        return souceId;
    }

    private void setSouceId(String souceId) {
        this.souceId = souceId;
    }

    public String getStatus() {
        return status;
    }

    private void setStatus(String status) {
        this.status = status;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    private void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getChannel() {
        return channel;
    }

    private void setChannel(String channel) {
        this.channel = channel;
    }

    @Override
    public String toString() {
        return "DataEventEntity{" +
                "souceId='" + souceId + '\'' +
                ", status='" + status + '\'' +
                ", timestamp=" + timestamp +
                ", channel='" + channel + '\'' +
                '}';
    }
}
