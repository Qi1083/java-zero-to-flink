package flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlertEvent {
    private String caseId;
    private String module;
    private int consecutiveFails;
    private long alertTime;
}
