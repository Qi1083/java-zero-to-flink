package flink;

// 测试用例事件
public class TestCaseEvent {
    public String caseId;
    public String status;  // PASS/FAIL/ERROR
    public long timestamp;
    public String module;

    public TestCaseEvent(String caseId, String status, long timestamp, String module) {
        this.caseId = caseId;
        this.status = status;
        this.timestamp = timestamp;
        this.module = module;
    }

    @Override
    public String toString() {
        return "TestCaseEvent{" + caseId + ", " + status + ", " + module + "}";
    }
}