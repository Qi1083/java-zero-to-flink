package entity;

// 测试用例事件
public class TestCaseEvent {
    public String caseId;
    public String status;  // PASS/FAIL/ERROR
    public long timestamp;
    public String module;

    public TestCaseEvent() {
    }

    public TestCaseEvent(String caseId, String status, long timestamp, String module) {
        this.caseId = caseId;
        this.status = status;
        this.timestamp = timestamp;
        this.module = module;
    }

    public String getCaseId() {
        return caseId;
    }

    public void setCaseId(String caseId) {
        this.caseId = caseId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    @Override
    public String toString() {
        return "TestCaseEvent{" + caseId + ", " + status + ", " + module + "}";
    }
}