package model;

public class ResponseStat {
    private final long requestStartTime;
    private final long responseEndTime;
    private final long threadId;
    private final boolean encounteredError;

    public ResponseStat(long requestStartTime,
                 long responseEndTime,
                 long threadID,
                 boolean encounteredError) {
        this.requestStartTime = requestStartTime;
        this.responseEndTime = responseEndTime;
        this.threadId = threadID;
        this.encounteredError = encounteredError;
    }

    public long getRequestStartTime() {
        return this.requestStartTime;
    }

    public long getResponseEndTime() {
        return this.responseEndTime;
    }

    public long getThreadId() {
        return this.threadId;
    }

    public boolean getErrorFlag() {
        return this.encounteredError;
    }
}
