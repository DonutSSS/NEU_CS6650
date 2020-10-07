package model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ResponseStat {
    private final long requestStartTime;
    private final long responseEndTime;
    private final long threadId;
    private final boolean encounteredError;
}
