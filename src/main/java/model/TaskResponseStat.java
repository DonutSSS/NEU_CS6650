package model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TaskResponseStat {
    private final long startTime;
    private final long endTime;
    private final long threadId;
    private final boolean encounteredError;
}
