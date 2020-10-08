package model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RequestResponseStat {
    private final long startTime;
    private final long latency;
    private final String requestType;
    private final int responseCode;
}
