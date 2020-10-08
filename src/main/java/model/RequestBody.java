package model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RequestBody {
    private final String resortID;
    private final int dayID;
    private final int skierID;
    private final int time;
    private final int liftID;
}
