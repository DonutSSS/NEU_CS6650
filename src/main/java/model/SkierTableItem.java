package model;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.List;

@Data
@Builder
public class SkierTableItem {
    @NonNull private final String skierID;
    @NonNull private final String dayAndResort;
    @NonNull private List<Integer> times;
    @NonNull private List<Integer> lifts;
    private int totalVertical;
    private long lastUpdateTime;
}
