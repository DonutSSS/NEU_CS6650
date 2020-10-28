package model;

import lombok.*;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SkierPOSTRequest {
    private String resortID;
    private int dayID;
    private int skierID;
    private int time;
    private int liftID;
}
