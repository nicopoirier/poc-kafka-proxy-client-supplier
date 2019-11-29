package poc.kafka.domain;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class CarTechnical {
    private String powerHP;
    private String coupleEngine;
    private String cylinder;
    private Integer nbOfCylinder;
}
