package poc.kafka.domain;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Car {

    private String name;
    private String generation;
    private String model;
    @Builder.Default
    private CarTechnical carTechnical = new CarTechnical();
    private Integer priceMax;
    private Integer priceMin;
    private Integer price;
    private String year;
    @Builder.Default
    private boolean buy = false;
}
