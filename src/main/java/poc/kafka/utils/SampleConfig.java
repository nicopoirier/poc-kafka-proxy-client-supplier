package poc.kafka.utils;

import lombok.Builder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("kafka")
public class SampleConfig {
    @Builder.Default
    private String boostrapServersConfig = "localhost:9092";
}
