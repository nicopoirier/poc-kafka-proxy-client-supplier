package poc.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import poc.kafka.domain.Car;

import java.util.Map;
@Slf4j
public class CarSerializer implements Serializer<Car> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Car car) {
        byte[] retVal = null;
        try {
            retVal = JSONUtils.getInstance().asJsonString(car).getBytes();
        } catch (JsonProcessingException e) {
            log.error("Can't serialize source " + car, e);
        }
        return retVal;
    }

    @Override
    public void close() {
        // nothing to close
    }
}