package poc.kafka.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class GenericDeserializer<T> implements Deserializer<T> {

    private final Class<T> clazz;

    public GenericDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return JSONUtils.getInstance().parse(bytes, clazz);
        } catch (Exception e) {
            log.error("Deserialization failed for topic {}", topic);
            throw new SerializationException(e);
        }
    }

    public void close() {
        // nothing to close

    }
}

