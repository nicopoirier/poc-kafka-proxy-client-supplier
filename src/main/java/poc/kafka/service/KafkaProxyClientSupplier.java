package poc.kafka.service;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

import java.util.Map;

public class KafkaProxyClientSupplier implements KafkaClientSupplier {

    private static final String CLIENT_ID = "client.id";
    public static final String HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID = "HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID";
    public static final String HEADER_TOPIC_IN = "HEADER_TOPIC_IN";
    private final String streamName;

    public KafkaProxyClientSupplier(String streamName) {
        this.streamName = streamName;
    }

    private Map<String, Object> rewriteMapConfig(Map<String, Object> mapConfig) {
        if (mapConfig.get(CLIENT_ID) != null) {
            String clientId = (String) mapConfig.get(CLIENT_ID);
            mapConfig.put(CLIENT_ID, clientId + streamName);
        } else {
            mapConfig.put(CLIENT_ID, streamName);
        }
        return mapConfig;
    }

    @Override
    public AdminClient getAdminClient(Map<String, Object> mapConfig) {
        return AdminClient.create(rewriteMapConfig(mapConfig));
    }

    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> mapConfig) {
        mapConfig.put("key.serializer", ByteArraySerializer.class);
        mapConfig.put("value.serializer", ByteArraySerializer.class);
        Producer<byte[], byte[]> producer = new KafkaProducer<>(rewriteMapConfig(mapConfig));
        return new ProducerProxy<>(producer, streamName);
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> mapConfig) {
        mapConfig.put("key.deserializer", ByteArrayDeserializer.class);
        mapConfig.put("value.deserializer", ByteArrayDeserializer.class);
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(rewriteMapConfig(mapConfig));
        return new ConsumerProxy<>(consumer, streamName);
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> mapConfig) {
        return getConsumer(rewriteMapConfig(mapConfig));
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> mapConfig) {
        return getConsumer(rewriteMapConfig(mapConfig));
    }
}
