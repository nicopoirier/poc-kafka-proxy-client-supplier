package poc.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

public class KafkaUtils {
    public static Properties createKStreamProperties(String nameProcess, String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "application-process" + nameProcess);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
        props.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 5000);
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG, "all");
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG, 25000);
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.LINGER_MS_CONFIG, 50);
        return props;
    }


}