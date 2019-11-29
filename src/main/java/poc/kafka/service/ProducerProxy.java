package poc.kafka.service;


import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.fasterxml.jackson.databind.jsonschema.JsonSerializableSchema.NO_VALUE;
import static poc.kafka.service.KafkaProxyClientSupplier.HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID;
import static poc.kafka.service.KafkaProxyClientSupplier.HEADER_TOPIC_IN;

public class ProducerProxy<K, V> implements Producer<K, V> {

    private final String streamName;
    private final Producer<K, V> producerReference;

    public ProducerProxy(Producer<K, V> producerReference, String streamName) {
        this.producerReference = producerReference;
        this.streamName = streamName;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        updateHeader(producerRecord);
        final Future<RecordMetadata> send = producerReference.send(producerRecord, null);
        return send;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
        updateHeader(producerRecord);
        final Future<RecordMetadata> send = producerReference.send(producerRecord, callback);
        return send;
    }

    private void updateHeader(ProducerRecord<K, V> record) {
        Optional<Header> header = Streams.stream(record.headers())
                .filter(itemHeader -> itemHeader.key().equals(HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID))
                .findFirst();
        if (header.isPresent()) {
            Long timestampConsumer = Long.parseLong(new String(header.get().value()));
            metricTopicToTopic(timestampConsumer, record, HEADER_TOPIC_IN);
        }
        //remove
        record.headers().remove(HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID);
        record.headers().remove(HEADER_TOPIC_IN);
    }

    private void metricTopicToTopic(Long timestampConsumer, ProducerRecord<K, V> record, String headerTopicIn) {
        String topicIn = extractHeader(record.headers(), headerTopicIn);
        if (topicIn != null && !topicIn.equals(NO_VALUE)) {
            timerMetricTopicToTopic(streamName, topicIn, record.topic(), System.currentTimeMillis() - timestampConsumer);
        }
    }

    private String extractHeader(Headers headers, String key) {
        if (key != null && headers != null) {

            String result = StreamSupport.stream(headers.spliterator(), false)
                    .filter(itemHeader -> itemHeader.key().equals(key) && itemHeader.value() != null)
                    .map(itemHeader -> new String(itemHeader.value()))
                    .collect(Collectors.joining(","));
            return result != null && !result.equals("") ? result : NO_VALUE;
        }
        return NO_VALUE;
    }


    private void timerMetricTopicToTopic(String streamName, String topicOriginal, String topicFinal, long duration) {
        Metrics.timer(topicOriginal + "-to-" + topicFinal,
                Lists.newArrayList(
                        Tag.of("TOPIC_ORIGINAL", topicOriginal),
                        Tag.of("TOPIC_FINAL", topicFinal),
                        Tag.of("FUNCTION", "topic-to-topic"),
                        Tag.of("STREAM_NAME", streamName),
                        Tag.of("SOCLE_EVENT_DRIVE", "true")
                )
        ).record(duration, TimeUnit.MILLISECONDS);
    }

    @Override
    public void initTransactions() {
        producerReference.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        producerReference.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s) throws ProducerFencedException {
        producerReference.sendOffsetsToTransaction(map, s);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        producerReference.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        producerReference.abortTransaction();
    }

    @Override
    public void flush() {
        producerReference.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return producerReference.partitionsFor(s);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producerReference.metrics();
    }

    @Override
    public void close() {
        producerReference.close();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        producerReference.close(l, timeUnit);
    }

    @Override
    public void close(Duration timeout) {
        producerReference.close(timeout);
    }
}
