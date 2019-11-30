package poc.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;
import poc.kafka.domain.Car;
import poc.kafka.utils.CarSerializer;
import poc.kafka.utils.GenericDeserializer;
import poc.kafka.utils.KafkaUtils;

import java.util.concurrent.TimeUnit;

import static poc.kafka.utils.KafkaUtils.createKStreamProperties;

@Component
@Slf4j
public class SampleKsStream {
    public final static String TOPIC_INPUT_NO_EOS = "test-input-no-eos";
    public final static String TOPIC_OUTPUT_NO_EOS = "test-output-no-eos";


    public static Serde<Car> carSerde() {
        return Serdes.serdeFrom(new CarSerializer(), new GenericDeserializer<>(Car.class));
    }

    public void createKStreamWithKafkaProxyClientSupplier() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Car> streamInput = builder.stream(TOPIC_INPUT_NO_EOS, Consumed.with(Serdes.String(), carSerde()));

        KStream<String, Car> streamParsed = streamInput.filter((key,value)-> {
            if(value.getPrice()>value.getPriceMax()){
                log.error("Drop  price {} max Price",value.getPrice(),value.getPriceMax());
                return false;
            }
            return true;
        }).mapValues((value) -> {
            Integer average = value.getPriceMin()+(value.getPriceMax() - value.getPriceMin())/2;
            if(average>value.getPrice()){
                value.setBuy(true);
                log.info("Buy this car {}", value.toString());
            }else{
                value.setBuy(false);
            }
            return value;
        });

        final Serde<String> stringSerdes = Serdes.String();

        streamParsed.to(TOPIC_OUTPUT_NO_EOS, Produced.with(stringSerdes, carSerde()));
        String streamName = "createKstreamCarithKafkaProxyClientSupplier";
        KafkaProxyClientSupplier kafkaProxyClientSupplier = new KafkaProxyClientSupplier(streamName);
        KafkaStreams streams = new KafkaStreams(builder.build(), createKStreamProperties(streamName, "kafka-1:9092,kafka-2:9092,kafka-3:9092"),kafkaProxyClientSupplier);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    public void createKStream() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Car> streamInput = builder.stream(TOPIC_INPUT_NO_EOS, Consumed.with(Serdes.String(), carSerde()));

        KStream<String, Car> streamParsed = streamInput.filter((key,value)-> {
            if(value.getPrice()>value.getPriceMax()){
                log.error("Drop  price {} max Price",value.getPrice(),value.getPriceMax());
                return false;
            }
            return true;
        }).mapValues((value) -> {
            Integer average = value.getPriceMin()+(value.getPriceMax() - value.getPriceMin())/2;
            if(average>value.getPrice()){
                value.setBuy(true);
                log.info("Buy this car {}", value.toString());
            }else{
                value.setBuy(false);
            }
            return value;
        });

        final Serde<String> stringSerdes = Serdes.String();

        streamParsed.to(TOPIC_OUTPUT_NO_EOS, Produced.with(stringSerdes, carSerde()));
        String streamName = "createKstreamCar";
        KafkaStreams streams = new KafkaStreams(builder.build(), createKStreamProperties(streamName, "kafka-1:9092,kafka-2:9092,kafka-3:9092"));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }


}
