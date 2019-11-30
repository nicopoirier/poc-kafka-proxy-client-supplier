package poc.kafka.service;

import kafka.common.KafkaException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import poc.kafka.domain.Car;
import poc.kafka.domain.CarTechnical;
import poc.kafka.utils.CarSerializer;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static poc.kafka.service.SampleKsStream.*;

@Slf4j
@Component
public class ProducerSample {

    private List<Car> listCar;

    @PostConstruct
    public void init() {
        listCar = new ArrayList<>();
        listCar.add(Car.builder().name("BMW M1 2012").model("M1").generation("E82").year("2012").priceMin(20000).priceMax(46000).carTechnical(CarTechnical.builder().coupleEngine("500").cylinder("2979").nbOfCylinder(6).powerHP("340").build()).build());
        listCar.add(Car.builder().name("BMW M3 1988").model("M3").generation("E30").year("1988").priceMin(10000).priceMax(90000).carTechnical(CarTechnical.builder().coupleEngine("240").cylinder("2302").nbOfCylinder(4).powerHP("200").build()).build());
        listCar.add(Car.builder().name("BMW M3 1998").model("M3").generation("E36").year("1998").priceMin(10000).priceMax(27000).carTechnical(CarTechnical.builder().coupleEngine("320").cylinder("2990").nbOfCylinder(6).powerHP("286").build()).build());
        listCar.add(Car.builder().name("BMW M3 2002").model("M3").generation("E46").year("2002").priceMin(30000).priceMax(35000).carTechnical(CarTechnical.builder().coupleEngine("365").cylinder("3246").nbOfCylinder(6).powerHP("343").build()).build());
        listCar.add(Car.builder().name("BMW M3 2010").model("M3").generation("E90").year("2010").priceMin(10000).priceMax(51000).carTechnical(CarTechnical.builder().coupleEngine("400").cylinder("3999").nbOfCylinder(8).powerHP("420").build()).build());
        listCar.add(Car.builder().name("BMW M3 2011").model("M3").generation("E92").year("2011").priceMin(15000).priceMax(52000).carTechnical(CarTechnical.builder().coupleEngine("400").cylinder("3999").nbOfCylinder(8).powerHP("420").build()).build());
        listCar.add(Car.builder().name("BMW M3 2014").model("M3").generation("F80").year("2014").priceMin(15000).priceMax(60000).carTechnical(CarTechnical.builder().coupleEngine("550").cylinder("2979").nbOfCylinder(6).powerHP("430").build()).build());
        listCar.add(Car.builder().name("BMW M5 2001").model("M5").generation("E39").year("2001").priceMin(20000).priceMax(45000).carTechnical(CarTechnical.builder().coupleEngine("500").cylinder("4941").nbOfCylinder(8).powerHP("400").build()).build());
        listCar.add(Car.builder().name("BMW M5 2008").model("M5").generation("E60").year("2008").priceMin(20000).priceMax(55000).carTechnical(CarTechnical.builder().coupleEngine("520").cylinder("4999").nbOfCylinder(10).powerHP("507").build()).build());
        listCar.add(Car.builder().name("BMW M5 2012").model("M5").generation("F10").year("2012").priceMin(20000).priceMax(70000).carTechnical(CarTechnical.builder().coupleEngine("680").cylinder("2979").nbOfCylinder(8).powerHP("560").build()).build());
    }

    public void procudeDataSampleWithOutTransaction(Integer argSize) {
        try (Producer<String, Car> producer = getWithoutTransaction()) {
            for (int i = 0; i < argSize; i++) {
                Car produce = getCar();
                ProducerRecord<String, Car> record = new ProducerRecord<>(TOPIC_INPUT_NO_EOS, produce.getName(), produce);
                log.info("Sending without " + record.key() + " " + record.value());
                producer.send(record);
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } catch (Exception e) {
            log.error("Exception {}", e);
        }
    }

    private Car getCar(){
        Random ranItem = new Random();
        Car car = listCar.get(ranItem.nextInt(listCar.size()));
        Random ranThreshold = new Random();
        int threshold = car.getPriceMin() + ranThreshold.nextInt(10000);
        Random ranPrice = new Random();
        car.setPrice(ranPrice.nextInt(car.getPriceMax()-car.getPriceMin())+threshold);
        return car;
    }

    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CarSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 25000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        return props;
    }

    private Producer<String, Car> getWithoutTransaction() {
        Properties props = getProducerConfig();
        Producer<String, Car> producer = new KafkaProducer<>(props);
        return producer;
    }

}
