package poc.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ExampleProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        kvStore = (KeyValueStore) context.getStateStore("countValueStore");
        this.context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            KeyValueIterator<String, Long> iter = this.kvStore.all();
            Long sum = 0L;
            while (iter.hasNext()) {
                KeyValue<String, Long> entry = iter.next();
                sum += entry.value;
                kvStore.delete(entry.key);
            }
            iter.close();
            log.info("SUM IS {}",sum);
            context.commit();
        });
    }

    @Override
    public void process(String key, String value) {
        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (Exception e) {
            log.error("Exception sleep");
        }
        log.info("Read {} {}",value,key);
        log.debug("stupid stuff");
        kvStore.put(key,Long.valueOf(value));
        context.forward(key,value);
    }

    @Override
    public void close() {
        log.error("Closing {}", this.getClass().getName());
    }

}
