package poc.kafka.web;

import poc.kafka.service.ProducerSample;
import poc.kafka.service.SampleKsStream;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

import static org.springframework.http.HttpStatus.OK;

@RestController
@RequestMapping("/sample")
@AllArgsConstructor
public class ApiSampleLag {

    private final SampleKsStream sampleKsStream;
    private final ProducerSample producerSample;

    @ResponseStatus(OK)
    @GetMapping("launchKs")
    public void launchKs() {
        sampleKsStream.createKStream();
    }

    @ResponseStatus(OK)
    @GetMapping("launchKsWithKafkaProxyClientSupplier")
    public void launchKsWithKafkaProxyClientSupplier() {
        sampleKsStream.createKStreamWithKafkaProxyClientSupplier();
    }

    @ResponseStatus(OK)
    @GetMapping("generateData")
    public void generateData(@RequestParam Integer argSize) {
        producerSample.procudeDataSampleWithOutTransaction(argSize);
    }


}
