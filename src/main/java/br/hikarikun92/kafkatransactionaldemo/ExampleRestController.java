package br.hikarikun92.kafkatransactionaldemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class ExampleRestController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExampleRestController.class);

    private final KafkaTemplate<String, String> template;

    public ExampleRestController(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    //Note: this method should be a POST in practice, but I'm keeping a GET for simplicity
    @Transactional
    @GetMapping(produces = MediaType.TEXT_PLAIN_VALUE)
    public String sendMessage(@RequestParam String key, @RequestParam String value){
        LOGGER.info("Sending message with key {} and value {}", key, value);
        template.send("example-topic", key, value);

        if ("error".equals(key)) {
            //Force an error to demonstrate the read_committed and read_uncommitted behaviors. The message MIGHT be
            //received by the "read_uncommitted" consumer if the transaction takes a while to be aborted (some seconds,
            //for example), or might not be received if the rollback is quick enough.
            throw new RuntimeException("An error happened");
        }

        return "OK";
    }
}
