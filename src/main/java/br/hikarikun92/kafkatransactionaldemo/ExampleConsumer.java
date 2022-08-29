package br.hikarikun92.kafkatransactionaldemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ExampleConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExampleConsumer.class);

    //This method will only receive messages that have been committed (i.e. the method who produced it threw no
    //exceptions after producing it). Property isolation.level is set to read_committed via application.properties.
    @KafkaListener(topics = "example-topic")
    public void consumeCommitted(ConsumerRecord<String, String> record) {
        LOGGER.info("Received committed message with key \"{}\" and value \"{}\"", record.key(), record.value());
    }

    //This method MIGHT receive all the messages that have been produced, even if uncommitted (i.e. the method who
    //produced it threw an exception after producing it). Property isolation.level here is read_uncommitted (Kafka's default).
    @KafkaListener(topics = "example-topic", groupId = "another-group", properties = "isolation.level=read_uncommitted")
    public void consumeUncommitted(ConsumerRecord<String, String> record) {
        LOGGER.info("Received possibly uncommitted message with key \"{}\" and value \"{}\"", record.key(), record.value());
    }
}
