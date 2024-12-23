package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


import java.util.Collections;
import java.util.List;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.example.KafkaUtil.*;

public class KafkaTest {

    private static Producer<String, String> producer;
    private static Consumer<String, String> consumer;

    @BeforeClass
    public void setup() {
        producer = initilizeKafkaProducer();
        consumer = initilizeKafkaConsumer();
    }

    @Test
    public void testProducerConsumer() {
        String topic = "testTopic";
        // Consumer subscribes to the topic
        consumer.subscribe(Collections.singletonList(topic));

        // Send a message with Kafka Producer
        String key1 = randomAlphanumeric(2);
        String value1 = randomAlphanumeric(30);
        String key2 = randomAlphanumeric(2);;
        String value2 =  randomAlphanumeric(30);;
        produceMsgsInTopic(producer, topic, key1, value1);
        produceMsgsInTopic(producer, topic, key2, value2);

        // Consume messages with Kafka Consumer
        List<ConsumerRecord<String, String>> records = consumeMsgsFromTopic(consumer, topic);

        // Assert the message content received by the consumer
        assertThat(records).hasSizeGreaterThan(0)
                .anyMatch(record -> (record.key().equals(key1) && record.value().equals(value1)))
                .anyMatch(record -> (record.key().equals(key2) && record.value().equals(value2)));

    }


    @AfterClass
    public void tearDown() {
        // Close the producer and consumer
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

}
