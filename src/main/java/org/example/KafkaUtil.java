package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaUtil {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Kafka broker address

    public static Producer<String, String> initilizeKafkaProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }

    public static Consumer<String, String> initilizeKafkaConsumer() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(consumerProps);
    }

    public static void produceMsgsInTopic(Producer<String, String> producer, String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                System.out.println("Error sending message: " + exception.getMessage());
            } else {
                System.out.println("Message with key "+ key + " value " +value+ " sent to topic: " + metadata.topic());
            }
        });
    }

    public static List<ConsumerRecord<String, String>> consumeMsgsFromTopic(Consumer<String, String> consumer, String topic) {
        List<ConsumerRecord<String, String>> recordsList = new ArrayList<>();
        long endTime = System.currentTimeMillis() + 20000; // 20 seconds timeout
        while (System.currentTimeMillis() < endTime) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
            for (ConsumerRecord<String, String> rec : records) {
                if (rec.topic().equals(topic)) {
                    recordsList.add(rec); // Add the message to the list
                }
            }
        }
        return recordsList;
    }
}
