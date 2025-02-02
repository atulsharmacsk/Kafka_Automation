package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtil.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_GROUP_ID = "test-group";
    private static final int POLL_TIMEOUT_MS = 2000;
    private static final int CONSUMER_TIMEOUT_MS = 20000;

    // Common method to get Producer properties
    private static <T> Properties getProducerProperties(Class<? extends Serializer<T>> serializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer.getName());
        return props;
    }

    // Generic Producer Initialization
    public static <T> Producer<String, T> initilizeKafkaProducer(Class<? extends Serializer<T>> serializer) {
        return new KafkaProducer<>(getProducerProperties(serializer));
    }

    // Common method to get Consumer properties
    private static <T> Properties getConsumerProperties(Class<? extends Deserializer<T>> deserializer) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer.getName());
        return props;
    }

    // Generic Consumer Initialization
    public static <T> Consumer<String, T> initilizeKafkaConsumer(Class<? extends Deserializer<T>> deserializer) {
        return new KafkaConsumer<>(getConsumerProperties(deserializer));
    }

    // Generic method to produce messages
    public static <T> void produceMsgsInTopic(Producer<String, T> producer, String topic, String key, T value) {
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Error sending message: {}", exception.getMessage());
            } else {
                LOGGER.info("Message sent to topic {} | key: {} | value: {}", metadata.topic(), key, value);
            }
        });
        producer.flush();
    }

    // Generic method to consume messages
    public static <T> List<ConsumerRecord<String, T>> consumeMsgsFromTopic(Consumer<String, T> consumer, String topic) {
        List<ConsumerRecord<String, T>> recordsList = new ArrayList<>();
        long endTime = System.currentTimeMillis() + CONSUMER_TIMEOUT_MS;

        while (System.currentTimeMillis() < endTime) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
            records.records(topic).forEach(recordsList::add);
        }

        return recordsList;
    }
}
