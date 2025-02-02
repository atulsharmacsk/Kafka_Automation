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

public class KafkaUserTest {

    private static Producer<String, User> producer;
    private static Consumer<String, User> consumer;

    @BeforeClass
    public void setup() {
        producer = initilizeKafkaProducer(UserSerializer.class);
        consumer = initilizeKafkaConsumer(UserDeserializer.class);
    }

    @Test
    public void testUserSerialization() {
        String topic = "userTopic";
        consumer.subscribe(Collections.singletonList(topic));

        String id1 = randomAlphanumeric(2);
        String name1 = randomAlphanumeric(30);
        String id2 = randomAlphanumeric(2);;
        String name2 =  randomAlphanumeric(30);;

        // Create User messages
        User user1 = new User(id1, name1, 25);
        User user2 = new User(id2, name2, 30);

        // Send messages to Kafka
        produceMsgsInTopic(producer, topic, user1.getId(), user1);
        produceMsgsInTopic(producer, topic, user2.getId(), user2);

        // Consume messages from Kafka
        List<ConsumerRecord<String, User>> records = consumeMsgsFromTopic(consumer, topic);

        // Assertions to verify serialization & deserialization
        assertThat(records).hasSizeGreaterThan(0)
                .anyMatch(record -> record.key().equals(user1.getId()) && record.value().equals(user1))
                .anyMatch(record -> record.key().equals(user2.getId()) && record.value().equals(user2));

        // Assertions to verify serialization, deserialization, and names
        assertThat(records)
                .hasSizeGreaterThan(0)
                .anyMatch(record -> record.key().equals(user1.getId()) && record.value().getName().equals(user1.getName()))
                .anyMatch(record -> record.key().equals(user2.getId()) && record.value().getName().equals(user2.getName()));

    }

    @AfterClass
    public void tearDown() {
        if (producer != null) producer.close();
        if (consumer != null) consumer.close();
    }

}
