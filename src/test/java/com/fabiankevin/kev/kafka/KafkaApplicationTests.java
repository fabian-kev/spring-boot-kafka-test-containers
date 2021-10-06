package com.fabiankevin.kev.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;


class KafkaApplicationTests extends AbstractIntegrationTest {

    @Autowired
    KafkaProperties properties;

    @Autowired
    private NewTopic topic;

    @Test
    void contextLoads() {
        System.out.println("Topic name "+topic.name());
        // kafka consumer
        final Consumer<String, String> consumer = createConsumer(topic.name());
        // consumer read all messages

        final ArrayList<String> actualValues = new ArrayList<>();
        while (true) {
            final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            if (records.isEmpty()) {
                break;
            }
            records.forEach(stringStringConsumerRecord -> actualValues.add(stringStringConsumerRecord.value().replaceAll("[^a-zA-Z0-9]","")));
        }

        // fixture
        final ArrayList<String> ours = new ArrayList<>();
        LongStream.range(0, 10).forEach(i -> ours.add("kafka".toUpperCase() + i)
        );
        assertEquals(ours, actualValues);
    }


    private Consumer<String, String> createConsumer(final String topicName) {

        final Consumer<String, String>
                consumer = new DefaultKafkaConsumerFactory(properties.buildConsumerProperties(), StringDeserializer::new,
                        StringDeserializer::new).createConsumer();

        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }

}
