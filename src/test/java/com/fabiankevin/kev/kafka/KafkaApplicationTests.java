package com.fabiankevin.kev.kafka;

import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;


class KafkaApplicationTests extends AbstractIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    KafkaProperties properties;

    @Autowired
    private NewTopic topic;

    @BeforeEach
    public void produce() {
        LongStream.range(0, 10).forEach(i -> {
            String key = "kafka";
            String value = key.toUpperCase() + i;
            kafkaTemplate.send(topic.name(), key, value).addCallback(result -> {
                if (result != null) {
                    final RecordMetadata recordMetadata = result.getRecordMetadata();
                    System.out.println(recordMetadata);
//                    log.info("produced to {}, {}, {}", recordMetadata.topic(), recordMetadata.partition(),
//                            recordMetadata.offset());
                }
            }, ex -> {
                System.out.println("err "+ex);
//                log.info("ERRORR {}", ex);
            });

        });
        kafkaTemplate.flush();
    }

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
