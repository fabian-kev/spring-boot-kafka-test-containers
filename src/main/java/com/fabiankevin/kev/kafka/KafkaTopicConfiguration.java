package com.fabiankevin.kev.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfiguration {
//
    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;
//
//    @Bean
//    public KafkaAdmin kafkaAdmin() {
//        Map<String, Object> configs = new HashMap<>();
//        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//        return new KafkaAdmin(configs);
//    }
    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name("topic").partitions(6).replicas(1).build();
    }

//    @Bean
//    public RetryTopicConfiguration myRetryTopic(KafkaTemplate<String, KafkaApplication.Message> template) {
//        return RetryTopicConfigurationBuilder
//                .newInstance()
//                .fixedBackOff(3000)
//                .maxAttempts(4)
//                .create(template);
//    }
}
