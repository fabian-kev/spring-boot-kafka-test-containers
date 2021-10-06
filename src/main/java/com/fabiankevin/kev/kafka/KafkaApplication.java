package com.fabiankevin.kev.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.LongStream;

@SpringBootApplication
@RestController
public class KafkaApplication {
//    @Autowired
//    private KafkaTemplate<String, String> kafka;

//    @Autowired
//    private NewTopic topic1;

//    @Autowired
//    private KafkaAdmin kafkaAdmin;
    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }


//    @PutMapping
//    public void updateTopic(){
////        topic1.configs();
//        kafkaAdmin.createOrModifyTopics(new NewTopic("topic-1", 1, (short) 1));
//    }

//    @PostMapping
//    public void sendMessage(@RequestBody Message message){
//        kafkaTemplate.send("topic-1", message).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
//            @Override
//            public void onFailure(Throwable ex) {
//                System.out.println("Fail");
//            }
//
//            @Override
//            public void onSuccess(SendResult<String, Object> result) {
//                System.out.println(result);
//            }
//        });
//    }

    public static class Message {
        private String content;

        public Message() {
        }

        public Message(String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("topic").partitions(6).replicas(1).build();
    }

    @Component
    @RequiredArgsConstructor
    @Log4j2
    public static class Producer {

        private final KafkaTemplate<String, String> kafkaTemplate;
        private final NewTopic topic;

        @EventListener(ApplicationStartedEvent.class)
        public void produce() {
            log.info("PROCUDING!!");
            LongStream.range(0, 10).forEach(i -> {
                String key = "kafka";
                String value = key.toUpperCase() + i;
                System.out.println("PROCUDING "+value);
                kafkaTemplate.send(topic.name(), key, value).addCallback(result -> {
                    if (result != null) {
                        final RecordMetadata recordMetadata = result.getRecordMetadata();
                        log.info("produced to {}, {}, {}", recordMetadata.topic(), recordMetadata.partition(),
                                recordMetadata.offset());
                    }
                }, ex -> {
                    log.info("ERRORR {}", ex);
                });

            });
            kafkaTemplate.flush();
        }
    }
}
