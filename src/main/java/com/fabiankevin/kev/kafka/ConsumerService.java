package com.fabiankevin.kev.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public class ConsumerService {

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 1000, multiplier = 2, maxDelay = 2000))
    @KafkaListener(topics = {"topic-1"}, groupId = "g1")
    private void listen1(String message){
        System.out.println("Received1: "+message);
    }

    @KafkaListener(topics = {"topic-1"}, groupId = "g1")
    private void listen2(String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition){
        System.out.println("Received2: "+message + "Partition: "+partition);
    }

    @KafkaListener(topics = {"topic-1"}, groupId = "g1")
    private void listen3(String message){
        System.out.println("Received3: "+message);
    }
    @KafkaListener(topics = {"topic-1"}, groupId = "g1")
    private void listen4(String message){
        System.out.println("Received4: "+message);
    }
}
