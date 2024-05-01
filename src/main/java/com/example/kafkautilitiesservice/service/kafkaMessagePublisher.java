package com.example.kafkautilitiesservice.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class kafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private final String topic = "javatechie-demo1";

    public void sendMessageToTopic (String message) throws ExecutionException, InterruptedException, TimeoutException {
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, message);
        SendResult<String, Object> sendResult = future.get(10, TimeUnit.SECONDS);
        System.out.println("Sent message=[" + message + "] with offset=[" + sendResult.getRecordMetadata().offset() + "]");
        System.out.println("Partition count:[" + sendResult.getRecordMetadata().partition() + "] and Replication Factor:[]" );
        System.out.println();
//        future.whenComplete((result, exception) -> {
//            if (exception != null) {
//                System.out.println("Unable to send message=[" + message + "] due to : " + exception.getMessage());
//            } else {
//                System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
//            }
//        });
    }
}
