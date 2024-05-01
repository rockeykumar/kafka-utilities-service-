package com.example.kafkautilitiesservice.controller;

import com.example.kafkautilitiesservice.service.kafkaMessagePublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/")
public class EventController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private kafkaMessagePublisher publisher;

    @Autowired
    private AdminClient adminClient;

    private final String topic = "javatechie-demo1";

    @Autowired
    private Properties consumerProperties;


    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            int count = 100;
            for (int i = 0; i < count; i++) {
                publisher.sendMessageToTopic(message + " - " + i);
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
        return ResponseEntity.ok("message published successfully...!");
    }

    @GetMapping("/list-all-topics") //done
    public List<String> listTopics() {
        List<String> topics = new ArrayList<>();
        try {
            ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
            Set<String> topicNames = adminClient.listTopics(listTopicsOptions).names().get();
            topics.addAll(topicNames);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return topics;
    }

    @GetMapping("/consumer/message")
    public void getMessage() {
        String bootstrapServers = "localhost:9092";
        String groupId = "my-consumer-group";
        String topic = "javatechie-demo1"; // Replace with your actual topic name

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        TopicPartition partition = new TopicPartition(topic, 0);
        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(partition);
        consumer.assign(partitions);
        consumer.subscribe(Collections.singletonList(topic));

//        int messagesToRead = 5;
//        int messagesRead = 0;
//
//        while (messagesRead < messagesToRead) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            System.out.println("Records size: " + records.count());
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println("Message: " + record.value());
//                messagesRead++;
//            }
//        }
//
//        consumer.close();

        int messagesToRetrieve = 5;
        consumer.seekToEnd(partitions);
        long startIndex = consumer.position(partition) - messagesToRetrieve;
        consumer.seek(partition, startIndex);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));

        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Message: " + record.value());
        }

        consumer.close();
    }

    @GetMapping("/lastMessages")
    public List<Map<String, Object>> getLastMessage() {
        String bootstrapServers = "localhost:9092";
        String groupId = "my-consumer-group";
        String topic = "javatechie-demo1";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        List<Map<String, Object>> messages = new ArrayList<>();
        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {

            TopicPartition partition = new TopicPartition(topic, 0);
            List<TopicPartition> partitions = new ArrayList<>();
            partitions.add(partition);
            consumer.assign(partitions);

            int messagesToRetrieve = 10;
            consumer.seekToEnd(partitions);
            long startIndex = consumer.position(partition) - messagesToRetrieve;
            consumer.seek(partition, startIndex);
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMinutes(1));

            ObjectMapper objectMapper = new ObjectMapper();

            for (ConsumerRecord<String, Object> record : records) {
                System.out.println("Sent message=[" + record.value() + "] with offset=[" + record.offset() + "]");
                Map<String, Object> map = new HashMap<>();
                map.put("Message", record.value());
                map.put("Offset", record.offset());
                messages.add(map);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return messages;
    }

    @GetMapping("/describe-topic/{topicName}")  // done
    public Map<String, Object> describeTopics(@PathVariable String topicName) {
        Map<String, Object> topicDetails = new HashMap<>();
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(properties)) {

            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));
            KafkaFuture<TopicDescription> topicDescriptionFuture = describeTopicsResult.values().get(topicName);

            try {
                TopicDescription topicDescription = topicDescriptionFuture.get();
                System.out.println("Topic details for '" + topicName + "':");
                System.out.println("Partition: " + topicDescription.partitions().get(0).partition());
                System.out.println("PartitionCount: " + topicDescription.partitions().size());
                System.out.println("Replication factor: " + topicDescription.partitions().get(0).replicas().size());
                System.out.println("Partition-Rockey: " + topicDescription.partitions().get(0).partition());

                topicDetails.put("Topic Name", topicName);
                topicDetails.put("Partition: ", topicDescription.partitions().get(0).partition());
                topicDetails.put("Partitions", topicDescription.partitions().size());
                topicDetails.put("Replication Factor", topicDescription.partitions().get(0).replicas().size());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return topicDetails;
    }
}
