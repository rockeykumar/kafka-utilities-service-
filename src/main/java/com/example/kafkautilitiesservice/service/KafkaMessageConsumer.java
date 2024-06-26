package com.example.kafkautilitiesservice.service;

import com.example.kafkautilitiesservice.dto.MessageInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaMessageConsumer {


    public List<MessageInfo> getNLastMessages(String bootStrapServer, String topicName, int sizeOfMessages) {
        List<MessageInfo> messageResult = new ArrayList<MessageInfo>();
        try(KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(new Properties())) {

            List<TopicPartition> partitions = consumer.partitionsFor(topicName).stream()
                    .map(partitionInfo -> new TopicPartition(topicName, partitionInfo.partition()))
                    .collect(Collectors.toList());

            if (partitions.isEmpty()) return Collections.emptyList();

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            for (TopicPartition partition : partitions) {
                long endOffset = endOffsets.get(partition);
                long startOffset = Math.max(0, endOffset - sizeOfMessages);

                consumer.assign(Collections.singleton(partition));
                consumer.seek(partition, startOffset);

                while (consumer.position(partition) < endOffset) {
                    ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, Object> record : records) {
                        String message = (String) record.value();
                        MessageInfo messageInfo = MessageInfo.builder()
                                .topic(record.topic())
                                .offset(record.offset())
                                .timestamp(record.timestamp())
                                .partition(record.partition())
                                .date(new Date(record.timestamp()))
                                .messages(new ObjectMapper().readValue(message, Map.class))
                                .build();
                        messageResult.add(messageInfo);
                    }
                }
            }
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return messageResult;
    }
}
