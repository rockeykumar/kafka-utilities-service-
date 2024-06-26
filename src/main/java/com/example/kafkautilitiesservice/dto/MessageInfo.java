package com.example.kafkautilitiesservice.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageInfo {
    private String topic;
    private Long offset;
    private Long timestamp;
    private Integer partition;
    private Date date;
    private Map<String, Object> messages;
}
