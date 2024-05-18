package com.example.kafkautilitiesservice.controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/")
public class RootController {

    @GetMapping(value = {"", "/kafka-tool"})
    public String kafkaPage() {
        return "kafka-tool";
    }
}
