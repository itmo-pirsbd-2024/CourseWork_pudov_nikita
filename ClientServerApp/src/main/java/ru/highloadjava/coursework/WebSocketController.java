package ru.highloadjava.coursework;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;


@Controller
@RequestMapping("/")
public class WebSocketController {
    private final KafkaConsumerService kafkaConsumerService;

    public WebSocketController(KafkaConsumerService kafkaConsumerService) {
        this.kafkaConsumerService = kafkaConsumerService;

    }

    @GetMapping("/chart")
    public String chart(Model model) {
        return "chart";
    }


}
