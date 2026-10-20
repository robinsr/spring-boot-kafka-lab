package com.robinsr.bnsfkafka;


import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

  private final KafkaTemplate<String, Map<String, String>> kTemplate;
  private final NewTopic kTopic;

  public HelloController(KafkaTemplate<String, Map<String, String>> kTemplate, NewTopic kTopic) {
    this.kTemplate = kTemplate;
    this.kTopic = kTopic;
  }

  @GetMapping("/hello")
  public String handleGetHello(
    @RequestParam Map<String, String> params
  ) {
    kTemplate.send(kTopic.name(), params);
    String name = params.getOrDefault("name", "Buttface");
    return "Hello, %s!".formatted(name);
  }
}
