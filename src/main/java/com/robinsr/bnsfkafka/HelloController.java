package com.robinsr.bnsfkafka;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

  @GetMapping("/hello")
  public String handleGetHello(
    @RequestParam(defaultValue = "Butt-Face") String name
  ) {
    return "Hello, %s!".formatted(name);
  }
}
