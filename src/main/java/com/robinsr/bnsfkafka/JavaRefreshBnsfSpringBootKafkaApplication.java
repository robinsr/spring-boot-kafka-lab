package com.robinsr.bnsfkafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class JavaRefreshBnsfSpringBootKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(JavaRefreshBnsfSpringBootKafkaApplication.class, args);
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("sensor-observations")
			.partitions(1)
			.replicas(1)
			.build();
	}

	@KafkaListener(id = "log-writer", topics = { "sensor-observations" })
	public void listen(String data) {
		System.out.println("Kafka data: '%s'".formatted(data));
	}
}
