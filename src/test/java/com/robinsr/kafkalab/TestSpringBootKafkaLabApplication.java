package com.robinsr.kafkalab;

import org.springframework.boot.SpringApplication;

public class TestSpringBootKafkaLabApplication {

	public static void main(String[] args) {
		SpringApplication.from(SpringBootKafkaLabApplication::main)
			.with(TestcontainersConfiguration.class).run(args);
	}
}
