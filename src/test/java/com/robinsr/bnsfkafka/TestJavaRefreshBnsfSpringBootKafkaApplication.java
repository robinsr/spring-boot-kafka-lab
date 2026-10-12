package com.robinsr.bnsfkafka;

import org.springframework.boot.SpringApplication;

public class TestJavaRefreshBnsfSpringBootKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.from(JavaRefreshBnsfSpringBootKafkaApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
