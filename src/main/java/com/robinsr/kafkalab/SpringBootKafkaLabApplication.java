package com.robinsr.kafkalab;


import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;


@SpringBootApplication
public class SpringBootKafkaLabApplication {

	private static final Logger log = LoggerFactory.getLogger(SpringBootKafkaLabApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaLabApplication.class, args);
	}

	@Bean(name = "sensor-observations")
	public NewTopic sensorObservationsRaw() {
		return TopicBuilder.name("sensor-observations")
			.partitions(1)
			.replicas(1)
			.build();
	}

	@KafkaListener(
		id = "log-writer",
		groupId = "temperature-summary-consumer",
		topics = { "sensor-observations.1m" }
	)
	public void listen(String data) {
		log.info("Temperature (1 minute) = {}", data);
	}
}
