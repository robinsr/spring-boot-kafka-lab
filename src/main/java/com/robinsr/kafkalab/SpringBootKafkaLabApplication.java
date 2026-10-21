package com.robinsr.kafkalab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.robinsr.kafkalab.TemperatureStreamsConfiguration.MinuteSummary;


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

	private static final ObjectMapper mapper = new ObjectMapper();

	public static String toJson(Object obj) throws Exception {
		return mapper.writeValueAsString(obj);
	}

	@KafkaListener(
		id = "log-writer",
		groupId = "temperature-summary-consumer",
		topics = { "sensor-observations.1m" }
	)
	public void listen(MinuteSummary summary) {
		try {
			log.debug(toJson(summary));
		} catch (Exception e) {
			log.error("MinuteSummary don't like be JSON ¯\\_(ツ)_/¯", e);
		}

		log.info("Temperature (1 minute) = {}", summary.averageTemperatureC());
	}
}
