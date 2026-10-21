package com.robinsr.kafkalab;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class SpringBootKafkaLabApplicationTests {

	@Test
	void contextLoads() {
		assertEquals(true, true);
	}

}
