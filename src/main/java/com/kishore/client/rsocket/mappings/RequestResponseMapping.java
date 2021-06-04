package com.kishore.client.rsocket.mappings;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.handler.annotation.MessageMapping;

import reactor.core.publisher.Flux;

public class RequestResponseMapping {

	
	private static final Logger logger = LoggerFactory.getLogger(RequestResponseMapping.class);

	@MessageMapping("client-status")
	public Flux<String> statusUpdate(String status) {
		logger.info("Inside status Update client with status {}", status);
		return Flux.interval(Duration.ofSeconds(5))
				.map(index -> String.valueOf(Runtime.getRuntime().freeMemory()));
	}
}
