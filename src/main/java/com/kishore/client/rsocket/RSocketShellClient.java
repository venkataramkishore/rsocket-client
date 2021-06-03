/**
 * 
 */
package com.kishore.client.rsocket;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.messaging.rsocket.RSocketRequester;

import java.time.Duration;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;

import org.apache.logging.log4j.Logger;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import com.kishore.rsocket.data.Message;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author kishore
 *
 */
@ShellComponent
public class RSocketShellClient {
	private static final Logger logger = LogManager.getLogger(RSocketShellClient.class);

	private final RSocketRequester rSocketRequester;
	private Disposable disposable;

	@Autowired
	public RSocketShellClient(RSocketRequester.Builder builder) {
		this.rSocketRequester = builder.tcp("localhost", 7888);
	}

	@ShellMethod("Send one request, One response will be printed...")
	public void requestResponse() throws InterruptedException {
		logger.info("Inside request response method..");

		Message message = this.rSocketRequester.route("request-response").data(new Message("Hi Kishore..!"))
				.retrieveMono(Message.class).block();
		logger.info("Exiting request response method.. {}", message);
	}

	@ShellMethod("Fire and forget request")
	public void fireRequest() throws InterruptedException {
		logger.info("Inside fire request method..");

		Void message = this.rSocketRequester.route("fire-and-forget").data(new Message("Hi Kishore..!"))
				.retrieveMono(Void.class).block();
		logger.info("Exiting fire request method.. {}", message);
	}

	@ShellMethod("Generate messages using streams")
	public void streamOfMessages() throws InterruptedException {
		logger.info("Inside a stream of messages");

		this.disposable = rSocketRequester.route("request-stream").data(new Message("Get me stream."))
				.retrieveFlux(Message.class)
				.subscribe(streamMessage -> logger.info("Stream message received as  {}", streamMessage));

	}

	@ShellMethod("To stop stream from requesting")
	public void stopStream() throws InterruptedException {
		if (Objects.nonNull(this.disposable)) {
			this.disposable.dispose();
		}
	}
	
	@ShellMethod("Send and receive stream of messages")
	public void pullAndFetchStreamOfMessages() throws InterruptedException {
		Mono<Duration> setting1 = Mono.just(Duration.ofSeconds(1));
		Mono<Duration> setting2 = Mono.just(Duration.ofSeconds(3)).delayElement(Duration.ofSeconds(5));
		Mono<Duration> setting3 = Mono.just(Duration.ofSeconds(5)).delayElement(Duration.ofSeconds(15));

		Flux<Duration> settings = Flux.concat(setting1, setting2, setting3)
		                                        .doOnNext(d -> logger.info("\nSending setting for {}-second interval.\n", d.getSeconds()));
		this.disposable = rSocketRequester.route("stream-stream")
				.data(settings)
				.retrieveFlux(Message.class)
				.subscribe( message -> logger.info("Message received {}", message));
	}

}
