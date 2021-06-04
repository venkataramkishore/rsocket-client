/**
 * 
 */
package com.kishore.client.rsocket;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.PreDestroy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.rsocket.metadata.SimpleAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import com.kishore.client.rsocket.mappings.RequestResponseMapping;
import com.kishore.rsocket.data.Message;

import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.WellKnownMimeType;
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
	private static final MimeType SIMPLE_AUTH = MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());
	
	private RSocketRequester rSocketRequester;
	private RSocketRequester.Builder builder;
	private RSocketStrategies rSocketStrategies;
	private Disposable disposable;

	@Autowired
	public RSocketShellClient(RSocketRequester.Builder builder, @Qualifier("rSocketStrategies")  RSocketStrategies strategies) {
//		this.rSocketRequester = builder.tcp("localhost", 7888);
		this.builder = builder;
		this.rSocketStrategies = strategies;

	}

	@ShellMethod("Login with username and password")
	public void login(String username, String password) {

		// 1 step
		String clientID = UUID.randomUUID().toString();
		logger.info("Connect client with ID : {}", clientID);
		
		SocketAcceptor responder = RSocketMessageHandler.responder(this.rSocketStrategies, new RequestResponseMapping());
		
		// 2 create user credentials
		UsernamePasswordMetadata user = new UsernamePasswordMetadata(username, password);
		
		// 3 step to setup route to get a request from server
		// Something wrong with tcp method which is recommended to use instead of connectTcp
		this.rSocketRequester = builder.setupRoute("shell-client").setupData(clientID)
				.setupMetadata(user, SIMPLE_AUTH)
				.rsocketStrategies(
						builder -> builder.encoder(new SimpleAuthenticationEncoder()))
				.rsocketConnector(connector -> connector.acceptor(responder))
				.connectTcp("localhost", 7888)
				.block();
		// 4 step
		this.rSocketRequester.rsocket().onClose()
				.doOnError(error -> logger.error("Connection closed with error {}", error))
				.doFinally(consumer -> logger.info("Client disconnected..!!")).subscribe();
		
	}
	
	@PreDestroy
	@ShellMethod("Logout user session")
	public void logout() throws InterruptedException {
		if(userLoggedIn()) {
			this.stopStream();
			this.rSocketRequester.dispose();
			logger.info("Logged user out...!!");
		}
	}
	
	public boolean userLoggedIn() {
		if(Objects.isNull(this.rSocketRequester) || this.rSocketRequester.rsocket().isDisposed()) {
			return false;
		}
		return true;
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
		if (userLoggedIn() &&  Objects.nonNull(this.disposable)) {
			this.disposable.dispose();
			logger.info("Stopping all clients..");
		}
	}

	@ShellMethod("Send and receive stream of messages")
	public void pullAndFetchStreamOfMessages() throws InterruptedException {
		Mono<Duration> setting1 = Mono.just(Duration.ofSeconds(1));
		Mono<Duration> setting2 = Mono.just(Duration.ofSeconds(3)).delayElement(Duration.ofSeconds(5));
		Mono<Duration> setting3 = Mono.just(Duration.ofSeconds(5)).delayElement(Duration.ofSeconds(15));

		Flux<Duration> settings = Flux.concat(setting1, setting2, setting3)
				.doOnNext(d -> logger.info("\nSending setting for {}-second interval.\n", d.getSeconds()));
		this.disposable = rSocketRequester.route("stream-stream").data(settings).retrieveFlux(Message.class)
				.subscribe(message -> logger.info("Message received {}", message));
	}

}
