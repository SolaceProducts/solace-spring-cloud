package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.test.spring.MessageGenerator;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ErrorChannelSendingCorrelationKeyTest {
	private final ErrorMessageStrategy errorMessageStrategy = new SolaceMessageHeaderErrorMessageStrategy();

	@Test
	void testNoErrorChannel() {
		Message<?> message = MessageBuilder.withPayload("test").build();
		ErrorChannelSendingCorrelationKey key = new ErrorChannelSendingCorrelationKey(message, null,
				errorMessageStrategy);

		String description = "some failure";
		Exception cause = new RuntimeException("test");

		MessagingException exception = key.send(description, cause);
		assertThat(exception).hasMessageStartingWith(description);
		assertThat(exception).hasCause(cause);
		assertThat(exception.getFailedMessage()).isEqualTo(message);
	}

	@Test
	void testErrorChannel(SoftAssertions softly) {
		Message<?> message = MessageBuilder.withPayload("test").build();
		DirectChannel errorChannel = new DirectChannel();
		ErrorChannelSendingCorrelationKey key = new ErrorChannelSendingCorrelationKey(message, errorChannel,
				errorMessageStrategy);

		String description = "some failure";
		Exception cause = new RuntimeException("test");

		errorChannel.subscribe(msg -> {
			softly.assertThat(msg).isInstanceOf(ErrorMessage.class);
			ErrorMessage errorMsg = (ErrorMessage) msg;
			softly.assertThat(errorMsg.getOriginalMessage()).isEqualTo(message);
			softly.assertThat(errorMsg.getPayload()).isInstanceOf(MessagingException.class);
			softly.assertThat(errorMsg.getPayload()).hasMessageStartingWith(description);
			softly.assertThat(errorMsg.getPayload()).hasCause(cause);
			softly.assertThat(((MessagingException) errorMsg.getPayload()).getFailedMessage()).isEqualTo(message);
		});

		MessagingException exception = key.send(description, cause);
		assertThat(exception).hasMessageStartingWith(description);
		assertThat(exception).hasCause(cause);
		assertThat(exception.getFailedMessage()).isEqualTo(message);
	}

	private enum MessageLayout {
		SERIAL_SINGLE(false), BATCH_MULTI(true), BATCH_SINGLE(true);
		private final boolean isBatched;

		MessageLayout(boolean isBatched) {
			this.isBatched = isBatched;
		}

		public boolean isBatched() {
			return isBatched;
		}
	}
}
