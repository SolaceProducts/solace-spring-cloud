package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.acks.AcknowledgmentCallback.Status;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class SolaceErrorMessageHandlerTest {
	SolaceMessageHeaderErrorMessageStrategy errorMessageStrategy = new SolaceMessageHeaderErrorMessageStrategy();
	SolaceErrorMessageHandler errorMessageHandler;
	AttributeAccessor attributeAccessor;

	@BeforeEach
	public void setup() {
		errorMessageHandler = new SolaceErrorMessageHandler();
		attributeAccessor = ErrorMessageUtils.getAttributeAccessor(null, null);
	}

	@Test
	public void testNotAnErrorMessage() {
		assertThatThrownBy(() -> errorMessageHandler.handleMessage(MessageBuilder.withPayload("test").build()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Expected an ErrorMessage");
	}

	@Test
	public void testAcknowledgmentCallbackHeader(@Mock AcknowledgmentCallback acknowledgementCallback) {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK,
						Mockito.mock(AcknowledgmentCallback.class))
				.build();

		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		attributeAccessor.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_ACKNOWLEDGMENT_CALLBACK,
				acknowledgementCallback);

		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(Status.REQUEUE);
	}

	@Test
	public void testFailedMessageAcknowledgmentCallback(@Mock AcknowledgmentCallback acknowledgementCallback) {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(Status.REQUEUE);
	}

	@Test
	public void testNoFailedMessage() {
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException("test"),
				attributeAccessor);

		assertThatThrownBy(() -> errorMessageHandler.handleMessage(errorMessage))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("does not contain an acknowledgment callback");
	}

	@Test
	public void testNonMessagingException(@Mock AcknowledgmentCallback acknowledgementCallback) {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new RuntimeException("test"),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(Status.REQUEUE);
	}

	@CartesianTest(name = "[{index}] ackCallbackHeaderProvider={0}")
	public void testMessagingExceptionContainingDifferentFailedMessage(
			@Values(strings = {"messaging-exception", "input-message", "error-message", "all:same", "all:different"})
			String ackCallbackHeaderProvider,
			@Mock AcknowledgmentCallback acknowledgementCallback1,
			@Mock AcknowledgmentCallback acknowledgementCallback2,
			@Mock AcknowledgmentCallback acknowledgementCallback3) {
		Message<?> messageWithAckCallback = MessageBuilder.withPayload("with-callback-1")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback1)
				.build();
		Message<String> messageNoAckCallback = MessageBuilder.withPayload("some-other-message").build();

		MessagingException exception = switch (ackCallbackHeaderProvider) {
			case "messaging-exception", "all:same", "all:different" -> new MessagingException(messageWithAckCallback);
			default -> new MessagingException(messageNoAckCallback);
		};

		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, switch (ackCallbackHeaderProvider) {
			case "input-message", "all:same" -> messageWithAckCallback;
			case "all:different" -> MessageBuilder.withPayload("with-callback-1")
					.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback2)
					.build();
			default -> messageNoAckCallback;
		});

		if (ackCallbackHeaderProvider.equals("error-message") || ackCallbackHeaderProvider.startsWith("all:")) {
			attributeAccessor.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_ACKNOWLEDGMENT_CALLBACK,
					ackCallbackHeaderProvider.equals("all:different") ? acknowledgementCallback3 : acknowledgementCallback1);
		}

		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(exception, attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback1).acknowledge(Status.REQUEUE);
		Mockito.verify(acknowledgementCallback2, Mockito.times(ackCallbackHeaderProvider.equals("all:different") ? 1 : 0))
				.acknowledge(Status.REQUEUE);
		Mockito.verify(acknowledgementCallback3, Mockito.times(ackCallbackHeaderProvider.equals("all:different") ? 1 : 0))
				.acknowledge(Status.REQUEUE);
	}

	@Test
	public void testMessagingExceptionWithNullFailedMessage(
			@Mock AcknowledgmentCallback acknowledgementCallback) {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);

		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException("test"),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(Status.REQUEUE);
	}

	@Test
	public void testSourceDataHeader(@Mock AcknowledgmentCallback acknowledgementCallback) {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		TextMessage sourceData = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		attributeAccessor.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_RAW_MESSAGE, sourceData);

		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(Status.REQUEUE);
	}

	@Test
	public void testStaleException(@Mock AcknowledgmentCallback acknowledgementCallback) {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage, new SolaceAcknowledgmentException("test", null)),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback, Mockito.never()).acknowledge(AcknowledgmentCallback.Status.REJECT);
	}

	@Test
	public void testHandleMessageWhenConsumerClosed(
			@Mock AcknowledgmentCallback acknowledgementCallback) {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK,
						acknowledgementCallback).build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage),
				attributeAccessor);

		Mockito.doThrow(new SolaceAcknowledgmentException("ack-error",
						new IllegalStateException("Attempted an operation on a closed message consumer")))
				.when(acknowledgementCallback)
				.acknowledge(Status.REQUEUE);

		assertThrows(SolaceAcknowledgmentException.class, () -> errorMessageHandler.handleMessage(errorMessage));
		Mockito.verify(acknowledgementCallback).acknowledge(Status.REQUEUE);
	}
}
