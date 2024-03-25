package com.solace.spring.cloud.stream.binder.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.acks.AcknowledgmentCallback.Status;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;

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
		Mockito.verify(StaticMessageHeaderAccessor.getAcknowledgmentCallback(inputMessage), Mockito.never())
				.acknowledge(Mockito.any());
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
	public void testNoFailedMessage(@Mock AcknowledgmentCallback acknowledgementCallback) {
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException("test"),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback, Mockito.never()).acknowledge(AcknowledgmentCallback.Status.REJECT);
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

	@Test
	public void testMessagingExceptionContainingDifferentFailedMessage(
			@Mock AcknowledgmentCallback acknowledgementCallback) {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY,
				MessageBuilder.withPayload("some-other-message").build());

		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(Status.REQUEUE);
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
