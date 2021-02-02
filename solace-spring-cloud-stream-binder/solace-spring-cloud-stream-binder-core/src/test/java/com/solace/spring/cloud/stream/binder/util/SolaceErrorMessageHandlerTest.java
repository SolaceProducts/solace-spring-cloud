package com.solace.spring.cloud.stream.binder.util;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;

public class SolaceErrorMessageHandlerTest {
	@Rule
	public MockitoRule initRule = MockitoJUnit.rule();

	@Mock
	JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback acknowledgementCallback;

	SolaceMessageHeaderErrorMessageStrategy errorMessageStrategy = new SolaceMessageHeaderErrorMessageStrategy();
	SolaceErrorMessageHandler errorMessageHandler;
	AttributeAccessor attributeAccessor;

	@Before
	public void setup() {
		errorMessageHandler = new SolaceErrorMessageHandler();
		attributeAccessor = ErrorMessageUtils.getAttributeAccessor(null, null);
	}

	@Test
	public void testHandleAcknowledgmentCallback() {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(AcknowledgmentCallback.Status.REJECT);
	}

	@Test
	public void testNoFailedMessage() {
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException("test"),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback, Mockito.never()).acknowledge(AcknowledgmentCallback.Status.REJECT);
	}

	@Test
	public void testNonMessagingException() {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new RuntimeException("test"),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(AcknowledgmentCallback.Status.REJECT);
	}

	@Test
	public void testMessagingExceptionContainingDifferentFailedMessage() {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY,
				MessageBuilder.withPayload("some-other-message").build());

		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(AcknowledgmentCallback.Status.REJECT);
	}

	@Test
	public void testMessagingExceptionWithNullFailedMessage() {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);

		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException("test"),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(AcknowledgmentCallback.Status.REJECT);
	}

	@Test
	public void testStaleException() {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage, new SolaceStaleMessageException("test")),
				attributeAccessor);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback, Mockito.never()).acknowledge(AcknowledgmentCallback.Status.REJECT);
	}

	@Test
	public void testStaleMessage() {
		Message<?> inputMessage = MessageBuilder.withPayload("test")
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgementCallback)
				.build();
		attributeAccessor.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, inputMessage);
		ErrorMessage errorMessage = errorMessageStrategy.buildErrorMessage(
				new MessagingException(inputMessage),
				attributeAccessor);

		Mockito.doThrow(new SolaceAcknowledgmentException("ack-error", new SolaceStaleMessageException("stale")))
				.when(acknowledgementCallback)
				.acknowledge(AcknowledgmentCallback.Status.REJECT);

		errorMessageHandler.handleMessage(errorMessage);
		Mockito.verify(acknowledgementCallback).acknowledge(AcknowledgmentCallback.Status.REJECT);
	}
}
