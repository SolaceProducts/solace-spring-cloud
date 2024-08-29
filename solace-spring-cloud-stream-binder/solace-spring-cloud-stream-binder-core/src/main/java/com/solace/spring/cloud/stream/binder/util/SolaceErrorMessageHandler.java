package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.inbound.acknowledge.SolaceAckUtil;
import com.solacesystems.jcsmp.XMLMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SolaceErrorMessageHandler implements MessageHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceErrorMessageHandler.class);

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		if (!(message instanceof ErrorMessage errorMessage)) {
			throw new IllegalArgumentException(String.format("Spring message %s: Expected an %s, but got a %s",
					StaticMessageHeaderAccessor.getId(message), ErrorMessage.class.getSimpleName(),
					message.getClass().getSimpleName()));
		}

		handleErrorMessage(errorMessage);
	}

	private void handleErrorMessage(ErrorMessage errorMessage) {
		Message<?> messagingExceptionFailedMessage = errorMessage.getPayload() instanceof MessagingException m ?
				m.getFailedMessage() : null;
		Set<AcknowledgmentCallback> acknowledgmentCallbacks = Stream.of(Stream.of(errorMessage),
						Stream.ofNullable(messagingExceptionFailedMessage),
						Stream.ofNullable(errorMessage.getOriginalMessage()))
				.flatMap(s -> s)
				.map(StaticMessageHeaderAccessor::getAcknowledgmentCallback)
				.filter(Objects::nonNull)
				.collect(Collectors.toUnmodifiableSet());

		LOGGER.atInfo()
				.setMessage("Processing message {} <messaging-exception-message: {}, original-message: {}, source-jcsmp-message: {}>")
				.addArgument(() -> StaticMessageHeaderAccessor.getId(errorMessage))
				.addArgument(() -> messagingExceptionFailedMessage != null ?
						StaticMessageHeaderAccessor.getId(messagingExceptionFailedMessage) : null)
				.addArgument(() -> errorMessage.getOriginalMessage() != null ?
						StaticMessageHeaderAccessor.getId(errorMessage.getOriginalMessage()) : null)
				.addArgument(() -> StaticMessageHeaderAccessor.getSourceData(errorMessage) instanceof XMLMessage m ?
						m.getMessageId() : null)
				.log();

		if (acknowledgmentCallbacks.isEmpty()) {
			// Should never happen under normal use
			throw new IllegalArgumentException(String.format(
					"Spring error message %s does not contain an acknowledgment callback. Message cannot be acknowledged",
					StaticMessageHeaderAccessor.getId(errorMessage)));
		}

		for(AcknowledgmentCallback acknowledgmentCallback : acknowledgmentCallbacks) {
			try {
				if (!SolaceAckUtil.republishToErrorQueue(acknowledgmentCallback)) {
					AckUtils.requeue(acknowledgmentCallback);
				}
			} catch (SolaceAcknowledgmentException e) {
				LOGGER.error("Spring error message {}: exception in error handler",
						StaticMessageHeaderAccessor.getId(errorMessage), e);
				throw e;
			}
		}
	}
}
