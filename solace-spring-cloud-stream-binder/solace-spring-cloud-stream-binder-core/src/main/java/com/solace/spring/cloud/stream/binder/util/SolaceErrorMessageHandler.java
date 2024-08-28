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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class SolaceErrorMessageHandler implements MessageHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceErrorMessageHandler.class);

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		UUID springId = StaticMessageHeaderAccessor.getId(message);
		StringBuilder info = new StringBuilder("Processing message ").append(springId).append(" <");

		if (!(message instanceof ErrorMessage errorMessage)) {
			throw new IllegalArgumentException(String.format("Spring message %s: Expected an %s, but got a %s",
					springId, ErrorMessage.class.getSimpleName(), message.getClass().getSimpleName()));
		}

		Set<AcknowledgmentCallback> acknowledgmentCallbacks = new HashSet<>();

		Optional.ofNullable(StaticMessageHeaderAccessor.getAcknowledgmentCallback(message))
				.ifPresent(acknowledgmentCallbacks::add);

		if (errorMessage.getPayload() instanceof MessagingException messagingException) {
			Optional.ofNullable(messagingException.getFailedMessage())
					.map(m -> {
						info.append("messaging-exception-message: ").append(StaticMessageHeaderAccessor.getId(m)).append(", ");
						return m;
					})
					.map(StaticMessageHeaderAccessor::getAcknowledgmentCallback)
					.ifPresent(acknowledgmentCallbacks::add);
		}

		Optional.ofNullable(errorMessage.getOriginalMessage())
				.map(m -> {
					info.append("original-message: ").append(StaticMessageHeaderAccessor.getId(m)).append(", ");
					return m;
				})
				.map(StaticMessageHeaderAccessor::getAcknowledgmentCallback)
				.ifPresent(acknowledgmentCallbacks::add);

		if (StaticMessageHeaderAccessor.getSourceData(message) instanceof XMLMessage xmlMessage) {
			info.append("source-jcsmp-message: ").append(xmlMessage.getMessageId()).append(", ");
		}

		LOGGER.info(info.append('>').toString());

		if (acknowledgmentCallbacks.isEmpty()) {
			// Should never happen under normal use
			throw new IllegalArgumentException(String.format(
					"Spring message %s does not contain an acknowledgment callback. Message cannot be acknowledged",
					springId));
		}

		for(AcknowledgmentCallback acknowledgmentCallback : acknowledgmentCallbacks) {
			try {
				if (!SolaceAckUtil.republishToErrorQueue(acknowledgmentCallback)) {
					AckUtils.requeue(acknowledgmentCallback);
				}
			} catch (SolaceAcknowledgmentException e) {
				LOGGER.error("Spring message {}: exception in error handler", springId, e);
				throw e;
			}
		}
	}
}
