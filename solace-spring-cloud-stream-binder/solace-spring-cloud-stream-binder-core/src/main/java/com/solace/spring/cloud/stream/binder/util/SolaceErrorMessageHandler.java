package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;

import java.util.UUID;

public class SolaceErrorMessageHandler implements MessageHandler {

	private static final Log logger = LogFactory.getLog(SolaceErrorMessageHandler.class);

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		XMLMessage rawMessage = (XMLMessage) message.getHeaders().get(SolaceMessageHeaderErrorMessageStrategy.SOLACE_RAW_MESSAGE);
		UUID springId = message.getHeaders().getId();
		if (!(message instanceof ErrorMessage)) {
			logger.warn(String.format("Spring message %s: Expected an %s, not a %s",
					springId, ErrorMessage.class.getSimpleName(), message.getClass().getSimpleName()));
			return;
		} else if (rawMessage == null) {
			logger.warn(String.format("Spring message %s: No header %s",
					springId, SolaceMessageHeaderErrorMessageStrategy.SOLACE_RAW_MESSAGE));
			return;
		}

		Object payload = message.getPayload();
		if (!(payload instanceof MessagingException)) {
			logger.warn(String.format("Spring message %s: Message payload is a %s, not a %s. Nothing to process",
					springId, payload.getClass().getSimpleName(), MessagingException.class.getSimpleName()));
			return;
		}

		Message<?> failedMsg = ((MessagingException) payload).getFailedMessage();
		if (failedMsg == null) {
			logger.warn(String.format("Spring message %s: %s does not have an attached %s, no message to process",
					springId, MessagingException.class.getSimpleName(), Message.class.getSimpleName()));
			return;
		}

		UUID failedSpringId = failedMsg.getHeaders().getId();
		logger.info(String.format("Spring message %s contains failed Spring message %s", springId, failedSpringId));
		logger.info(String.format("Spring message %s contains raw %s %s",
				springId, XMLMessage.class.getSimpleName(), rawMessage.getMessageId()));
		AcknowledgmentCallback acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(failedMsg);
		if (acknowledgmentCallback == null) { // Should never happen under normal use
			logger.warn(String.format("Failed Spring message %s: No header %s, message cannot be acknowledged",
					failedSpringId, IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK));
			return;
		}

		AckUtils.reject(acknowledgmentCallback);
	}
}
