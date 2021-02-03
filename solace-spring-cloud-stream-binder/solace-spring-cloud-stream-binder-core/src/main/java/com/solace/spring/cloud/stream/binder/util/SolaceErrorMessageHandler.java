package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
		UUID springId = message.getHeaders().getId();
		if (!(message instanceof ErrorMessage)) {
			logger.warn(String.format("Spring message %s: Expected an %s, not a %s",
					springId, ErrorMessage.class.getSimpleName(), message.getClass().getSimpleName()));
			return;
		}

		ErrorMessage errorMessage = (ErrorMessage) message;
		Throwable payload = errorMessage.getPayload();

		if (ExceptionUtils.indexOfType(payload, SolaceStaleMessageException.class) > -1) {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Spring message %s: Message is stale, nothing to do", springId), payload);
			} else {
				logger.info(String.format("Spring message %s: Message is stale, nothing to do", springId));
			}
			return;
		}

		Message<?> failedMsg;
		if (payload instanceof MessagingException && ((MessagingException) payload).getFailedMessage() != null) {
			failedMsg = ((MessagingException) payload).getFailedMessage();
		} else {
			failedMsg = errorMessage.getOriginalMessage();
		}

		if (failedMsg == null) {
			logger.warn(String.format("Spring message %s: Does not have an attached %s, nothing to process",
					springId, Message.class.getSimpleName()));
			return;
		}

		UUID failedSpringId = failedMsg.getHeaders().getId();
		logger.info(String.format("Spring message %s contains failed Spring message %s", springId, failedSpringId));

		if (message.getHeaders().containsKey(SolaceBinderHeaders.RAW_MESSAGE)) {
			XMLMessage rawMessage = (XMLMessage) message.getHeaders().get(SolaceBinderHeaders.RAW_MESSAGE);
			if (rawMessage != null) {
				logger.info(String.format("Spring message %s contains raw %s %s",
						springId, XMLMessage.class.getSimpleName(), rawMessage.getMessageId()));
			}
		}

		AcknowledgmentCallback acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(failedMsg);
		if (acknowledgmentCallback == null) { // Should never happen under normal use
			logger.warn(String.format("Failed Spring message %s: No header %s, message cannot be acknowledged",
					failedSpringId, IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK));
			return;
		}

		try {
			AckUtils.reject(acknowledgmentCallback);
		} catch (SolaceAcknowledgmentException e) {
			if (ExceptionUtils.indexOfType(e, SolaceStaleMessageException.class) > -1) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Spring message %s: Message is stale, nothing to do", springId), payload);
				} else {
					logger.info(String.format("Spring message %s: Message is stale, nothing to do", springId));
				}
			} else {
				throw e;
			}
		}
	}
}
