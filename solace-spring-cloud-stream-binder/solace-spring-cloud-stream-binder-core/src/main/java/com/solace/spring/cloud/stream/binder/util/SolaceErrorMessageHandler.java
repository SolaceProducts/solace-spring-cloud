package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.inbound.acknowledge.SolaceAckUtil;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
		UUID springId = StaticMessageHeaderAccessor.getId(message);
		StringBuilder info = new StringBuilder("Processing message ").append(springId).append(" <");

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

		if (failedMsg != null) {
			info.append("failed-message: ").append(StaticMessageHeaderAccessor.getId(failedMsg)).append(", ");
		}

		Object sourceData = StaticMessageHeaderAccessor.getSourceData(message);
		if (sourceData instanceof XMLMessage) {
			info.append("source-message: ").append(((XMLMessage) sourceData).getMessageId()).append(", ");
		}

		logger.info(info.append('>'));

		AcknowledgmentCallback acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(message);
		if (acknowledgmentCallback == null && failedMsg != null) {
			acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(failedMsg);
		}

		if (acknowledgmentCallback == null) {
			// Should never happen under normal use
			logger.warn(String.format(
					"Spring message %s does not contain an acknowledgment callback. Message cannot be acknowledged",
					springId));
			return;
		}

		try {
			if (!SolaceAckUtil.republishToErrorQueue(acknowledgmentCallback)) {
				AckUtils.requeue(acknowledgmentCallback);
			}
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
