package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;

public class ErrorChannelSendingCorrelationKey {
	private final Message<?> inputMessage;
	private final MessageChannel errorChannel;
	private final ErrorMessageStrategy errorMessageStrategy;
	private XMLMessage rawMessage;

	private static final Log logger = LogFactory.getLog(ErrorChannelSendingCorrelationKey.class);

	public ErrorChannelSendingCorrelationKey(Message<?> inputMessage, MessageChannel errorChannel,
											 ErrorMessageStrategy errorMessageStrategy) {
		this.inputMessage = inputMessage;
		this.errorChannel = errorChannel;
		this.errorMessageStrategy = errorMessageStrategy;
	}

	public Message<?> getInputMessage() {
		return inputMessage;
	}

	public XMLMessage getRawMessage() {
		return rawMessage;
	}

	public void setRawMessage(XMLMessage rawMessage) {
		this.rawMessage = rawMessage;
	}

	/**
	 * Send the message to the error channel if defined.
	 * @param msg the failure description
	 * @param cause the failure cause
	 * @return the exception wrapper containing the failed input message
	 */
	public MessagingException send(String msg, Exception cause) {
		MessagingException exception = new MessagingException(inputMessage, msg, cause);
		if (errorChannel != null) {
			AttributeAccessor attributes = ErrorMessageUtils.getAttributeAccessor(inputMessage, null);
			if (rawMessage != null) {
				attributes.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_RAW_MESSAGE, rawMessage);
			}
			logger.debug(String.format("Sending message %s to error channel %s", inputMessage.getHeaders().getId(),
					errorChannel));
			errorChannel.send(errorMessageStrategy.buildErrorMessage(exception, attributes));
		}
		return exception;
	}
}
