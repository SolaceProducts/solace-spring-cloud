package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solacesystems.jcsmp.XMLMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;

import java.util.List;

public class ErrorChannelSendingCorrelationKey {
	private final Message<?> inputMessage;
	private final MessageChannel errorChannel;
	private final ErrorMessageStrategy errorMessageStrategy;
	private List<XMLMessage> rawMessages;
	private CorrelationData confirmCorrelation;

	private static final Logger LOGGER = LoggerFactory.getLogger(ErrorChannelSendingCorrelationKey.class);

	public ErrorChannelSendingCorrelationKey(Message<?> inputMessage, MessageChannel errorChannel,
											 ErrorMessageStrategy errorMessageStrategy) {
		this.inputMessage = inputMessage;
		this.errorChannel = errorChannel;
		this.errorMessageStrategy = errorMessageStrategy;
	}

	public Message<?> getInputMessage() {
		return inputMessage;
	}

	public List<XMLMessage> getRawMessages() {
		return rawMessages;
	}

	public void setRawMessages(List<XMLMessage> rawMessages) {
		this.rawMessages = rawMessages;
	}

	public CorrelationData getConfirmCorrelation() {
		return confirmCorrelation;
	}

	public void setConfirmCorrelation(CorrelationData confirmCorrelation) {
		this.confirmCorrelation = confirmCorrelation;
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
			if (rawMessages != null && !rawMessages.isEmpty()) {
				attributes.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_RAW_MESSAGE,
						inputMessage.getHeaders().containsKey(SolaceBinderHeaders.BATCHED_HEADERS) ?
								rawMessages : rawMessages.get(0));
			}
			LOGGER.debug("Sending message {} to error channel {}", inputMessage.getHeaders().getId(), errorChannel);
			errorChannel.send(errorMessageStrategy.buildErrorMessage(exception, attributes));
		}
		return exception;
	}
}
