package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;

import java.util.Optional;
import java.util.UUID;

public class JCSMPSessionProducerManager extends SharedResourceManager<XMLMessageProducer> {
	private final JCSMPSession session;
	private final CloudStreamEventHandler publisherEventHandler = new CloudStreamEventHandler();

	private static final Log logger = LogFactory.getLog(JCSMPSessionProducerManager.class);

	public JCSMPSessionProducerManager(JCSMPSession session) {
		super("producer");
		this.session = session;
	}

	@Override
	XMLMessageProducer create() throws JCSMPException {
		return session.getMessageProducer(publisherEventHandler);
	}

	@Override
	void close() {
		sharedResource.close();
	}

	static class CloudStreamEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

		@Override
		public void responseReceivedEx(Object correlationKey) {
			if (correlationKey instanceof ErrorChannelSendingCorrelationKey) {
				ErrorChannelSendingCorrelationKey key = (ErrorChannelSendingCorrelationKey) correlationKey;
				logger.debug("Producer received response for message " + StaticMessageHeaderAccessor.getId(key.getInputMessage()));
				if (key.getConfirmCorrelation() != null) {
					key.getConfirmCorrelation().success();
				}
			} else if (correlationKey instanceof ErrorQueueRepublishCorrelationKey) {
				ErrorQueueRepublishCorrelationKey key = (ErrorQueueRepublishCorrelationKey) correlationKey;
				try {
					key.handleSuccess();
				} catch (SolaceStaleMessageException e) { // unlikely to happen
					logger.warn(String.format("Message %s successfully sent to error queue %s, " +
									"but the reference is now stale. Message is likely duplicated and was/will be" +
									" redelivered on the original queue.",
							key.getSourceMessageId(), key.getErrorQueueName()), e);
				}
			} else {
				logger.debug("Producer received response for correlation key: " + correlationKey);
			}
		}

		@SuppressWarnings("ThrowableNotThrown")
		@Override
		public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
			if (correlationKey instanceof ErrorChannelSendingCorrelationKey) {
				ErrorChannelSendingCorrelationKey key = (ErrorChannelSendingCorrelationKey) correlationKey;
				String messageId = key.getRawMessage() != null ? key.getRawMessage().getMessageId() : null;
				UUID springMessageId = Optional.ofNullable(key.getInputMessage())
						.map(Message::getHeaders)
						.map(MessageHeaders::getId)
						.orElse(null);
				String msg = String.format("Producer received error for message %s (Spring message %s) at %s",
						messageId, springMessageId, timestamp);
				logger.warn(msg, cause);
				MessagingException messagingException = key.send(msg, cause);

				if (key.getConfirmCorrelation() != null) {
					key.getConfirmCorrelation().failed(messagingException);
				}
			} else if (correlationKey instanceof ErrorQueueRepublishCorrelationKey) {
				ErrorQueueRepublishCorrelationKey key = (ErrorQueueRepublishCorrelationKey) correlationKey;
				try {
					key.handleError(true);
				} catch (SolaceStaleMessageException e) { // unlikely to happen
					logger.warn(String.format("Cannot republish message %s to error queue %s. " +
									"It was/will be redelivered on the original queue",
							key.getSourceMessageId(), key.getErrorQueueName()), e);
				}
			} else {
				logger.warn(String.format("Producer received error for correlation key: %s at %s", correlationKey,
						timestamp), cause);
			}
		}
	}
}
