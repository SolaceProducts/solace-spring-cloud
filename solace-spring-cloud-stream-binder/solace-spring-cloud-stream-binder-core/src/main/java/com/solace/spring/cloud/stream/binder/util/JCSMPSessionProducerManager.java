package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;

import java.util.Optional;
import java.util.UUID;

public class JCSMPSessionProducerManager extends SharedResourceManager<XMLMessageProducer> {
	private final SolaceSessionManager solaceSessionManager;
	private final CloudStreamEventHandler publisherEventHandler = new CloudStreamEventHandler();

	private static final Logger LOGGER = LoggerFactory.getLogger(JCSMPSessionProducerManager.class);

	public JCSMPSessionProducerManager(SolaceSessionManager solaceSessionManager) {
		super("producer");
		this.solaceSessionManager = solaceSessionManager;
	}

	@Override
	XMLMessageProducer create() throws JCSMPException {
		return solaceSessionManager.getSession().getMessageProducer(publisherEventHandler);
	}

	@Override
	void close() {
		sharedResource.close();
	}

	public static class CloudStreamEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

		@Override
		public void responseReceivedEx(Object correlationKey) {
			if (correlationKey instanceof BatchProxyCorrelationKey batchProxyCorrelationKey) {
				correlationKey = batchProxyCorrelationKey.getCorrelationKeyForSuccess();
			}

			if (correlationKey instanceof ErrorChannelSendingCorrelationKey key) {
				LOGGER.trace("Producer received response for message {}",
						StaticMessageHeaderAccessor.getId(key.getInputMessage()));
				if (key.getConfirmCorrelation() != null) {
					key.getConfirmCorrelation().success();
				}
			} else if (correlationKey instanceof ErrorQueueRepublishCorrelationKey key) {
				try {
					key.handleSuccess();
				} catch (SolaceAcknowledgmentException e) { // unlikely to happen
					LOGGER.warn("Message {} successfully sent to error queue {}, but failed to acknowledge consumer message. Message is likely duplicated and was/will be redelivered on the original queue.",
							key.getSourceMessageId(), key.getErrorQueueName(), e);
					throw e;
				}
			} else {
				LOGGER.trace("Producer received response for correlation key: {}", correlationKey);
			}
		}

		@Override
		public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
			if (correlationKey instanceof BatchProxyCorrelationKey batchProxyCorrelationKey) {
				correlationKey = batchProxyCorrelationKey.getCorrelationKeyForFailure();
			}

			if (correlationKey instanceof ErrorChannelSendingCorrelationKey key) {
				UUID springMessageId = Optional.ofNullable(key.getInputMessage())
						.map(Message::getHeaders)
						.map(MessageHeaders::getId)
						.orElse(null);
				String msg = String.format("Producer received error during publishing (Spring message %s) at %s",
						springMessageId, timestamp);
				LOGGER.warn(msg, cause);
				MessagingException messagingException = key.send(msg, cause);

				if (key.getConfirmCorrelation() != null) {
					key.getConfirmCorrelation().failed(messagingException);
				}
			} else if (correlationKey instanceof ErrorQueueRepublishCorrelationKey key) {
				try {
					key.handleError();
				} catch (SolaceAcknowledgmentException e) { // unlikely to happen
					LOGGER.warn("Cannot republish message {} to error queue {}. It was/will be redelivered on the original queue",
							key.getSourceMessageId(), key.getErrorQueueName(), e);
					throw e;
				}
			} else {
				LOGGER.warn("Producer received error for correlation key: {} at {}", correlationKey, timestamp, cause);
			}
		}
	}
}
