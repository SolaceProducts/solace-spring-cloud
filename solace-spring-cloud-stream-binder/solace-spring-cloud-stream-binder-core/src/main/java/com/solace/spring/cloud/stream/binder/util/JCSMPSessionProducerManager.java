package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Optional;
import java.util.UUID;

public class JCSMPSessionProducerManager extends SharedResourceManager<XMLMessageProducer> {
	private final JCSMPSession session;

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

	private final JCSMPStreamingPublishCorrelatingEventHandler publisherEventHandler =
			new JCSMPStreamingPublishCorrelatingEventHandler() {

		@Override
		public void responseReceivedEx(Object correlationKey) {
			logger.debug("Producer received response for correlation key: " + correlationKey);
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
				key.send(msg, cause);
			} else {
				logger.warn(String.format("Producer received error for correlation key: %s at %s", correlationKey,
						timestamp), cause);
			}
		}
	};
}
