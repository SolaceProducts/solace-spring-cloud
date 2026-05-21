package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.StaleSessionException;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessagingException;

public final class ErrorQueueInfrastructure {
	private final JCSMPSessionProducerManager producerManager;
	private final String producerKey;
	private final String errorQueueName;
	private final SolaceConsumerProperties consumerProperties;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final Logger LOGGER = LoggerFactory.getLogger(ErrorQueueInfrastructure.class);

	public ErrorQueueInfrastructure(JCSMPSessionProducerManager producerManager, String producerKey,
									String errorQueueName, SolaceConsumerProperties consumerProperties) {
		this.producerManager = producerManager;
		this.producerKey = producerKey;
		this.errorQueueName = errorQueueName;
		this.consumerProperties = consumerProperties;
	}

	public void send(MessageContainer messageContainer, ErrorQueueRepublishCorrelationKey key) throws JCSMPException {
		XMLMessage xmlMessage = xmlMessageMapper.mapError(messageContainer.getMessage(), consumerProperties);
		xmlMessage.setCorrelationKey(key);
		Queue queue = JCSMPFactory.onlyInstance().createQueue(errorQueueName);
		XMLMessageProducer producer;
		try {
			producer = producerManager.get(producerKey);
			if (producer.isClosed()) {
				LOGGER.warn("Detected closed shared JCSMP producer before sending to error queue {}; recreating",
						errorQueueName);
				producer = producerManager.forceRecreate(producer);
			}
		} catch (Exception e) {
			MessagingException wrappedException = new MessagingException(
					String.format("Failed to get producer to send message %s to queue %s", xmlMessage.getMessageId(),
							errorQueueName), e);
			LOGGER.warn(wrappedException.getMessage(), e);
			throw wrappedException;
		}

		try {
			producer.send(xmlMessage, queue);
		} catch (JCSMPException e) {
			if (e instanceof StaleSessionException
					|| e instanceof JCSMPTransportException
					|| e instanceof ClosedFacilityException
					|| producer.isClosed()) {
				LOGGER.debug("Detected stale shared JCSMP producer while sending to error queue {} (cause: {}); " +
								"recreating for next attempt",
						errorQueueName, e.getClass().getSimpleName());
				try {
					producerManager.forceRecreate(producer);
				} catch (Exception recreateError) {
					LOGGER.warn("Failed to recreate shared JCSMP producer after stale-flow detection", recreateError);
					e.addSuppressed(recreateError);
				}
			}
			throw e;
		}
	}

	public ErrorQueueRepublishCorrelationKey createCorrelationKey(MessageContainer messageContainer,
																  FlowReceiverContainer flowReceiverContainer) {
		return new ErrorQueueRepublishCorrelationKey(this, messageContainer, flowReceiverContainer);
	}

	public String getErrorQueueName() {
		return errorQueueName;
	}

	public long getMaxDeliveryAttempts() {
		return consumerProperties.getErrorQueueMaxDeliveryAttempts();
	}
}
