package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.messaging.MessagingException;

public class ErrorQueueInfrastructure {
	private final JCSMPSessionProducerManager producerManager;
	private final String producerKey;
	private final String errorQueueName;
	private final SolaceConsumerProperties consumerProperties;
	private final RetryableTaskService retryableTaskService;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final Log logger = LogFactory.getLog(ErrorQueueInfrastructure.class);

	public ErrorQueueInfrastructure(JCSMPSessionProducerManager producerManager, String producerKey,
									String errorQueueName, SolaceConsumerProperties consumerProperties,
									RetryableTaskService retryableTaskService) {
		this.producerManager = producerManager;
		this.producerKey = producerKey;
		this.errorQueueName = errorQueueName;
		this.consumerProperties = consumerProperties;
		this.retryableTaskService = retryableTaskService;
	}

	public void send(MessageContainer messageContainer, ErrorQueueRepublishCorrelationKey key) throws JCSMPException {
		XMLMessage xmlMessage = xmlMessageMapper.mapError(messageContainer.getMessage(), consumerProperties);
		xmlMessage.setCorrelationKey(key);
		Queue queue = JCSMPFactory.onlyInstance().createQueue(errorQueueName);
		XMLMessageProducer producer;
		try {
			producer = producerManager.get(producerKey);
		} catch (Exception e) {
			String msg = String.format("Failed to get producer to send message %s to queue %s",
					xmlMessage.getMessageId(), errorQueueName);
			logger.warn(msg, e);
			throw new MessagingException(msg, e);
		}

		producer.send(xmlMessage, queue);
	}

	public ErrorQueueRepublishCorrelationKey createCorrelationKey(MessageContainer messageContainer,
																  FlowReceiverContainer flowReceiverContainer,
																  boolean hasTemporaryQueue) {
		return new ErrorQueueRepublishCorrelationKey(this, messageContainer, flowReceiverContainer,
				hasTemporaryQueue, retryableTaskService);
	}

	public String getErrorQueueName() {
		return errorQueueName;
	}

	public long getMaxDeliveryAttempts() {
		return consumerProperties.getErrorQueueMaxDeliveryAttempts();
	}
}
