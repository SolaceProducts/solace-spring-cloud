package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.messaging.MessagingException;

public class ErrorQueueInfrastructure {
	private final JCSMPSessionProducerManager producerManager;
	private final String producerKey;
	private final String errorQueueName;
	private final SolaceConsumerProperties consumerProperties;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final Log logger = LogFactory.getLog(ErrorQueueInfrastructure.class);

	public ErrorQueueInfrastructure(JCSMPSessionProducerManager producerManager, String producerKey,
									String errorQueueName, SolaceConsumerProperties consumerProperties) {
		this.producerManager = producerManager;
		this.producerKey = producerKey;
		this.errorQueueName = errorQueueName;
		this.consumerProperties = consumerProperties;
	}

	public void send(BytesXMLMessage message) {
		XMLMessage xmlMessage = xmlMessageMapper.mapError(message, consumerProperties);
		try {
			Queue queue = JCSMPFactory.onlyInstance().createQueue(errorQueueName);
			producerManager.get(producerKey).send(xmlMessage, queue);
		} catch (Exception e) {
			String msg = String.format("Failed to send message %s to queue %s", xmlMessage.getMessageId(),
					errorQueueName);
			logger.warn(msg, e);
			throw new MessagingException(msg, e);
		}
	}

	public String getErrorQueueName() {
		return errorQueueName;
	}
}
