package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
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
	private String consumerQueueName;
	private ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties;
	private final String producerKey;
	private final JCSMPSessionProducerManager producerManager;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final Log logger = LogFactory.getLog(SolaceErrorMessageHandler.class);

	public SolaceErrorMessageHandler(ConsumerDestination destination,
									 ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
									 String producerKey,
									 JCSMPSessionProducerManager producerManager) {
		this.consumerQueueName = destination.getName();
		this.consumerProperties = consumerProperties;
		this.producerKey = producerKey;
		this.producerManager = producerManager;
	}

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		XMLMessage rawMessage = (XMLMessage) message.getHeaders().get(SolaceMessageHeaderErrorMessageStrategy.SOLACE_RAW_MESSAGE);
		UUID springId = message.getHeaders().getId();
		if (!(message instanceof ErrorMessage)) {
			logger.warn(String.format("Spring message %s: Expected an %s, not a %s",
					springId, ErrorMessage.class.getSimpleName(), message.getClass().getSimpleName()));
			return;
		} else if (rawMessage == null) {
			logger.warn(String.format("Spring message %s: No header %s",
					springId, SolaceMessageHeaderErrorMessageStrategy.SOLACE_RAW_MESSAGE));
			return;
		}

		Object payload = message.getPayload();
		if (!(payload instanceof MessagingException)) {
			logger.warn(String.format("Spring message %s: Message payload is a %s, not a %s. Nothing to process",
					springId, payload.getClass().getSimpleName(), MessagingException.class.getSimpleName()));
			return;
		}

		Message<?> failedMsg = ((MessagingException) payload).getFailedMessage();
		if (failedMsg == null) {
			logger.warn(String.format("Spring message %s: %s does not have an attached %s, no message to process",
					springId, MessagingException.class.getSimpleName(), Message.class.getSimpleName()));
			return;
		}

		UUID failedSpringId = failedMsg.getHeaders().getId();
		logger.info(String.format("Spring message %s contains failed Spring message %s", springId, failedSpringId));
		logger.info(String.format("Spring message %s contains raw %s %s",
				springId, XMLMessage.class.getSimpleName(), rawMessage.getMessageId()));
		AcknowledgmentCallback acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(failedMsg);
		if (acknowledgmentCallback == null) { // Should never happen under normal use
			logger.warn(String.format("Failed Spring message %s: No header %s, message cannot be acknowledged",
					failedSpringId, IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK));
			return;
		}

		if (consumerProperties.getExtension().isAutoBindDmq()) {
			republishToDMQ(rawMessage);
		} else if (consumerProperties.getExtension().isRequeueRejected()) {
			requeue(rawMessage);
		} else {
			logger.info(String.format("Raw %s %s: Will be rejected",
					XMLMessage.class.getSimpleName(), rawMessage.getMessageId()));
		}

		AckUtils.autoNack(acknowledgmentCallback);
	}

	private void republishToDMQ(XMLMessage rawMessage) {
		String dmqName = SolaceProvisioningUtil.getDMQName(consumerQueueName);
		logger.info(String.format("Raw %s %s: Will be republished to DMQ %s",
				XMLMessage.class.getSimpleName(), rawMessage.getMessageId(), dmqName));
		sendOneMessage(dmqName, xmlMessageMapper.map(rawMessage));
	}

	private void requeue(XMLMessage rawMessage) {
		logger.info(String.format("Raw %s %s: Will be re-queued onto queue %s",
				XMLMessage.class.getSimpleName(), rawMessage.getMessageId(), consumerQueueName));
		sendOneMessage(consumerQueueName, xmlMessageMapper.map(rawMessage));
	}

	private void sendOneMessage(String queueName, Message<?> message) {
		XMLMessage xmlMessage = xmlMessageMapper.map(message, consumerProperties.getExtension());
		try {
			Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
			producerManager.get(producerKey).send(xmlMessage, queue);
		} catch (Exception e) {
			String msg = String.format("Failed to send message %s to queue %s", xmlMessage.getMessageId(), queueName);
			logger.warn(msg, e);
			throw new MessagingException(msg, e);
		}
	}
}
