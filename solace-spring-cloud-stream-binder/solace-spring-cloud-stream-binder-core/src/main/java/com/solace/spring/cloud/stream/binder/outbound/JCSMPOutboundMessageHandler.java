package com.solace.spring.cloud.stream.binder.outbound;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.util.ClosedChannelBindingException;
import com.solace.spring.cloud.stream.binder.util.CorrelationData;
import com.solace.spring.cloud.stream.binder.util.ErrorChannelSendingCorrelationKey;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.util.StringUtils;

import java.util.UUID;

public class JCSMPOutboundMessageHandler implements MessageHandler, Lifecycle {
	private final String id = UUID.randomUUID().toString();
	private final Topic topic;
	private final JCSMPSession jcsmpSession;
	private final MessageChannel errorChannel;
	private final JCSMPSessionProducerManager producerManager;
	private final SolaceProducerProperties properties;
	private XMLMessageProducer producer;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private boolean isRunning = false;
	private ErrorMessageStrategy errorMessageStrategy;

	private static final Log logger = LogFactory.getLog(JCSMPOutboundMessageHandler.class);

	public JCSMPOutboundMessageHandler(ProducerDestination destination,
									   JCSMPSession jcsmpSession,
									   MessageChannel errorChannel,
									   JCSMPSessionProducerManager producerManager,
									   SolaceProducerProperties properties) {
		this.topic = JCSMPFactory.onlyInstance().createTopic(destination.getName());
		this.jcsmpSession = jcsmpSession;
		this.errorChannel = errorChannel;
		this.producerManager = producerManager;
		this.properties = properties;
	}

	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		ErrorChannelSendingCorrelationKey correlationKey = new ErrorChannelSendingCorrelationKey(message,
				errorChannel, errorMessageStrategy);

		if (! isRunning()) {
			String msg0 = String.format("Cannot send message using handler %s", id);
			String msg1 = String.format("Message handler %s is not running", id);
			throw handleMessagingException(correlationKey, msg0, new ClosedChannelBindingException(msg1));
		}

		Topic targetTopic = topic;

		try {
			String targetDestinationHeader = message.getHeaders().get(BinderHeaders.TARGET_DESTINATION, String.class);
			if (StringUtils.hasText(targetDestinationHeader)) {
				targetTopic = JCSMPFactory.onlyInstance().createTopic(targetDestinationHeader);
			}
		} catch (IllegalArgumentException e) {
			throw handleMessagingException(correlationKey,
					String.format("Unable to parse header %s", BinderHeaders.TARGET_DESTINATION), e);
		}

		try {
			CorrelationData correlationData = message.getHeaders().get(SolaceBinderHeaders.CONFIRM_CORRELATION, CorrelationData.class);
			if (correlationData != null) {
				correlationData.setMessage(message);
				correlationKey.setConfirmCorrelation(correlationData);
			}
		} catch (IllegalArgumentException e) {
			throw handleMessagingException(correlationKey,
					String.format("Unable to parse header %s", SolaceBinderHeaders.CONFIRM_CORRELATION), e);
		}

		XMLMessage xmlMessage = xmlMessageMapper.map(message, properties.getHeaderExclusions(),
				properties.isNonserializableHeaderConvertToString());
		correlationKey.setRawMessage(xmlMessage);
		xmlMessage.setCorrelationKey(correlationKey);

		try {
			producer.send(xmlMessage, targetTopic);
		} catch (JCSMPException e) {
			throw handleMessagingException(correlationKey,
					String.format("Unable to send message to topic %s", targetTopic.getName()), e);
		}
	}

	@Override
	public void start() {
		logger.info(String.format("Creating producer to topic %s <message handler ID: %s>", topic.getName(), id));
		if (isRunning()) {
			logger.warn(String.format("Nothing to do, message handler %s is already running", id));
			return;
		}

		try {
			producer = producerManager.get(id);
		} catch (Exception e) {
			String msg = String.format("Unable to get a message producer for session %s", jcsmpSession.getSessionName());
			logger.warn(msg, e);
			throw new RuntimeException(msg, e);
		}

		isRunning = true;
	}

	@Override
	public void stop() {
		if (!isRunning()) return;
		logger.info(String.format("Stopping producer to topic %s <message handler ID: %s>", topic.getName(), id));
		producerManager.release(id);
		isRunning = false;
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	public void setErrorMessageStrategy(ErrorMessageStrategy errorMessageStrategy) {
		this.errorMessageStrategy = errorMessageStrategy;
	}

	private MessagingException handleMessagingException(ErrorChannelSendingCorrelationKey key, String msg, Exception e)
			throws MessagingException {
		logger.warn(msg, e);
		return key.send(msg, e);
	}
}
