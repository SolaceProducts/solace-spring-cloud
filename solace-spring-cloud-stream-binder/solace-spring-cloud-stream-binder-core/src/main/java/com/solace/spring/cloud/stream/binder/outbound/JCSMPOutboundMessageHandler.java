package com.solace.spring.cloud.stream.binder.outbound;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.util.ClosedChannelBindingException;
import com.solace.spring.cloud.stream.binder.util.CorrelationData;
import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.spring.cloud.stream.binder.util.ErrorChannelSendingCorrelationKey;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.util.StringUtils;

import java.util.UUID;

public class JCSMPOutboundMessageHandler implements MessageHandler, Lifecycle {
	private final String id = UUID.randomUUID().toString();
	private final DestinationType configDestinationType;
	private final Destination configDestination;
	private final JCSMPSession jcsmpSession;
	private final MessageChannel errorChannel;
	private final JCSMPSessionProducerManager producerManager;
	private final ExtendedProducerProperties<SolaceProducerProperties> properties;
	@Nullable private final SolaceMeterAccessor solaceMeterAccessor;
	private XMLMessageProducer producer;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private boolean isRunning = false;
	private ErrorMessageStrategy errorMessageStrategy;

	private static final Log logger = LogFactory.getLog(JCSMPOutboundMessageHandler.class);

	public JCSMPOutboundMessageHandler(ProducerDestination destination,
									   JCSMPSession jcsmpSession,
									   MessageChannel errorChannel,
									   JCSMPSessionProducerManager producerManager,
									   ExtendedProducerProperties<SolaceProducerProperties> properties,
									   @Nullable SolaceMeterAccessor solaceMeterAccessor) {
		this.configDestinationType = properties.getExtension().getDestinationType();
		this.configDestination = configDestinationType == DestinationType.TOPIC ?
				JCSMPFactory.onlyInstance().createTopic(destination.getName()) :
				JCSMPFactory.onlyInstance().createQueue(destination.getName());
		this.jcsmpSession = jcsmpSession;
		this.errorChannel = errorChannel;
		this.producerManager = producerManager;
		this.properties = properties;
		this.solaceMeterAccessor = solaceMeterAccessor;
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

		Destination targetDestination = checkDynamicDestination(message, correlationKey);
		if (targetDestination == null) {
			targetDestination = configDestination;
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

		XMLMessage xmlMessage = xmlMessageMapper.map(message, properties.getExtension().getHeaderExclusions(),
				properties.getExtension().isNonserializableHeaderConvertToString());
		correlationKey.setRawMessage(xmlMessage);
		xmlMessage.setCorrelationKey(correlationKey);

		try {
			producer.send(xmlMessage, targetDestination);
		} catch (JCSMPException e) {
			throw handleMessagingException(correlationKey,
					String.format("Unable to send message to destination %s %s",
							targetDestination instanceof Topic ? "TOPIC" : "QUEUE", targetDestination.getName()), e);
		} finally {
			if (solaceMeterAccessor != null) {
				solaceMeterAccessor.recordMessage(properties.getBindingName(), xmlMessage);
			}
		}
	}

	private Destination checkDynamicDestination(Message<?> message, ErrorChannelSendingCorrelationKey correlationKey) {
		try {
			String dynamicDestName;
			String targetDestinationHeader = message.getHeaders().get(BinderHeaders.TARGET_DESTINATION, String.class);
			if (StringUtils.hasText(targetDestinationHeader)) {
				dynamicDestName = targetDestinationHeader.trim();
			} else {
				return null;
			}

			String targetDestinationTypeHeader = message.getHeaders().get(SolaceBinderHeaders.TARGET_DESTINATION_TYPE, String.class);
			if (StringUtils.hasText(targetDestinationTypeHeader)) {
				targetDestinationTypeHeader = targetDestinationTypeHeader.trim().toUpperCase();
				if (targetDestinationTypeHeader.equals(DestinationType.TOPIC.name())) {
					return JCSMPFactory.onlyInstance().createTopic(dynamicDestName);
				} else if (targetDestinationTypeHeader.equals(DestinationType.QUEUE.name())) {
					return JCSMPFactory.onlyInstance().createQueue(dynamicDestName);
				} else {
					throw new IllegalArgumentException(String.format("Incorrect value specified for header '%s'. Expected [ %s|%s ] but actual value is [ %s ]",
							SolaceBinderHeaders.TARGET_DESTINATION_TYPE, DestinationType.TOPIC.name(), DestinationType.QUEUE.name(), targetDestinationTypeHeader));
				}
			}

			//No dynamic destinationType present so use configured destinationType
			return configDestinationType == DestinationType.TOPIC ?
					JCSMPFactory.onlyInstance().createTopic(dynamicDestName) :
					JCSMPFactory.onlyInstance().createQueue(dynamicDestName);
		} catch (Exception e) {
			throw handleMessagingException(correlationKey, "Unable to parse headers", e);
		}
	}

	@Override
	public void start() {
		logger.info(String.format("Creating producer to %s %s <message handler ID: %s>", configDestinationType, configDestination.getName(), id));
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
		logger.info(String.format("Stopping producer to %s %s <message handler ID: %s>", configDestinationType, configDestination.getName(), id));
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
