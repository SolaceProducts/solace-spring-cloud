package com.solace.spring.cloud.stream.binder.outbound;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
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
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	@Nullable private TransactedSession transactedSession;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private boolean isRunning = false;
	private ErrorMessageStrategy errorMessageStrategy;

	private static final Logger LOGGER = LoggerFactory.getLogger(JCSMPOutboundMessageHandler.class);

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

		LOGGER.debug("Publishing message to destination [ {}:{} ]",
				targetDestination instanceof Topic ? "TOPIC" : "QUEUE", targetDestination);

		try {
			producer.send(xmlMessage, targetDestination);
			if (transactedSession != null) {
				LOGGER.debug("Committing transaction <message handler ID: {}>", id);
				transactedSession.commit();
			}
		} catch (JCSMPException e) {
			if (transactedSession != null && !(e instanceof RollbackException)) {
				try {
					LOGGER.debug("Rolling back transaction <message handler ID: {}>", id);
					transactedSession.rollback();
				} catch (JCSMPException ex) {
					LOGGER.debug("Failed to rollback transaction", ex);
					e.addSuppressed(ex);
				}
			}

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
		LOGGER.info("Creating producer to {} {} <message handler ID: {}>", configDestinationType, configDestination.getName(), id);
		if (isRunning()) {
			LOGGER.warn("Nothing to do, message handler {} is already running", id);
			return;
		}

		try {
			producerManager.get(id);
			if (properties.getExtension().isTransacted()) {
				LOGGER.info("Creating transacted session  <message handler ID: {}>", id);
				transactedSession = jcsmpSession.createTransactedSession();
				producer = transactedSession.createProducer(SolaceProvisioningUtil.getProducerFlowProperties(jcsmpSession),
						new JCSMPSessionProducerManager.CloudStreamEventHandler());
			} else {
				producer = jcsmpSession.createProducer(SolaceProvisioningUtil.getProducerFlowProperties(jcsmpSession),
						new JCSMPSessionProducerManager.CloudStreamEventHandler());
			}
		} catch (Exception e) {
			String msg = String.format("Unable to get a message producer for session %s", jcsmpSession.getSessionName());
			LOGGER.warn(msg, e);
			closeResources();
			throw new RuntimeException(msg, e);
		}

		isRunning = true;
	}

	@Override
	public void stop() {
		if (!isRunning()) return;
		closeResources();
		isRunning = false;
	}

	private void closeResources() {
		LOGGER.info("Stopping producer to {} {} <message handler ID: {}>", configDestinationType, configDestination.getName(), id);
		if (producer != null) {
			LOGGER.info("Closing producer <message handler ID: {}>", id);
			producer.close();
		}
		if (transactedSession != null) {
			LOGGER.info("Closing transacted session <message handler ID: {}>", id);
			transactedSession.close();
		}
		producerManager.release(id);
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
		LOGGER.warn(msg, e);
		return key.send(msg, e);
	}
}
