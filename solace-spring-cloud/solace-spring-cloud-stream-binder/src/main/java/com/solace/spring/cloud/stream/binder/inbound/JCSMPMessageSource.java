package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.ClosedChannelBindingException;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.messaging.MessagingException;

import java.util.UUID;
import java.util.function.Consumer;

public class JCSMPMessageSource extends AbstractMessageSource<Object> implements Lifecycle {
	private final String id = UUID.randomUUID().toString();
	private final String queueName;
	private final JCSMPSession jcsmpSession;
	private final EndpointProperties endpointProperties;
	private final Consumer<Queue> postStart;
	private ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties;
	private FlowReceiver consumerFlowReceiver;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private boolean isRunning = false;

	public JCSMPMessageSource(ConsumerDestination destination,
							  JCSMPSession jcsmpSession,
							  ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
							  EndpointProperties endpointProperties,
							  Consumer<Queue> postStart) {
		this.queueName = destination.getName();
		this.jcsmpSession = jcsmpSession;
		this.consumerProperties = consumerProperties;
		this.endpointProperties = endpointProperties;
		this.postStart = postStart;
	}

	@Override
	protected Object doReceive() {
		if (!isRunning()) {
			String msg0 = String.format("Cannot receive message using message source %s", id);
			String msg1 = String.format("Message source %s is not running", id);
			ClosedChannelBindingException closedBindingException = new ClosedChannelBindingException(msg1);
			logger.warn(msg0, closedBindingException);
			throw new MessagingException(msg0, closedBindingException);
		}

		BytesXMLMessage xmlMessage;
		try {
			int timeout = consumerProperties.getExtension().getPolledConsumerWaitTimeInMillis();
			xmlMessage = consumerFlowReceiver.receive(timeout);
		} catch (JCSMPException e) {
			if (!isRunning()) {
				logger.warn(String.format("Exception received while consuming a message, but the consumer " +
						"<message source ID: %s> is currently shutdown. Exception will be ignored", id), e);
				return null;
			} else {
				String msg = String.format("Unable to consume message from queue %s", queueName);
				logger.warn(msg, e);
				throw new MessagingException(msg, e);
			}
		}

		return xmlMessage != null ? xmlMessageMapper.map(xmlMessage, true) : null;
	}

	@Override
	public String getComponentType() {
		return "jcsmp:message-source";
	}

	@Override
	public void start() {
		logger.info(String.format("Creating consumer to queue %s <message source ID: %s>", queueName, id));
		if (isRunning()) {
			logger.warn(String.format("Nothing to do, message source %s is already running", id));
			return;
		}

		Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		try {
			final ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
			flowProperties.setEndpoint(queue);
			flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
			consumerFlowReceiver = jcsmpSession.createFlow(null, flowProperties, endpointProperties);
			consumerFlowReceiver.start();
		} catch (JCSMPException e) {
			String msg = String.format("Unable to get a message consumer for session %s", jcsmpSession.getSessionName());
			logger.warn(msg, e);
			throw new RuntimeException(msg, e);
		}

		if (postStart != null) {
			postStart.accept(queue);
		}

		isRunning = true;
	}

	@Override
	public void stop() {
		if (!isRunning()) return;
		logger.info(String.format("Stopping consumer to queue %s <message source ID: %s>", queueName, id));
		consumerFlowReceiver.close();
		isRunning = false;
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}
}
