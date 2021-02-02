package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.ClosedChannelBindingException;
import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.messaging.MessagingException;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class JCSMPMessageSource extends AbstractMessageSource<Object> implements Lifecycle {
	private final String id = UUID.randomUUID().toString();
	private final String queueName;
	private final JCSMPSession jcsmpSession;
	private final EndpointProperties endpointProperties;
	private final boolean hasTemporaryQueue;
	private final ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties;
	private FlowReceiverContainer flowReceiverContainer;
	private JCSMPAcknowledgementCallbackFactory ackCallbackFactory;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private boolean isRunning = false;
	private ErrorQueueInfrastructure errorQueueInfrastructure;
	private Consumer<Queue> postStart;

	public JCSMPMessageSource(ConsumerDestination destination,
							  JCSMPSession jcsmpSession,
							  ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
							  EndpointProperties endpointProperties,
							  boolean hasTemporaryQueue) {
		this.queueName = destination.getName();
		this.jcsmpSession = jcsmpSession;
		this.consumerProperties = consumerProperties;
		this.endpointProperties = endpointProperties;
		this.hasTemporaryQueue = hasTemporaryQueue;
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

		MessageContainer messageContainer;
		try {
			int timeout = consumerProperties.getExtension().getPolledConsumerWaitTimeInMillis();
			messageContainer = flowReceiverContainer.receive(timeout);
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

		AcknowledgmentCallback acknowledgmentCallback = ackCallbackFactory.createCallback(messageContainer);

		BytesXMLMessage xmlMessage = messageContainer != null ? messageContainer.getMessage() : null;
		return xmlMessage != null ? xmlMessageMapper.map(xmlMessage, acknowledgmentCallback, true) : null;
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

		try {
			flowReceiverContainer = new FlowReceiverContainer(jcsmpSession, queueName, endpointProperties);
			flowReceiverContainer.setRebindWaitTimeout(consumerProperties.getExtension().getFlowPreRebindWaitTimeout(),
					TimeUnit.MILLISECONDS);
			flowReceiverContainer.bind();
		} catch (JCSMPException e) {
			String msg = String.format("Unable to get a message consumer for session %s", jcsmpSession.getSessionName());
			logger.warn(msg, e);
			throw new RuntimeException(msg, e);
		}

		if (postStart != null) {
			postStart.accept(JCSMPFactory.onlyInstance().createQueue(queueName));
		}

		ackCallbackFactory = new JCSMPAcknowledgementCallbackFactory(flowReceiverContainer, hasTemporaryQueue);
		ackCallbackFactory.setErrorQueueInfrastructure(errorQueueInfrastructure);

		isRunning = true;
	}

	@Override
	public void stop() {
		if (!isRunning()) return;
		logger.info(String.format("Stopping consumer to queue %s <message source ID: %s>", queueName, id));
		flowReceiverContainer.unbind();
		isRunning = false;
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	public void setErrorQueueInfrastructure(ErrorQueueInfrastructure errorQueueInfrastructure) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
	}

	public void setPostStart(Consumer<Queue> postStart) {
		this.postStart = postStart;
	}
}
