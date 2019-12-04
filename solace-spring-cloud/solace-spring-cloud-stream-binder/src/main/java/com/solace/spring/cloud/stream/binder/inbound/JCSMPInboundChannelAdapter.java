package com.solace.spring.cloud.stream.binder.inbound;

import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

import java.util.UUID;
import java.util.function.Consumer;

public class JCSMPInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {
	private final String id = UUID.randomUUID().toString();
	private final ConsumerDestination consumerDestination;
	private final JCSMPSession jcsmpSession;
	private final EndpointProperties endpointProperties;
	private final Consumer<Queue> postStart;
	private RetryTemplate retryTemplate;
	private RecoveryCallback<?> recoveryCallback;
	private FlowReceiver consumerFlowReceiver;

	private static final Log logger = LogFactory.getLog(JCSMPInboundChannelAdapter.class);
	private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<>();

	public JCSMPInboundChannelAdapter(ConsumerDestination consumerDestination, JCSMPSession jcsmpSession,
							   @Nullable EndpointProperties endpointProperties, @Nullable Consumer<Queue> postStart) {
		this.consumerDestination = consumerDestination;
		this.jcsmpSession = jcsmpSession;
		this.endpointProperties = endpointProperties;
		this.postStart = postStart;
	}

	@Override
	protected void doStart() {
		final String queueName = consumerDestination.getName();
		logger.info(String.format("Creating consumer flow for queue %s <inbound adapter %s>", queueName, id));

		if (isRunning()) {
			logger.warn(String.format("Nothing to do. Inbound message channel adapter %s is already running", id));
			return;
		}

		XMLMessageListener listener = buildListener();
		Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		try {
			final ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
			flowProperties.setEndpoint(queue);
			flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
			consumerFlowReceiver = jcsmpSession.createFlow(listener, flowProperties, endpointProperties);
			consumerFlowReceiver.start();
		} catch (JCSMPException e) {
			String msg = "Failed to get message consumer from session";
			logger.warn(msg, e);
			throw new MessagingException(msg, e);
		}

		if (postStart != null) {
			postStart.accept(queue);
		}
	}

	@Override
	protected void doStop() {
		if (!isRunning()) return;
		final String queueName = consumerDestination.getName();
		logger.info(String.format("Stopping consumer flow from queue %s <inbound adapter ID: %s>", queueName, id));
		consumerFlowReceiver.close();
	}

	@Override
	public int beforeShutdown() {
		this.stop();
		return 0;
	}

	@Override
	public int afterShutdown() {
		return 0;
	}

	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	public void setRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	protected AttributeAccessor getErrorMessageAttributes(org.springframework.messaging.Message<?> message) {
		AttributeAccessor attributes = attributesHolder.get();
		return attributes == null ? super.getErrorMessageAttributes(message) : attributes;
	}

	private XMLMessageListener buildListener() {
		XMLMessageListener listener;
		if (retryTemplate != null) {
			Assert.state(getErrorChannel() == null,
					"Cannot have an 'errorChannel' property when a 'RetryTemplate' is provided; " +
							"use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to send " +
							"an error message when retries are exhausted");
			RetryableInboundXMLMessageListener retryableMessageListener = new RetryableInboundXMLMessageListener(
					consumerDestination,
					this::sendMessage,
					(exception) -> sendErrorMessageIfNecessary(null, exception),
					retryTemplate,
					recoveryCallback,
					attributesHolder
			);
			retryTemplate.registerListener(retryableMessageListener);
			listener = retryableMessageListener;
		} else {
			listener = new InboundXMLMessageListener(
					consumerDestination,
					this::sendMessage,
					(exception) -> sendErrorMessageIfNecessary(null, exception),
					attributesHolder,
					this.getErrorChannel() != null
			);
		}
		return listener;
	}
}
