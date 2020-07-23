package com.solace.spring.cloud.stream.binder.inbound;

import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class JCSMPInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {
	private final String id = UUID.randomUUID().toString();
	private final ConsumerDestination consumerDestination;
	private final JCSMPSession jcsmpSession;
	private final EndpointProperties endpointProperties;
	private final Consumer<Queue> postStart;
	private final int concurrency;
	private final Set<AtomicBoolean> consumerStopFlags;
	private ExecutorService executorService;
	private RetryTemplate retryTemplate;
	private RecoveryCallback<?> recoveryCallback;

	private static final Log logger = LogFactory.getLog(JCSMPInboundChannelAdapter.class);
	private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<>();

	public JCSMPInboundChannelAdapter(ConsumerDestination consumerDestination,
									  JCSMPSession jcsmpSession,
									  int concurrency,
									  @Nullable EndpointProperties endpointProperties,
									  @Nullable Consumer<Queue> postStart) {
		this.consumerDestination = consumerDestination;
		this.jcsmpSession = jcsmpSession;
		this.concurrency = concurrency;
		this.endpointProperties = endpointProperties;
		this.postStart = postStart;
		this.consumerStopFlags = new HashSet<>(this.concurrency);
	}

	@Override
	protected void doStart() {
		final String queueName = consumerDestination.getName();
		logger.info(String.format("Creating %s consumer flows for queue %s <inbound adapter %s>",
				concurrency, queueName, id));

		if (isRunning()) {
			logger.warn(String.format("Nothing to do. Inbound message channel adapter %s is already running", id));
			return;
		}

		if (concurrency < 1) {
			String msg = String.format("Concurrency must be greater than 0, was %s <inbound adapter %s>",
					concurrency, id);
			logger.warn(msg);
			throw new MessagingException(msg);
		}

		if (executorService != null && !executorService.isTerminated()) {
			logger.warn(String.format("Unexpectedly found running executor service while starting inbound adapter %s, " +
					"closing it...", id));
			stopAllConsumers();
		}

		Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		final ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
		flowProperties.setEndpoint(queue);
		flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

		final List<FlowReceiver> flowReceivers = new ArrayList<>(concurrency);

		try {
			for (int i = 0; i < concurrency; i++) {
				logger.info(String.format("Creating consumer %s of %s for inbound adapter %s", i + 1, concurrency, id));
				FlowReceiver flowReceiver = jcsmpSession.createFlow(null, flowProperties, endpointProperties);
				flowReceiver.start();
				flowReceivers.add(flowReceiver);
			}
		} catch (JCSMPException e) {
			String msg = String.format("Failed to get message consumer for inbound adapter %s", id);
			logger.warn(msg, e);
			flowReceivers.forEach(com.solacesystems.jcsmp.Consumer::close);
			throw new MessagingException(msg, e);
		}

		executorService = Executors.newFixedThreadPool(this.concurrency);
		flowReceivers.stream()
				.map(this::buildListener)
				.forEach(listener -> {
					consumerStopFlags.add(listener.getStopFlag());
					executorService.submit(listener);
				});
		executorService.shutdown(); // All tasks have been submitted

		if (postStart != null) {
			postStart.accept(queue);
		}
	}

	@Override
	protected void doStop() {
		if (!isRunning()) return;
		stopAllConsumers();
	}

	private void stopAllConsumers() {
		final String queueName = consumerDestination.getName();
		logger.info(String.format("Stopping all %s consumer flows to queue %s <inbound adapter ID: %s>",
				concurrency, queueName, id));
		consumerStopFlags.forEach(flag -> flag.set(true)); // Mark threads for shutdown
		executorService.shutdownNow(); // Interrupt any blocked threads
		try {
			if (executorService.awaitTermination(1, TimeUnit.MINUTES)) {
				// cleanup
				consumerStopFlags.clear();
			} else {
				String msg = String.format("executor service shutdown for inbound adapter %s timed out", id);
				logger.warn(msg);
				throw new MessagingException(msg);
			}
		} catch (InterruptedException e) {
			String msg = String.format("executor service shutdown for inbound adapter %s was interrupted", id);
			logger.warn(msg);
			throw new MessagingException(msg);
		}
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

	private InboundXMLMessageListener buildListener(FlowReceiver flowReceiver) {
		InboundXMLMessageListener listener;
		if (retryTemplate != null) {
			Assert.state(getErrorChannel() == null,
					"Cannot have an 'errorChannel' property when a 'RetryTemplate' is provided; " +
							"use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to send " +
							"an error message when retries are exhausted");
			RetryableInboundXMLMessageListener retryableMessageListener = new RetryableInboundXMLMessageListener(
					flowReceiver,
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
					flowReceiver,
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
