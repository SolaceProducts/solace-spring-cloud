package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceConsumerDestination;
import com.solace.spring.cloud.stream.binder.util.*;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.core.Pausable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class JCSMPInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable, Pausable {
	private final String id = UUID.randomUUID().toString();
	private final SolaceConsumerDestination consumerDestination;
	private final JCSMPSession jcsmpSession;
	private final SolaceConsumerProperties consumerProperties;
	private final EndpointProperties endpointProperties;
	private final int concurrency;
	private final RetryableTaskService taskService;
	private final long shutdownInterruptThresholdInMillis = 500; //TODO Make this configurable
	private final Set<AtomicBoolean> consumerStopFlags;
	private final Set<Consumer<Boolean>> pauseFns;
	private Consumer<Queue> postStart;
	private ExecutorService executorService;
	private AtomicBoolean remoteStopFlag;
	private RetryTemplate retryTemplate;
	private RecoveryCallback<?> recoveryCallback;
	private ErrorQueueInfrastructure errorQueueInfrastructure;

	private static final Log logger = LogFactory.getLog(JCSMPInboundChannelAdapter.class);
	private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<>();

	public JCSMPInboundChannelAdapter(SolaceConsumerDestination consumerDestination,
									  JCSMPSession jcsmpSession,
									  int concurrency,
									  RetryableTaskService taskService,
									  SolaceConsumerProperties consumerProperties,
									  @Nullable EndpointProperties endpointProperties) {
		this.consumerDestination = consumerDestination;
		this.jcsmpSession = jcsmpSession;
		this.concurrency = concurrency;
		this.taskService = taskService;
		this.consumerProperties = consumerProperties;
		this.endpointProperties = endpointProperties;
		this.consumerStopFlags = new HashSet<>(this.concurrency);
		this.pauseFns = new HashSet<>(this.concurrency);
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

		final List<FlowReceiverContainer> flowReceivers = new ArrayList<>(concurrency);

		try {
			for (int i = 0; i < concurrency; i++) {
				logger.info(String.format("Creating consumer %s of %s for inbound adapter %s", i + 1, concurrency, id));
				FlowReceiverContainer flowReceiverContainer = new FlowReceiverContainer(jcsmpSession, queueName, endpointProperties, new SolaceFlowEventHandler());
				flowReceiverContainer.setRebindWaitTimeout(consumerProperties.getFlowPreRebindWaitTimeout(),
						TimeUnit.MILLISECONDS);
				flowReceiverContainer.bind();
				flowReceivers.add(flowReceiverContainer);
			}
		} catch (JCSMPException e) {
			String msg = String.format("Failed to get message consumer for inbound adapter %s", id);
			logger.warn(msg, e);
			flowReceivers.forEach(FlowReceiverContainer::unbind);
			throw new MessagingException(msg, e);
		}

		if (retryTemplate != null) {
			retryTemplate.registerListener(new SolaceRetryListener(queueName));
		}

		executorService = Executors.newFixedThreadPool(this.concurrency);
		flowReceivers.stream()
				.map(this::buildListener)
				.forEach(listener -> {
					consumerStopFlags.add(listener.getStopFlag());
					pauseFns.add(listener::setPaused);
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
		try {
			if (!executorService.awaitTermination(shutdownInterruptThresholdInMillis, TimeUnit.MILLISECONDS)) {
				logger.info(String.format("Interrupting all workers for inbound adapter %s", id));
				executorService.shutdownNow();
				if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
					String msg = String.format("executor service shutdown for inbound adapter %s timed out", id);
					logger.warn(msg);
					throw new MessagingException(msg);
				}
			}

			// cleanup
			consumerStopFlags.clear();
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

	public void setPostStart(Consumer<Queue> postStart) {
		this.postStart = postStart;
	}

	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}

	public void setRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	public void setErrorQueueInfrastructure(ErrorQueueInfrastructure errorQueueInfrastructure) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
	}

	public void setRemoteStopFlag(AtomicBoolean remoteStopFlag) {
		this.remoteStopFlag = remoteStopFlag;
	}

	@Override
	protected AttributeAccessor getErrorMessageAttributes(org.springframework.messaging.Message<?> message) {
		AttributeAccessor attributes = attributesHolder.get();
		return attributes == null ? super.getErrorMessageAttributes(message) : attributes;
	}

	private InboundXMLMessageListener buildListener(FlowReceiverContainer flowReceiverContainer) {
		JCSMPAcknowledgementCallbackFactory ackCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, consumerDestination.isTemporary(), taskService);
		ackCallbackFactory.setErrorQueueInfrastructure(errorQueueInfrastructure);

		InboundXMLMessageListener listener;
		if (retryTemplate != null) {
			Assert.state(getErrorChannel() == null,
					"Cannot have an 'errorChannel' property when a 'RetryTemplate' is provided; " +
							"use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to send " +
							"an error message when retries are exhausted");
			RetryableInboundXMLMessageListener retryableMessageListener = new RetryableInboundXMLMessageListener(
					flowReceiverContainer,
					consumerDestination,
					this::sendMessage,
					ackCallbackFactory,
					retryTemplate,
					recoveryCallback,
					remoteStopFlag,
					attributesHolder
			);
			listener = retryableMessageListener;
		} else {
			listener = new BasicInboundXMLMessageListener(
					flowReceiverContainer,
					consumerDestination,
					this::sendMessage,
					ackCallbackFactory,
					this::sendErrorMessageIfNecessary,
					remoteStopFlag,
					attributesHolder,
					this.getErrorChannel() != null
			);
		}
		return listener;
	}

	@Override
	public void pause() {
		for (Consumer<Boolean> fn : pauseFns) {
			fn.accept(true);
		}
	}

	@Override
	public void resume() {
		for (Consumer<Boolean> fn : pauseFns) {
			fn.accept(false);
		}
	}

	private final class SolaceRetryListener implements RetryListener {

		private final String queueName;

		private SolaceRetryListener(String queueName) {
			this.queueName = queueName;
		}

		@Override
		public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
			return true;
		}

		@Override
		public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {

		}

		@Override
		public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
			logger.warn(String.format("Failed to consume a message from destination %s - attempt %s",
					queueName, context.getRetryCount()));
			for (Throwable nestedThrowable : ExceptionUtils.getThrowableList(throwable)) {
				if (nestedThrowable instanceof SolaceMessageConversionException ||
						nestedThrowable instanceof SolaceStaleMessageException) {
					// Do not retry if these exceptions are thrown
					context.setExhaustedOnly();
					break;
				}
			}
		}
	}
}
