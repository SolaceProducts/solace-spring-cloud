package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.inbound.acknowledge.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.inbound.acknowledge.SolaceAckUtil;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SmfMessageReaderProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.EndpointProvider;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceConsumerDestination;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.util.ClosedChannelBindingException;
import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.UnboundFlowReceiverContainerException;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.context.Lifecycle;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.core.Pausable;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class JCSMPMessageSource extends AbstractMessageSource<Object> implements Lifecycle, Pausable {
	private final String id = UUID.randomUUID().toString();
	private final SolaceConsumerDestination consumerDestination;
	private final JCSMPSession jcsmpSession;
	private final BatchCollector batchCollector;
	private final EndpointProperties endpointProperties;
	@Nullable private final SolaceMeterAccessor solaceMeterAccessor;
	private final ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties;
	private final SmfMessageReaderProperties smfMessageReaderProperties;
	private FlowReceiverContainer flowReceiverContainer;
	private JCSMPAcknowledgementCallbackFactory ackCallbackFactory;
	private XMLMessageMapper xmlMessageMapper;
	@Nullable private SolaceBinderHealthAccessor solaceBinderHealthAccessor;
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private volatile boolean isRunning = false;
	private volatile boolean paused = false;
	private Supplier<Boolean> remoteStopFlag;
	private ErrorQueueInfrastructure errorQueueInfrastructure;
	private Consumer<Endpoint> postStart;

	// overriding Spring's LogAccessor to just use plain SLF4J
	private static final Logger logger = LoggerFactory.getLogger(JCSMPMessageSource.class);

	public JCSMPMessageSource(SolaceConsumerDestination consumerDestination,
							  JCSMPSession jcsmpSession,
							  @Nullable BatchCollector batchCollector,
							  ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
							  EndpointProperties endpointProperties,
							  @Nullable SolaceMeterAccessor solaceMeterAccessor) {
		this.consumerDestination = consumerDestination;
		this.jcsmpSession = jcsmpSession;
		this.batchCollector = batchCollector;
		this.consumerProperties = consumerProperties;
		this.endpointProperties = endpointProperties;
		this.solaceMeterAccessor = solaceMeterAccessor;
		this.smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties.getExtension());
	}

	@Override
	protected Object doReceive() {
		Lock readLock = readWriteLock.readLock();
		readLock.lock();
		try {
			if (remoteStopFlag.get()) {
				logger.debug("Message source {} is not running. Cannot receive message", id);
				return null;
			} else if (!isRunning()) {
				ClosedChannelBindingException closedBindingException = new ClosedChannelBindingException(
						String.format("Message source %s is not running", id));
				MessagingException messagingException = new MessagingException(
						String.format("Cannot receive message using message source %s", id), closedBindingException);
				logger.warn(messagingException.getMessage(), closedBindingException);
				throw messagingException;
			}
		} finally {
			readLock.unlock();
		}

		MessageContainer messageContainer;
		try {
			if (batchCollector != null) {
				// Keep time-reset outside of loop.
				// Polled consumers need to respect the time in relation to when receive() was called,
				// not when the first message is read.
				batchCollector.resetBatchCollectionStartTimestampIfEmpty();
				do {
					MessageContainer messageContainerForBatch = flowReceiverContainer.receive(25);
					if (messageContainerForBatch != null && solaceMeterAccessor != null) {
						solaceMeterAccessor.recordMessage(
								consumerProperties.getBindingName(),
								messageContainerForBatch.getMessage());
					}
					batchCollector.addToBatch(messageContainerForBatch);
				} while (!batchCollector.isBatchAvailable());
				messageContainer = null;
			} else {
				int timeout = consumerProperties.getExtension().getPolledConsumerWaitTimeInMillis();
				messageContainer = flowReceiverContainer.receive(timeout);
				if (solaceMeterAccessor != null && messageContainer != null) {
					solaceMeterAccessor.recordMessage(
							consumerProperties.getBindingName(),
							messageContainer.getMessage());
				}
			}
		} catch (JCSMPException e) {
			if (!isRunning() || remoteStopFlag.get()) {
				logger.atLevel(e instanceof JCSMPTransportException || e instanceof ClosedFacilityException ? Level.DEBUG : Level.WARN)
						.setCause(e)
						.log("Exception received while consuming a message, but the consumer <message source ID: {}> is currently shutdown. Exception will be ignored", id);
				return null;
			} else {
				MessagingException wrappedException = new MessagingException("Unable to consume message from endpoint " +
						consumerDestination.getName(), e);
				logger.warn(wrappedException.getMessage(), e);
				throw wrappedException;
			}
		} catch (UnboundFlowReceiverContainerException e) {
			// Might be thrown when async rebinding and this is configured with a super short timeout.
			// Hide this so we don't flood the logger.
			logger.debug("Unable to receive message from endpoint {}", consumerDestination.getName(), e);
			return null;
		}

		if (batchCollector != null) {
			return processBatchIfAvailable();
		} else if (messageContainer != null) {
			return processMessage(messageContainer);
		} else {
			return null;
		}
	}

	private Message<?> processMessage(MessageContainer messageContainer) {
		AcknowledgmentCallback acknowledgmentCallback = ackCallbackFactory.createCallback(messageContainer);
		try {
			return xmlMessageMapper.mapToSpring(messageContainer.getMessage(), acknowledgmentCallback, true, smfMessageReaderProperties);
		} catch (Exception e) {
			//TODO If one day the errorChannel or attributesHolder can be retrieved, use those instead
			logger.warn("XMLMessage {} cannot be consumed. It will be requeued",
					messageContainer.getMessage().getMessageId(), e);
			if (!SolaceAckUtil.republishToErrorQueue(acknowledgmentCallback)) {
				AckUtils.requeue(acknowledgmentCallback);
			}
			return null;
		}
	}

	private Message<List<?>> processBatchIfAvailable() {
		Optional<List<MessageContainer>> batchedMessages = batchCollector.collectBatchIfAvailable();
		if (!batchedMessages.isPresent()) {
			return null;
		}

		AcknowledgmentCallback acknowledgmentCallback = consumerProperties.getExtension().isTransacted() ?
				ackCallbackFactory.createTransactedBatchCallback(batchedMessages.get(),
						flowReceiverContainer.getTransactedSession()) :
				ackCallbackFactory.createBatchCallback(batchedMessages.get());

		try {
			return xmlMessageMapper.mapBatchedToSpring(batchedMessages.get()
					.stream()
					.map(MessageContainer::getMessage)
					.collect(Collectors.toList()), acknowledgmentCallback, true, smfMessageReaderProperties);
		} catch (Exception e) {
			logger.warn("Message batch cannot be consumed. It will be requeued", e);
			if (!SolaceAckUtil.republishToErrorQueue(acknowledgmentCallback)) {
				AckUtils.requeue(acknowledgmentCallback);
			}
			return null;
		} finally {
			batchCollector.confirmDelivery();
		}
	}

	@Override
	public String getComponentType() {
		return "jcsmp:message-source";
	}

	@Override
	public void start() {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info("Creating consumer to {} {} <message source ID: {}>",
					consumerProperties.getExtension().getEndpointType(), consumerDestination.getName(), id);
			if (isRunning()) {
				logger.warn("Nothing to do, message source {} is already running", id);
				return;
			}

			Map<String, String> headerNameMapping = consumerProperties.getExtension().getHeaderNameMapping();
			if (headerNameMapping != null && !headerNameMapping.isEmpty()) {
				Set<String> targetHeaderNames = new HashSet<>(headerNameMapping.values());
				if (targetHeaderNames.size() < headerNameMapping.size()) {
					MessagingException exception = new MessagingException(String.format(
							"Two or more keys map to the same header name in headerNameMapping %s <inbound adapter %s>",
							consumerProperties.getExtension().getHeaderNameMapping(), id));
					logger.warn(exception.getMessage());
					throw exception;
				}
			}

			try {
				EndpointProvider<?> endpointProvider = EndpointProvider.from(consumerProperties.getExtension().getEndpointType());

				if (endpointProvider == null) {
					IllegalArgumentException exception = new IllegalArgumentException(
							"Consumer not supported for destination type " +
									consumerProperties.getExtension().getEndpointType() +
									" <inbound adapter " + id + ">");
					logger.warn(exception.getMessage());
					throw exception;
				}

				Endpoint endpoint = endpointProvider.createInstance(consumerDestination.getName());

				if (flowReceiverContainer == null) {
					ConsumerFlowProperties consumerFlowProperties = SolaceProvisioningUtil.getConsumerFlowProperties(
							consumerDestination.getBindingDestinationName(), consumerProperties);

					flowReceiverContainer = new FlowReceiverContainer(
							jcsmpSession,
							endpoint,
							consumerProperties.getExtension().isTransacted(),
							endpointProperties,
							consumerFlowProperties);
					this.xmlMessageMapper = flowReceiverContainer.getXMLMessageMapper();

					if (paused) {
						logger.info("Message source {} is paused, pausing newly created flow receiver container {}",
								id, flowReceiverContainer.getId());
						flowReceiverContainer.pause();
					}
				}

				if (solaceBinderHealthAccessor != null) {
					solaceBinderHealthAccessor.addFlow(consumerProperties.getBindingName(), 0, flowReceiverContainer);
				}

				flowReceiverContainer.bind();

				if (postStart != null) {
					postStart.accept(endpoint);
				}
			} catch (JCSMPException e) {
				RuntimeException runtimeException = new RuntimeException(
						"Unable to get a message consumer for session " + jcsmpSession.getSessionName(), e);
				logger.warn(runtimeException.getMessage(), e);
				throw runtimeException;
			}

			ackCallbackFactory = new JCSMPAcknowledgementCallbackFactory(flowReceiverContainer);
			ackCallbackFactory.setErrorQueueInfrastructure(errorQueueInfrastructure);

			isRunning = true;
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void stop() {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			if (!isRunning()) return;
			logger.info("Stopping consumer to endpoint {} <message source ID: {}>", consumerDestination.getName(), id);
			flowReceiverContainer.unbind();
			if (solaceBinderHealthAccessor != null) {
				solaceBinderHealthAccessor.removeFlow(consumerProperties.getBindingName(), 0);
			}
			isRunning = false;
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	public void setErrorQueueInfrastructure(ErrorQueueInfrastructure errorQueueInfrastructure) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
	}

	public void setPostStart(Consumer<Endpoint> postStart) {
		this.postStart = postStart;
	}

	public void setRemoteStopFlag(Supplier<Boolean> remoteStopFlag) {
		this.remoteStopFlag = remoteStopFlag;
	}

	@Override
	public void pause() {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info("Pausing message source {}", id);
			if (flowReceiverContainer != null) {
				flowReceiverContainer.pause();
			}
			paused = true;
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void resume() {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info("Resuming message source {}", id);
			if (flowReceiverContainer != null) {
				try {
					flowReceiverContainer.resume();
				} catch (JCSMPException e) {
					throw new RuntimeException(String.format("Failed to resume message source %s", id), e);
				}
			}
			paused = false;
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public boolean isPaused() {
		if (paused) {
			if (flowReceiverContainer.isPaused()) {
				return true;
			} else {
				logger.warn("Flow receiver container {} is unexpectedly running for message source {}",
						flowReceiverContainer.getId(), id);
				return false;
			}
		} else {
			return false;
		}
	}

	public void setSolaceBinderHealthAccessor(@Nullable SolaceBinderHealthAccessor solaceBinderHealthAccessor) {
		this.solaceBinderHealthAccessor = solaceBinderHealthAccessor;
	}
}
