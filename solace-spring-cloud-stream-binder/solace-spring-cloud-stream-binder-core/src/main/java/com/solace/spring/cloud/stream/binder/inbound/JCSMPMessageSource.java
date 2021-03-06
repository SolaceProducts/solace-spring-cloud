package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceConsumerDestination;
import com.solace.spring.cloud.stream.binder.util.ClosedChannelBindingException;
import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.RetryableTaskService;
import com.solace.spring.cloud.stream.binder.util.UnboundFlowReceiverContainerException;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Queue;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.context.Lifecycle;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.messaging.MessagingException;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class JCSMPMessageSource extends AbstractMessageSource<Object> implements Lifecycle {
	private final String id = UUID.randomUUID().toString();
	private final String queueName;
	private final JCSMPSession jcsmpSession;
	private final RetryableTaskService taskService;
	private final EndpointProperties endpointProperties;
	private final boolean hasTemporaryQueue;
	private final ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties;
	private FlowReceiverContainer flowReceiverContainer;
	private JCSMPAcknowledgementCallbackFactory ackCallbackFactory;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private volatile boolean isRunning = false;
	private Supplier<Boolean> remoteStopFlag;
	private ErrorQueueInfrastructure errorQueueInfrastructure;
	private Consumer<Queue> postStart;

	public JCSMPMessageSource(SolaceConsumerDestination destination,
							  JCSMPSession jcsmpSession,
							  RetryableTaskService taskService,
							  ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
							  EndpointProperties endpointProperties) {
		this.queueName = destination.getName();
		this.jcsmpSession = jcsmpSession;
		this.taskService = taskService;
		this.consumerProperties = consumerProperties;
		this.endpointProperties = endpointProperties;
		this.hasTemporaryQueue = destination.isTemporary();
	}

	@Override
	protected Object doReceive() {
		Lock readLock = readWriteLock.readLock();
		readLock.lock();
		try {
			if (remoteStopFlag.get()) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Message source %s is not running. Cannot receive message", id));
				}
				return null;
			} else if (!isRunning()) {
				String msg0 = String.format("Cannot receive message using message source %s", id);
				String msg1 = String.format("Message source %s is not running", id);
				ClosedChannelBindingException closedBindingException = new ClosedChannelBindingException(msg1);
				logger.warn(closedBindingException, msg0);
				throw new MessagingException(msg0, closedBindingException);
			}
		} finally {
			readLock.unlock();
		}

		MessageContainer messageContainer;
		try {
			int timeout = consumerProperties.getExtension().getPolledConsumerWaitTimeInMillis();
			messageContainer = flowReceiverContainer.receive(timeout);
		} catch (JCSMPException e) {
			if (!isRunning() || remoteStopFlag.get()) {
				String msg = String.format("Exception received while consuming a message, but the consumer " +
						"<message source ID: %s> is currently shutdown. Exception will be ignored", id);
				if (e instanceof JCSMPTransportException || e instanceof ClosedFacilityException) {
					logger.debug(e, msg);
				} else {
					logger.warn(e, msg);
				}
				return null;
			} else {
				String msg = String.format("Unable to consume message from queue %s", queueName);
				logger.warn(e, msg);
				throw new MessagingException(msg, e);
			}
		} catch (UnboundFlowReceiverContainerException e) {
			if (logger.isDebugEnabled()) {
				// Might be thrown when async rebinding and this is configured with a super short timeout.
				// Hide this so we don't flood the logger.
				logger.debug(e, String.format("Unable to receive message from queue %s", queueName));
			}
			return null;
		}

		if (messageContainer == null) return null;

		AcknowledgmentCallback acknowledgmentCallback = ackCallbackFactory.createCallback(messageContainer);

		try {
			return xmlMessageMapper.map(messageContainer.getMessage(), acknowledgmentCallback, true);
		} catch (Exception e) {
			//TODO If one day the errorChannel or attributesHolder can be retrieved, use those instead
			logger.warn(e, String.format("XMLMessage %s cannot be consumed. It will be rejected",
					messageContainer.getMessage().getMessageId()));
			AckUtils.reject(acknowledgmentCallback);
			return null;
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
				logger.warn(e, msg);
				throw new RuntimeException(msg, e);
			}

			if (postStart != null) {
				postStart.accept(JCSMPFactory.onlyInstance().createQueue(queueName));
			}

			ackCallbackFactory = new JCSMPAcknowledgementCallbackFactory(flowReceiverContainer, hasTemporaryQueue,
					taskService);
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
			logger.info(String.format("Stopping consumer to queue %s <message source ID: %s>", queueName, id));
			flowReceiverContainer.unbind();
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

	public void setPostStart(Consumer<Queue> postStart) {
		this.postStart = postStart;
	}

	public void setRemoteStopFlag(Supplier<Boolean> remoteStopFlag) {
		this.remoteStopFlag = remoteStopFlag;
	}
}
