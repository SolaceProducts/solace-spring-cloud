package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.Nullable;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>A {@link FlowReceiver} wrapper object which allows for flow rebinds.</p>
 * <p>Messaging operations concurrently invoked through this object during a rebind operation are not affected
 * by the rebind.</p>
 */
public class FlowReceiverContainer {
	private final UUID id = UUID.randomUUID();
	private final JCSMPSession session;
	private final String queueName;
	private final EndpointProperties endpointProperties;
	private final AtomicReference<FlowReceiverReference> flowReceiverAtomicReference = new AtomicReference<>();
	private final AtomicBoolean isRebinding = new AtomicBoolean(false);

	/* Ideally we would cache the outgoing message IDs and remove them as they get acknowledged,
	 * but that has way too much overhead at scale (millions or billions of unacknowledged messages).
	 *
	 * Using a counter has more risks (e.g. if another object were to create a MessageContainer then send it here for
	 * acknowledgment) but is an reasonable compromise.
	 */
	// Also assuming we won't ever exceed the limit of an unsigned long...
	private final UnsignedCounterBarrier unacknowledgedMessageTracker = new UnsignedCounterBarrier();

	/**
	 * {@link #rebind(UUID)} can be invoked while this {@link FlowReceiverContainer} is "active"
	 * (i.e. after a {@link #bind()} and before a {@link #unbind()}).
	 * Operations which normally expect an active flow to function should use this lock's read lock to seamlessly
	 * operate as if the flow <b>was not</b> rebinding.
	 */
	// Ideally we'd only use this for locking the rebind function,
	// but since we can't create FlowReceiver objects without connecting it, we have to use this lock everywhere.
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	private long rebindWaitTimeout = -1;
	private TimeUnit rebindWaitTimeoutUnit = TimeUnit.SECONDS;

	private static final Log logger = LogFactory.getLog(FlowReceiverContainer.class);

	public FlowReceiverContainer(JCSMPSession session, String queueName, EndpointProperties endpointProperties) {
		this.session = session;
		this.queueName = queueName;
		this.endpointProperties = endpointProperties;
	}

	/**
	 * <p>Create the {@link FlowReceiver} and {@link FlowReceiver#start() starts} it.</p>
	 * <p>Does nothing if this container is already bound to a {@link FlowReceiver}.</p>
	 * @return If no flow is bound, return the new flow reference ID. Otherwise return the existing flow reference ID.
	 * @throws JCSMPException a JCSMP exception
	 */
	public UUID bind() throws JCSMPException {
		// Ideally would just use flowReceiverReference.compareAndSet(null, newFlowReceiver),
		// but we can't initialize a FlowReceiver object without actually provisioning it.
		// Explicitly using a lock here is our only option.
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info(String.format("Binding flow receiver container %s", id));
			FlowReceiverReference existingFlowReceiverReference = flowReceiverAtomicReference.get();
			if (existingFlowReceiverReference != null) {
				UUID existingFlowRefId = existingFlowReceiverReference.getId();
				logger.info(String.format("Flow receiver container %s is already bound to %s", id, existingFlowRefId));
				return existingFlowRefId;
			} else {
				final ConsumerFlowProperties flowProperties = new ConsumerFlowProperties()
						.setEndpoint(JCSMPFactory.onlyInstance().createQueue(queueName))
						.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT)
						.setStartState(true);
				FlowReceiver flowReceiver = session.createFlow(null, flowProperties, endpointProperties);
				FlowReceiverReference newFlowReceiverReference = new FlowReceiverReference(flowReceiver);
				flowReceiverAtomicReference.set(newFlowReceiverReference);
				return newFlowReceiverReference.getId();
			}
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * Closes the bound {@link FlowReceiver}.
	 */
	public void unbind() {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			FlowReceiverReference flowReceiverReference = flowReceiverAtomicReference.getAndSet(null);
			if (flowReceiverReference != null) {
				logger.info(String.format("Unbinding flow receiver container %s", id));
				flowReceiverReference.getStaleMessagesFlag().set(true);
				flowReceiverReference.get().close();
				unacknowledgedMessageTracker.reset();
			}
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * <p>Rebinds the flow if {@code flowReceiverReferenceId} matches this container's existing flow reference's ID.
	 * </p>
	 * <p><b>Note:</b> If the flow is bound to a temporary queue, it may lose all of its messages when rebound.</p>
	 * @param flowReceiverReferenceId The flow receiver reference ID to match.
	 * @return The new flow reference ID or the existing flow reference ID if it the flow reference IDs do not match.
	 * @throws JCSMPException a JCSMP exception
	 * @throws InterruptedException was interrupted while waiting for the remaining messages to be acknowledged
	 * @throws IllegalStateException flow receiver container is not bound
	 */
	public UUID rebind(UUID flowReceiverReferenceId) throws JCSMPException, InterruptedException {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info(String.format("Rebinding flow receiver container %s", id));
			FlowReceiverReference flowReceiverReference = flowReceiverAtomicReference.get();
			if (flowReceiverReference == null) {
				throw new IllegalStateException(String.format("Flow receiver container %s is not bound", id));
			}

			UUID existingFlowReceiverReferenceId = flowReceiverReference.getId();
			if (!flowReceiverReferenceId.equals(existingFlowReceiverReferenceId)) {
				logger.info(String.format(
						"Skipping rebind of flow receiver container %s, flow receiver reference ID %s " +
								" does not match existing flow receiver reference ID %s",
						id, flowReceiverReferenceId, existingFlowReceiverReferenceId));
				return existingFlowReceiverReferenceId;
			}

			isRebinding.set(true);
			logger.info(String.format("Stopping flow receiver container %s", id));
			flowReceiverReference.get().stop();
			try {
				if (!unacknowledgedMessageTracker.awaitEmpty(rebindWaitTimeout, rebindWaitTimeoutUnit)) {
					logger.info(String.format("Timed out while flow receiver container %s was waiting for the" +
							" remaining messages to be acknowledged. They will be stale. Continuing rebind.", id));
				}
			} catch (InterruptedException e) {
				logger.info(String.format("Flow receiver container %s was interrupted while waiting for the " +
						"remaining messages to be acknowledged. Starting flow.", id));
				flowReceiverReference.get().start();
				throw e;
			}

			unbind();
			return bind();
		} finally {
			isRebinding.compareAndSet(true, false);
			writeLock.unlock();
		}
	}

	/**
	 * <p>Receives the next available message, waiting until one is available.</p>
	 * <p><b>Note:</b> This method is not thread-safe.</p>
	 * @return The next available message or null if is interrupted or no flow is bound.
	 * @throws JCSMPException a JCSMP exception
	 * @see FlowReceiver#receive()
	 */
	public MessageContainer receive() throws JCSMPException {
		return receive(null);
	}

	/**
	 * <p>Receives the next available message. If no message is available, this method blocks until
	 * {@code timeoutInMillis} is reached. A timeout of zero never expires, and the call blocks indefinitely.</p>
	 * <p><b>Note:</b> This method is not thread-safe.</p>
	 * @param timeoutInMillis The timeout in milliseconds.
	 * @return The next available message or null if the timeout expires, is interrupted, or no flow is bound.
	 * @throws JCSMPException a JCSMP exception
	 * @throws IllegalStateException flow receiver container is not bound
	 * @see FlowReceiver#receive(int)
	 */
	public MessageContainer receive(Integer timeoutInMillis) throws JCSMPException {
		FlowReceiverReference flowReceiverReference;

		Lock readLock = readWriteLock.readLock();
		readLock.lock();
		try {
			flowReceiverReference = flowReceiverAtomicReference.get();
			if (flowReceiverReference == null) {
				// since we have the read lock, this cannot occur due to a rebind
				throw new IllegalStateException(String.format("Flow receiver container %s is not bound", id));
			}
		} finally {
			readLock.unlock();
		}

		// The flow's receive shouldn't be locked behind the read lock.
		// This lets it be interrupt-able if the flow were to be shutdown mid-receive.
		BytesXMLMessage xmlMessage;
		try {
			xmlMessage = timeoutInMillis != null ? flowReceiverReference.get().receive(timeoutInMillis) :
					flowReceiverReference.get().receive();
		} catch (JCSMPTransportException | ClosedFacilityException e) {
			if (isRebinding.get()) {
				logger.debug(String.format(
						"Flow receiver container %s was interrupted by a rebind, exception will be ignored", id), e);
				return null;
			} else {
				throw e;
			}
		}

		if (xmlMessage == null) {
			return null;
		}

		MessageContainer messageContainer = new MessageContainer(xmlMessage, flowReceiverReference.getId(),
				flowReceiverReference.getStaleMessagesFlag());
		unacknowledgedMessageTracker.increment();
		return messageContainer;
	}

	/**
	 * <p>Acknowledge the message off the broker and mark the provided message container as acknowledged.</p>
	 * <p><b>WARNING:</b> Only messages created by this {@link FlowReceiverContainer} instance's {@link #receive()}
	 * may be passed as a parameter to this function. Failure to do so will misalign the timing for when rebinds
	 * will occur, causing rebinds to unintentionally trigger early/late.</p>
	 * @param messageContainer The message
	 * @throws SolaceStaleMessageException the message is stale and cannot be acknowledged
	 */
	public void acknowledge(MessageContainer messageContainer) throws SolaceStaleMessageException {
		if (messageContainer == null || messageContainer.isAcknowledged()) {
			return;
		}
		if (messageContainer.isStale()) {
			throw new SolaceStaleMessageException(String.format("Message container %s (XMLMessage %s) is stale",
					messageContainer.getId(), messageContainer.getMessage().getMessageId()));
		}

		messageContainer.getMessage().ackMessage();
		unacknowledgedMessageTracker.decrement();
		messageContainer.setAcknowledged(true);
	}

	/**
	 * <p>Mark the provided message container as acknowledged and initiate a {@link #rebind}.</p>
	 * <p><b>WARNING:</b> Only messages created by this {@link FlowReceiverContainer} instance's {@link #receive()}
	 * may be passed as a parameter to this function. Failure to do so will misalign the timing for when rebinds
	 * will occur, causing rebinds to unintentionally trigger early/late.</p>
	 * @param messageContainer The message.
	 * @return The new flow reference ID or {@code null} if no flow was bound or the message was already acknowledged.
	 * @throws JCSMPException a JCSMP exception
	 * @throws InterruptedException was interrupted while waiting for the remaining messages to be acknowledged
	 */
	public UUID acknowledgeRebind(MessageContainer messageContainer)
			throws JCSMPException, InterruptedException, SolaceStaleMessageException {
		if (messageContainer == null || messageContainer.isAcknowledged()) {
			return null;
		}
		if (messageContainer.isStale()) {
			throw new SolaceStaleMessageException(String.format("Message container %s (XMLMessage %s) is stale",
					messageContainer.getId(), messageContainer.getMessage().getMessageId()));
		}

		unacknowledgedMessageTracker.decrement();

		UUID flowReceiverReferenceId;
		try {
			flowReceiverReferenceId = rebind(messageContainer.getFlowReceiverReferenceId());
		} catch (Exception e) {
			logger.debug("Failed to rebind, re-incrementing unacknowledged-messages counter", e);
			unacknowledgedMessageTracker.increment();
			throw e;
		}

		messageContainer.setAcknowledged(true);
		return flowReceiverReferenceId;
	}

	public void setRebindWaitTimeout(long timeout, TimeUnit unit) {
		this.rebindWaitTimeout = timeout;
		this.rebindWaitTimeoutUnit = unit;
	}

	/**
	 * <p>Get the nested {@link FlowReceiver}.</p>
	 * <p><b>Caution:</b> Instead of using this, consider instead implementing a new function with the required rebind
	 * guards.</p>
	 * @return The nested flow receiver.
	 */
	@Nullable
	FlowReceiverReference getFlowReceiverReference() {
		return flowReceiverAtomicReference.get();
	}

	/**
	 * Gets the number of unacknowledged messages. This value is an unsigned.
	 * @return the number of unacknowledged messages.
	 */
	long getNumUnacknowledgedMessages() {
		return unacknowledgedMessageTracker.getCount();
	}

	public UUID getId() {
		return id;
	}

	public String getQueueName() {
		return queueName;
	}

	static class FlowReceiverReference {
		private final UUID id = UUID.randomUUID();
		private final FlowReceiver flowReceiver;
		private final AtomicBoolean staleMessagesFlag = new AtomicBoolean(false);

		public FlowReceiverReference(FlowReceiver flowReceiver) {
			this.flowReceiver = flowReceiver;
		}

		/**
		 * Get the flow receiver reference ID.
		 * This is <b>NOT</b> the actual flow ID, but just a reference ID used by this binder to identify a
		 * particular {@link FlowReceiver} instance.
		 * We can't use the actual flow ID for this since that can transparently change by itself due to
		 * events such as reconnection.
		 * @return the flow receiver reference ID.
		 */
		public UUID getId() {
			return id;
		}

		public FlowReceiver get() {
			return flowReceiver;
		}

		public AtomicBoolean getStaleMessagesFlag() {
			return staleMessagesFlag;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			FlowReceiverReference that = (FlowReceiverReference) o;
			return id.equals(that.id) && flowReceiver.equals(that.flowReceiver);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, flowReceiver);
		}
	}
}
