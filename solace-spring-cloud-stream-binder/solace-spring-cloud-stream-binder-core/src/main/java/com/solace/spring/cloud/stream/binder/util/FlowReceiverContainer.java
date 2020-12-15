package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.impl.flow.FlowHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.Nullable;

import java.util.UUID;
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
	private final AtomicReference<FlowReceiver> flowReceiverReference = new AtomicReference<>();

	/* Ideally we would cache the outgoing message IDs and remove them as they get acknowledged,
	 * but that has way too much overhead at scale (millions or billions of unacknowledged messages).
	 *
	 * Using a counter has more risks (e.g. if another object were to create a MessageContainer then send it here for
	 * acknowledgment) but is an reasonable compromise.
	 */
	// Also assuming we won't ever exceed the limit of an unsigned long...
	private final UnsignedCounterBarrier unacknowledgedMessageTracker = new UnsignedCounterBarrier();

	/**
	 * {@link #rebind(long)} can be invoked while this {@link FlowReceiverContainer} is "active"
	 * (i.e. after a {@link #bind()} and before a {@link #unbind()}).
	 * Operations which normally expect an active flow to function should use this lock's read lock to seamlessly
	 * operate as if the flow <b>was not</b> rebinding.
	 */
	// Ideally we'd only use this for locking the rebind function,
	// but since we can't create FlowReceiver objects without connecting it, we have to use this lock everywhere.
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	private static final Log logger = LogFactory.getLog(FlowReceiverContainer.class);

	public FlowReceiverContainer(JCSMPSession session, String queueName, EndpointProperties endpointProperties) {
		this.session = session;
		this.queueName = queueName;
		this.endpointProperties = endpointProperties;
	}

	/**
	 * <p>Create the {@link FlowReceiver} and {@link FlowReceiver#start() starts} it.</p>
	 * <p>Does nothing if this container is already bound to a {@link FlowReceiver}.</p>
	 * @return If no flow is bound, return the new flow ID. Otherwise return the existing flow ID.
	 * @throws JCSMPException a JCSMP exception
	 */
	public long bind() throws JCSMPException {
		// Ideally would just use flowReceiverReference.compareAndSet(null, newFlowReceiver),
		// but we can't initialize a FlowReceiver object without actually provisioning it.
		// Explicitly using a lock here is our only option.
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info(String.format("Binding %s %s", FlowReceiverContainer.class.getSimpleName(), id));
			FlowReceiver existingFlowReceiver = flowReceiverReference.get();
			if (existingFlowReceiver != null) {
				long existingFlowId = ((FlowHandle) existingFlowReceiver).getFlowId();
				logger.info(String.format("%s %s is already bound to %s",
						FlowReceiverContainer.class.getSimpleName(), id, existingFlowId));
				return existingFlowId;
			} else {
				final ConsumerFlowProperties flowProperties = new ConsumerFlowProperties()
						.setEndpoint(JCSMPFactory.onlyInstance().createQueue(queueName))
						.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT)
						.setStartState(true);
				FlowReceiver flowReceiver = session.createFlow(null, flowProperties, endpointProperties);
				flowReceiverReference.set(flowReceiver);
				return ((FlowHandle) flowReceiver).getFlowId();
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
			FlowReceiver flowReceiver = flowReceiverReference.getAndSet(null);
			if (flowReceiver != null) {
				logger.info(String.format("Unbinding %s %s", FlowReceiverContainer.class.getSimpleName(), id));
				flowReceiver.close();
			}
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * <p>Rebinds the flow if {@code flowId} matches this container's existing flow's ID.</p>
	 * <p><b>Note:</b> If the flow is bound to a temporary queue, it may lose all of its messages when rebound.</p>
	 * @param flowId The flow ID to match.
	 * @return The new flow ID or {@code null} if no flow was bound.
	 * @throws JCSMPException a JCSMP exception
	 * @throws InterruptedException was interrupted while waiting for the remaining messages to be acknowledged
	 */
	public Long rebind(long flowId) throws JCSMPException, InterruptedException {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info(String.format("Rebinding %s %s", FlowReceiverContainer.class.getSimpleName(), id));
			FlowReceiver flowReceiver = flowReceiverReference.get();
			if (flowReceiver == null) {
				logger.info(String.format("%s %s does not have a bound flow receiver",
						FlowReceiverContainer.class.getSimpleName(), id));
				return null; //TODO Throw an exception?
			}

			long existingFlowId = ((FlowHandle) flowReceiver).getFlowId();
			if (flowId != existingFlowId) {
				logger.info(String.format("Skipping rebind of %s %s, flow ID %s does not match existing flow ID %s",
						FlowReceiverContainer.class.getSimpleName(), id, flowId, existingFlowId));
				return existingFlowId;
			}

			logger.info(String.format("Stopping %s %s", FlowReceiverContainer.class.getSimpleName(), id));
			flowReceiver.stop();
			try {
				unacknowledgedMessageTracker.awaitEmpty();
			} catch (InterruptedException e) {
				logger.info(String.format("%s %s was interrupted while waiting for the remaining messages to be " +
						"acknowledged. Starting flow.", FlowReceiverContainer.class.getSimpleName(), id));
				flowReceiver.start();
				throw e;
			}

			unbind();
			return bind();
		} finally {
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
	 * @see FlowReceiver#receive(int)
	 */
	public MessageContainer receive(Integer timeoutInMillis) throws JCSMPException {
		FlowReceiver flowReceiver;
		long flowId;

		Lock readLock = readWriteLock.readLock();
		readLock.lock();
		try {
			flowReceiver = flowReceiverReference.get();
			if (flowReceiver == null) {
				// flowReceiver == null & we are not rebinding means that now flow is bound yet...
				return null; //TODO Should we block?
			}

			flowId = ((FlowHandle) flowReceiver).getFlowId();
		} finally {
			readLock.unlock();
		}

		// The flow's receive shouldn't be locked behind the read lock.
		// This lets it be interrupt-able if the flow were to be shutdown mid-receive.
		BytesXMLMessage xmlMessage =  timeoutInMillis != null ? flowReceiver.receive(timeoutInMillis) :
				flowReceiver.receive();
		if (xmlMessage == null) {
			return null;
		}

		MessageContainer messageContainer = new MessageContainer(xmlMessage, flowId);
		unacknowledgedMessageTracker.increment();
		return messageContainer;
	}

	/**
	 * <p>Acknowledge the message off the broker and mark the provided message container as acknowledged.</p>
	 * <p><b>WARNING:</b> Only messages created by this {@link FlowReceiverContainer} instance's {@link #receive()}
	 * may be passed as a parameter to this function. Failure to do so will misalign the timing for when rebinds
	 * will occur, causing rebinds to unintentionally trigger early/late.</p>
	 * @param messageContainer The message
	 */
	public void acknowledge(MessageContainer messageContainer) {
		if (messageContainer == null || messageContainer.isAcknowledged()) {
			return;
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
	 * @return The new flow ID or {@code null} if no flow was bound or the message was already acknowledged.
	 * @throws JCSMPException a JCSMP exception
	 * @throws InterruptedException was interrupted while waiting for the remaining messages to be acknowledged
	 */
	public Long acknowledgeRebind(MessageContainer messageContainer) throws JCSMPException, InterruptedException {
		if (messageContainer == null || messageContainer.isAcknowledged()) {
			return null;
		}

		unacknowledgedMessageTracker.decrement();

		Long flowId;
		try {
			flowId = rebind(messageContainer.getFlowId());
		} catch (Exception e) {
			logger.debug("Failed to rebind, re-incrementing unacknowledged-messages counter", e);
			unacknowledgedMessageTracker.increment();
			throw e;
		}

		messageContainer.setAcknowledged(true);
		return flowId;
	}

	/**
	 * <p>Get the nested {@link FlowReceiver}.</p>
	 * <p><b>Caution:</b> Instead of using this, consider instead implementing a new function with the required rebind
	 * guards.</p>
	 * @return The nested flow receiver.
	 */
	@Nullable
	FlowReceiver get() {
		return flowReceiverReference.get();
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
}
