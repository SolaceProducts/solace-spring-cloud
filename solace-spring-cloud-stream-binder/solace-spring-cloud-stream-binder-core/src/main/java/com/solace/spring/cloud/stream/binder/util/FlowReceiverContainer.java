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
	private final String id = UUID.randomUUID().toString();
	private final JCSMPSession session;
	private final String queueName;
	private final EndpointProperties endpointProperties;
	private final AtomicReference<FlowReceiver> flowReceiverReference = new AtomicReference<>();

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
			logger.info(String.format("Binding %s %s", this.getClass().getSimpleName(), id));
			FlowReceiver existingFlowReceiver = flowReceiverReference.get();
			if (existingFlowReceiver != null) {
				long existingFlowId = ((FlowHandle) existingFlowReceiver).getFlowId();
				logger.info(String.format("%s %s is already bound to %s",
						this.getClass().getSimpleName(), id, existingFlowId));
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
		// Ideally, we'd fully rely on flowReceiverReference's AtomicReference methods for handling concurrency.
		// But because bind() is forced to use a write lock, we must use a writeLock here as well.
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			FlowReceiver flowReceiver = flowReceiverReference.getAndSet(null);
			if (flowReceiver != null) {
				logger.info(String.format("Unbinding %s %s", this.getClass().getSimpleName(), id));
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
	 */
	public Long rebind(long flowId) throws JCSMPException {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info(String.format("Rebinding %s %s", this.getClass().getSimpleName(), id));
			FlowReceiver flowReceiver = flowReceiverReference.get();
			if (flowReceiver == null) {
				logger.info(String.format("%s %s does not have a bound flow receiver",
						this.getClass().getSimpleName(), id));
				return null; //TODO Throw an exception?
			}

			long existingFlowId = ((FlowHandle) flowReceiver).getFlowId();
			if (flowId != existingFlowId) {
				logger.info(String.format("Skipping rebind of %s %s, flow ID %s does not match existing flow ID %s",
						this.getClass().getSimpleName(), id, flowId, existingFlowId));
				return existingFlowId;
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
		Lock readLock = readWriteLock.readLock();
		readLock.lock();
		try {
			FlowReceiver flowReceiver = flowReceiverReference.get();
			if (flowReceiver == null) {
				// flowReceiver == null & we are not rebinding means that now flow is bound yet...
				return null; //TODO Should we block?
			}

			long flowId = ((FlowHandle) flowReceiver).getFlowId();
			BytesXMLMessage xmlMessage =  timeoutInMillis != null ? flowReceiver.receive(timeoutInMillis) :
					flowReceiver.receive();
			return xmlMessage != null ? new MessageContainer(xmlMessage, flowId) : null;
		} finally {
			readLock.unlock();
		}
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

	public String getQueueName() {
		return queueName;
	}
}