package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.health.handlers.SolaceFlowHealthEventHandler;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
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
	private final BackOff backOff;
	private final AtomicReference<FlowReceiverReference> flowReceiverAtomicReference = new AtomicReference<>();
	private final AtomicBoolean isRebinding = new AtomicBoolean(false);
	private final AtomicBoolean isPaused = new AtomicBoolean(false);
	private final AtomicReference<SettableListenableFuture<UUID>> rebindFutureReference = new AtomicReference<>(
			new SettableListenableFuture<>());
	private final AtomicReference<BackOffExecution> backOffExecutionReference = new AtomicReference<>();

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
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Condition bindCondition = readWriteLock.writeLock().newCondition();

	private long rebindWaitTimeout = -1;
	private TimeUnit rebindWaitTimeoutUnit = TimeUnit.SECONDS;

	private static final Log logger = LogFactory.getLog(FlowReceiverContainer.class);
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private FlowEventHandler eventHandler;

	public FlowReceiverContainer(JCSMPSession session,
								 String queueName,
								 EndpointProperties endpointProperties,
								 BackOff backOff) {
		this.session = session;
		this.queueName = queueName;
		this.endpointProperties = endpointProperties;
		this.backOff = backOff;
		this.eventHandler = new SolaceFlowEventHandler(xmlMessageMapper, id.toString());
	}

	/**
	 * <p>Create the {@link FlowReceiver} and {@link FlowReceiver#start() starts} it.</p>
	 * <p>Does nothing if this container is already bound to a {@link FlowReceiver}.</p>
	 * @return If no flow is bound, return the new flow reference ID. Otherwise, return the existing flow reference ID.
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
				logger.info(String.format("Flow receiver container %s started in state '%s'", id, isPaused.get() ? "Paused" : "Running"));
				final ConsumerFlowProperties flowProperties = new ConsumerFlowProperties()
						.setEndpoint(JCSMPFactory.onlyInstance().createQueue(queueName))
						.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT)
						.setStartState(!isPaused.get());
				FlowReceiver flowReceiver = session.createFlow(null, flowProperties, endpointProperties, eventHandler);
				if (eventHandler != null && eventHandler instanceof SolaceFlowHealthEventHandler) {
					((SolaceFlowHealthEventHandler) eventHandler).setHealthStatusUp();
				}
				FlowReceiverReference newFlowReceiverReference = new FlowReceiverReference(flowReceiver);
				flowReceiverAtomicReference.set(newFlowReceiverReference);
				xmlMessageMapper.resetIgnoredProperties(id.toString());
				rebindFutureReference.getAndSet(new SettableListenableFuture<>()).set(newFlowReceiverReference.getId());
				bindCondition.signalAll();
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
	 * <p><b>Note:</b> If an exception is thrown, the flow container may be left in an unbound state.
	 * Use {@link #isBound()} to check and {@link #bind()} to recover.</p>
	 * @param flowReceiverReferenceId The flow receiver reference ID to match.
	 * @return The new flow reference ID or the existing flow reference ID if it the flow reference IDs do not match.
	 * @throws JCSMPException a JCSMP exception
	 * @throws InterruptedException was interrupted while waiting for the remaining messages to be acknowledged
	 * @throws UnboundFlowReceiverContainerException flow receiver container is not bound
	 */
	public UUID rebind(UUID flowReceiverReferenceId) throws JCSMPException, InterruptedException,
			UnboundFlowReceiverContainerException {
		return rebind(flowReceiverReferenceId, false);
	}

	/**
	 * Same as {@link #rebind(UUID)}, but with the option to return immediately with {@code null} if the lock cannot
	 * be acquired.
	 * @param flowReceiverReferenceId The flow receiver reference ID to match.
	 * @param returnImmediately return {@code null} if {@code true} and lock cannot be acquired.
	 * @return The new flow reference ID or the existing flow reference ID if it the flow reference IDs do not match.
	 * Or {@code null} if {@code returnImmediately} is {@code true} and the lock cannot be acquired.
	 * @throws JCSMPException a JCSMP exception
	 * @throws InterruptedException was interrupted while waiting for the remaining messages to be acknowledged
	 * @throws UnboundFlowReceiverContainerException flow receiver container is not bound
	 * @see #rebind(UUID)
	 */
	public UUID rebind(UUID flowReceiverReferenceId, boolean returnImmediately) throws JCSMPException,
			InterruptedException, UnboundFlowReceiverContainerException {
		Lock writeLock = readWriteLock.writeLock();
		if (returnImmediately) {
			if (!writeLock.tryLock()) {
				return null;
			}
		} else {
			writeLock.lock();
		}

		try {
			logger.info(String.format("Rebinding flow receiver container %s", id));
			FlowReceiverReference flowReceiverReference = flowReceiverAtomicReference.get();
			if (flowReceiverReference == null) {
				throw new UnboundFlowReceiverContainerException(String.format("Flow receiver container %s is not bound", id));
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
			BackOffExecution backOffExecution = backOffExecutionReference.getAndUpdate(
					b -> b != null ? b : backOff.start());
			try {
				if (backOffExecution != null) {
					long backoff = backOffExecution.nextBackOff();
					if (backoff == BackOffExecution.STOP) { // shouldn't ever happen...
						if (logger.isDebugEnabled()) {
							logger.debug(String.format(
									"Next rebind back-off is %s ms, but this isn't valid, resetting back-off...",
									backoff));
						}
						backoff = backOffExecutionReference.updateAndGet(b -> backOff.start()).nextBackOff();
					}
					if (logger.isDebugEnabled()) {
						logger.debug(String.format(
								"Back-off rebind of flow receiver container %s by %s ms", id, backoff));
					}
					Thread.sleep(backoff);
				}
			} catch (InterruptedException e) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format(
							"Flow receiver container %s back-off was interrupted, continuing...", id));
				}
			}
			return bind();
		} catch (Throwable e) {
			rebindFutureReference.getAndSet(new SettableListenableFuture<>()).setException(e);
			throw e;
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
	 * @throws UnboundFlowReceiverContainerException flow receiver container is not bound
	 * @see FlowReceiver#receive()
	 */
	public MessageContainer receive() throws JCSMPException, UnboundFlowReceiverContainerException {
		return receive(null);
	}

	/**
	 * <p>Receives the next available message. If no message is available, this method blocks until
	 * {@code timeoutInMillis} is reached.</p>
	 * <p><b>Note:</b> This method is not thread-safe.</p>
	 * @param timeoutInMillis The timeout in milliseconds. If {@code null}, wait forever.
	 *                           If less than zero and no message is available, return immediately.
	 * @return The next available message or null if the timeout expires, is interrupted, or no flow is bound.
	 * @throws JCSMPException a JCSMP exception
	 * @throws UnboundFlowReceiverContainerException flow receiver container is not bound
	 * @see FlowReceiver#receive(int)
	 */
	public MessageContainer receive(Integer timeoutInMillis) throws JCSMPException, UnboundFlowReceiverContainerException {
		final Long expiry = timeoutInMillis != null ? timeoutInMillis + System.currentTimeMillis() : null;

		FlowReceiverReference flowReceiverReference;
		Integer realTimeout;

		Lock readLock = readWriteLock.readLock();
		readLock.lock();
		try {
			flowReceiverReference = flowReceiverAtomicReference.get();
			if (flowReceiverReference == null) {
				Lock writeLock = readWriteLock.writeLock(); // waitForBind() uses a write lock. Upgrade lock.
				readLock.unlock();
				writeLock.lock();
				try {
					if (waitForBind(expiry == null ? 5000 : expiry - System.currentTimeMillis())) {
						flowReceiverReference = flowReceiverAtomicReference.get();
					} else {
						throw new UnboundFlowReceiverContainerException(
								String.format("Flow receiver container %s is not bound", id));
					}
				} catch (InterruptedException e) {
					return null;
				} finally {
					// downgrade to read lock
					readLock.lock();
					writeLock.unlock();
				}
			}

			if (expiry != null) {
				try {
					realTimeout = Math.toIntExact(expiry - System.currentTimeMillis());
					if (realTimeout < 0) {
						realTimeout = 0;
					}
				} catch (ArithmeticException e) {
					logger.debug("Failed to compute real timeout", e);
					// Always true: expiry - System.currentTimeMillis() < timeoutInMillis
					// So just set it to 0 (no-wait) if we underflow
					realTimeout = 0;
				}
			} else {
				realTimeout = null;
			}
		} finally {
			readLock.unlock();
		}

		// The flow's receive shouldn't be locked behind the read lock.
		// This lets it be interrupt-able if the flow were to be shutdown mid-receive.
		BytesXMLMessage xmlMessage;
		try {
			if (realTimeout == null) {
				xmlMessage = flowReceiverReference.get().receive();
			} else if (realTimeout == 0) {
				xmlMessage = flowReceiverReference.get().receiveNoWait();
			} else {
				// realTimeout > 0: Wait until timeout
				// realTimeout < 0: Equivalent to receiveNoWait()
				xmlMessage = flowReceiverReference.get().receive(realTimeout);
			}
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
	 * Wait until either this flow receiver container becomes bound or the timeout elapses.
	 * @param timeoutInMillis maximum wait time. Providing a value less than 0 is equivalent to 0.
	 * @return true if the container became bound.
	 * @throws InterruptedException was interrupted.
	 */
	public boolean waitForBind(long timeoutInMillis) throws InterruptedException {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			if (timeoutInMillis > 0) {
				final long expiry = timeoutInMillis + System.currentTimeMillis();
				while (!isBound()) {
					long realTimeout = expiry - System.currentTimeMillis();
					if (realTimeout <= 0) {
						return false;
					}
					bindCondition.await(realTimeout, TimeUnit.MILLISECONDS);
				}
				return true;
			} else {
				return isBound();
			}
		} finally {
			writeLock.unlock();
		}
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
		backOffExecutionReference.set(null);
	}

	/**
	 * <p>Mark the provided message container as acknowledged and initiate a {@link #rebind}.</p>
	 * <p><b>WARNING:</b> Only messages created by this {@link FlowReceiverContainer} instance's {@link #receive()}
	 * may be passed as a parameter to this function. Failure to do so will misalign the timing for when rebinds
	 * will occur, causing rebinds to unintentionally trigger early/late.</p>
	 * <p><b>Note:</b> If an exception is thrown, the flow container may be left in an unbound state.
	 * Use {@link MessageContainer#isStale()} and {@link #isBound()} to check and {@link #bind()} to recover.</p>
	 * @param messageContainer The message.
	 * @return The new flow reference ID or the current flow reference ID if message was already acknowledge.
	 * @throws JCSMPException a JCSMP exception
	 * @throws InterruptedException was interrupted while waiting for the remaining messages to be acknowledged
	 * @throws SolaceStaleMessageException the message is stale and cannot be acknowledged
	 * @throws UnboundFlowReceiverContainerException flow container is not bound
	 */
	public UUID acknowledgeRebind(MessageContainer messageContainer)
			throws JCSMPException, InterruptedException, SolaceStaleMessageException, UnboundFlowReceiverContainerException {
		return acknowledgeRebind(messageContainer, false);
	}

	/**
	 * Same as {@link #acknowledge(MessageContainer)}, but with the option to return immediately with if the lock
	 * cannot be acquired. Even if immediately returned, the message container will still be marked as acknowledged.
	 * @param messageContainer The message.
	 * @param returnImmediately Return {@code null} if {@code true} and the lock cannot be acquired.
	 * @return The new flow reference ID, or the current flow reference ID if message was already acknowledge,
	 * or {@code null} if {@code returnImmediately} is {@code true} and the lock cannot be acquired.
	 * @throws JCSMPException a JCSMP exception
	 * @throws InterruptedException was interrupted while waiting for the remaining messages to be acknowledged
	 * @throws SolaceStaleMessageException the message is stale and cannot be acknowledged
	 * @throws UnboundFlowReceiverContainerException flow container is not bound
	 * @see #acknowledgeRebind(MessageContainer)
	 */
	public UUID acknowledgeRebind(MessageContainer messageContainer, boolean returnImmediately)
			throws JCSMPException, InterruptedException, SolaceStaleMessageException, UnboundFlowReceiverContainerException {
		if (messageContainer == null || messageContainer.isAcknowledged()) {
			Lock readLock = readWriteLock.readLock();
			if (returnImmediately) {
				if (!readLock.tryLock()) {
					return null;
				}
			} else {
				readLock.lock();
			}

			try {
				FlowReceiverReference flowReceiverReference = flowReceiverAtomicReference.get();
				if (flowReceiverReference == null) {
					throw new UnboundFlowReceiverContainerException(String.format(
							"Flow receiver container %s is not bound", id));
				} else {
					return flowReceiverReference.getId();
				}
			} finally {
				readLock.unlock();
			}
		}

		if (messageContainer.isStale()) {
			throw new SolaceStaleMessageException(String.format("Message container %s (XMLMessage %s) is stale",
					messageContainer.getId(), messageContainer.getMessage().getMessageId()));
		}

		final boolean decremented;
		if (messageContainer.getAckInProgress().compareAndSet(false, true)) {
			if (logger.isTraceEnabled()) {
				logger.trace(String.format(
						"Marked message container %s as ack-in-progress, decrementing unacknowledged-messages counter",
						messageContainer.getId()));
			}
			unacknowledgedMessageTracker.decrement();
			decremented = true;
		} else {
			decremented = false;
		}

		try {
			UUID flowReceiverReferenceId;
			try {
				flowReceiverReferenceId = rebind(messageContainer.getFlowReceiverReferenceId(), returnImmediately);
			} catch (Exception e) {
				if (!messageContainer.isStale() && decremented) {
					logger.debug("Failed to rebind, re-incrementing unacknowledged-messages counter", e);
					unacknowledgedMessageTracker.increment();
				} else {
					logger.debug("Failed to rebind", e);
				}
				throw e;
			}

			messageContainer.setAcknowledged(true);
			return flowReceiverReferenceId;
		} finally {
			if (decremented) {
				messageContainer.getAckInProgress().set(false);
			}
		}
	}

	/**
	 * Same as {@link #acknowledge(MessageContainer)}, but returns a future to asynchronously capture the flow
	 * container's rebind result.
	 * @param messageContainer The message.
	 * @return a future containing the new flow reference ID or the current flow reference ID if message was already
	 * acknowledge.
	 * @throws SolaceStaleMessageException the message is stale and cannot be acknowledged
	 * @throws UnboundFlowReceiverContainerException flow container is not bound
	 * @see #acknowledgeRebind(MessageContainer)
	 */
	public Future<UUID> futureAcknowledgeRebind(MessageContainer messageContainer)
			throws SolaceStaleMessageException, UnboundFlowReceiverContainerException {
		if (messageContainer == null || messageContainer.isAcknowledged()) {
			Lock readLock = readWriteLock.readLock();
			readLock.lock();

			try {
				FlowReceiverReference flowReceiverReference = flowReceiverAtomicReference.get();
				if (flowReceiverReference == null) {
					throw new UnboundFlowReceiverContainerException(String.format(
							"Flow receiver container %s is not bound", id));
				} else {
					SettableListenableFuture<UUID> future = new SettableListenableFuture<>();
					future.set(flowReceiverReference.getId());
					return future;
				}
			} finally {
				readLock.unlock();
			}
		}

		if (messageContainer.isStale()) {
			throw new SolaceStaleMessageException(String.format("Message container %s (XMLMessage %s) is stale",
					messageContainer.getId(), messageContainer.getMessage().getMessageId()));
		}

		SettableListenableFuture<UUID> future = rebindFutureReference.get();

		if (!messageContainer.getAckInProgress().compareAndSet(false, true)) {
			if (logger.isTraceEnabled()) {
				logger.trace(String.format("Message container %s (XMLMessage %s) already has an ack in-progress",
						messageContainer.getId(), messageContainer.getMessage().getMessageId()));
			}
			return future;
		}

		if (logger.isTraceEnabled()) {
			logger.trace(String.format(
					"Decrementing unacknowledged-messages counter for message container %s (XMLMessage %s).",
					messageContainer.getId(), messageContainer.getMessage().getMessageId()));
		}
		unacknowledgedMessageTracker.decrement();

		future.addCallback(newFlowReceiverContainerId -> {
			messageContainer.setAcknowledged(true);
			messageContainer.getAckInProgress().set(false);
			}, e -> {
			try {
				if (!messageContainer.isStale()) {
					logger.trace("Failed to rebind, re-incrementing unacknowledged-messages counter", e);
					unacknowledgedMessageTracker.increment();
				} else {
					logger.trace("Failed to rebind", e);
				}
			} finally {
				messageContainer.getAckInProgress().set(false);
			}
		});
		return future;
	}

	public boolean isBound() {
		Lock readLock = readWriteLock.readLock();
		readLock.lock();
		try {
			return flowReceiverAtomicReference.get() != null;
		} finally {
			readLock.unlock();
		}
	}

	public void setRebindWaitTimeout(long timeout, TimeUnit unit) {
		this.rebindWaitTimeout = timeout;
		this.rebindWaitTimeoutUnit = unit;
	}

	public long getRebindWaitTimeout(TimeUnit unit) {
		return unit.convert(this.rebindWaitTimeout, this.rebindWaitTimeoutUnit);
	}

	public void pause() {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info(String.format("Pausing flow receiver container %s", id));
			doFlowReceiverReferencePause();
			isPaused.set(true);
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * <b>CAUTION:</b> DO NOT USE THIS. This is only exposed for testing. Use {@link #pause()} instead to pause
	 * the flow receiver container.
	 * @see #pause()
	 */
	void doFlowReceiverReferencePause() {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			FlowReceiverReference flowReceiverReference = flowReceiverAtomicReference.get();
			if (flowReceiverReference != null) {
				flowReceiverReference.pause();
			}
		} finally {
			writeLock.unlock();
		}
	}

	public void resume() throws JCSMPException {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			logger.info(String.format("Resuming flow receiver container %s", id));
			doFlowReceiverReferenceResume();
			isPaused.set(false);
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * <b>CAUTION:</b> DO NOT USE THIS. This is only exposed for testing. Use {@link #resume()} instead to resume
	 * the flow receiver container.
	 * @see #resume()
	 */
	void doFlowReceiverReferenceResume() throws JCSMPException {
		Lock writeLock = readWriteLock.writeLock();
		writeLock.lock();
		try {
			FlowReceiverReference flowReceiverReference = flowReceiverAtomicReference.get();
			if (flowReceiverReference != null) {
				flowReceiverReference.resume();
			}
		} finally {
			writeLock.unlock();
		}
	}

	public boolean isPaused() {
		return isPaused.get();
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

	public XMLMessageMapper getXMLMessageMapper() {
		return xmlMessageMapper;
	}

	public void setEventHandler(FlowEventHandler eventHandler) {
		this.eventHandler = eventHandler;
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

		private void pause() {
			flowReceiver.stop();
		}

		private void resume() throws JCSMPException {
			flowReceiver.start();
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
