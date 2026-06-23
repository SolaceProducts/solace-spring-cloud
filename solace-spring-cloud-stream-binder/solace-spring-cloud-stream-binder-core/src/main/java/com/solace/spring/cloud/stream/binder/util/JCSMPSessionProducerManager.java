package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;

import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JCSMPSessionProducerManager extends SharedResourceManager<XMLMessageProducer> {
	/** Default producer-close timeout used when none is supplied (e.g. by tests). */
	public static final long DEFAULT_CLOSE_TIMEOUT_IN_MILLIS = 10000L;

	private final SolaceSessionManager solaceSessionManager;
	private final CloudStreamEventHandler publisherEventHandler = new CloudStreamEventHandler();
	private final long closeTimeoutInMillis;
	private final ExecutorService closeExecutor = Executors.newCachedThreadPool(newCloseThreadFactory());

	private static final Logger LOGGER = LoggerFactory.getLogger(JCSMPSessionProducerManager.class);

	public JCSMPSessionProducerManager(SolaceSessionManager solaceSessionManager) {
		this(solaceSessionManager, DEFAULT_CLOSE_TIMEOUT_IN_MILLIS);
	}

	public JCSMPSessionProducerManager(SolaceSessionManager solaceSessionManager, long closeTimeoutInMillis) {
		super("producer");
		this.solaceSessionManager = solaceSessionManager;
		if (closeTimeoutInMillis <= 0) {
			LOGGER.warn("Invalid producer close timeout {} ms; falling back to default {} ms",
					closeTimeoutInMillis, DEFAULT_CLOSE_TIMEOUT_IN_MILLIS);
			this.closeTimeoutInMillis = DEFAULT_CLOSE_TIMEOUT_IN_MILLIS;
		} else {
			this.closeTimeoutInMillis = closeTimeoutInMillis;
		}
	}

	private static ThreadFactory newCloseThreadFactory() {
		CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("solace-producer-close-");
		threadFactory.setDaemon(true);
		return threadFactory;
	}

	@Override
	XMLMessageProducer create() throws JCSMPException {
		return solaceSessionManager.getSession().getMessageProducer(publisherEventHandler);
	}

	@Override
	void close(XMLMessageProducer resource) {
		// Guard the shared-producer close (last-user close via release()); see closeSafely().
		closeSafely(resource::close, "shared producer");
	}

	/**
	 * Closes a JCSMP producer-side resource with an upper time bound. {@code close()} parks in
	 * {@code JCSMPBasicSession.waitUntilSessionReconnectDone} while the session is reconnecting
	 * (DATAGO-137655); the close runs on a daemon thread and is interrupted after
	 * {@link #closeTimeoutInMillis} so a producer stop cannot wedge the binding lifecycle. Never
	 * propagates an exception.
	 *
	 * @param closeAction the blocking {@code close()} to run (e.g. {@code producer::close})
	 * @param description human-readable resource description, used in logging
	 */
	public void closeSafely(Runnable closeAction, String description) {
		Future<?> future;
		try {
			future = closeExecutor.submit(closeAction);
		} catch (RejectedExecutionException e) {
			// Executor already shut down (binder destroyed); nothing left to bound the close against.
			LOGGER.warn("Close executor unavailable (already shut down); skipping bounded close of {}", description);
			return;
		}
		try {
			future.get(closeTimeoutInMillis, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			LOGGER.warn("Timed out after {} ms closing {}; interrupting and proceeding "
					+ "(session is likely reconnecting)", closeTimeoutInMillis, description);
			future.cancel(true);
		} catch (ExecutionException e) {
			LOGGER.warn("Error while closing {}", description, e.getCause());
		} catch (InterruptedException e) {
			future.cancel(true);
			Thread.currentThread().interrupt();
			LOGGER.warn("Interrupted while closing {}; proceeding", description);
		}
	}

	/** Shuts down the close executor. Should be called when the owning binder is destroyed. */
	public void shutdown() {
		closeExecutor.shutdownNow();
		try {
			if (!closeExecutor.awaitTermination(closeTimeoutInMillis, TimeUnit.MILLISECONDS)) {
				// A worker still blocked in JCSMP (interrupt ignored) survives as a daemon thread; surface it.
				LOGGER.warn("Close executor did not terminate within {} ms; one or more producer-close "
						+ "workers may still be blocked (session likely still reconnecting)", closeTimeoutInMillis);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.warn("Interrupted while awaiting close-executor termination");
		}
	}

	public static class CloudStreamEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

		@Override
		public void responseReceivedEx(Object correlationKey) {
			if (correlationKey instanceof BatchProxyCorrelationKey batchProxyCorrelationKey) {
				correlationKey = batchProxyCorrelationKey.getCorrelationKeyForSuccess();
			}

			if (correlationKey instanceof ErrorChannelSendingCorrelationKey key) {
				LOGGER.trace("Producer received response for message {}",
						StaticMessageHeaderAccessor.getId(key.getInputMessage()));
				if (key.getConfirmCorrelation() != null) {
					key.getConfirmCorrelation().success();
				}
			} else if (correlationKey instanceof ErrorQueueRepublishCorrelationKey key) {
				try {
					key.handleSuccess();
				} catch (SolaceAcknowledgmentException e) { // unlikely to happen
					LOGGER.warn("Message {} successfully sent to error queue {}, but failed to acknowledge consumer message. Message is likely duplicated and was/will be redelivered on the original queue.",
							key.getSourceMessageId(), key.getErrorQueueName(), e);
					throw e;
				}
			} else {
				LOGGER.trace("Producer received response for correlation key: {}", correlationKey);
			}
		}

		@Override
		public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
			if (correlationKey instanceof BatchProxyCorrelationKey batchProxyCorrelationKey) {
				correlationKey = batchProxyCorrelationKey.getCorrelationKeyForFailure();
			}

			if (correlationKey instanceof ErrorChannelSendingCorrelationKey key) {
				UUID springMessageId = Optional.ofNullable(key.getInputMessage())
						.map(Message::getHeaders)
						.map(MessageHeaders::getId)
						.orElse(null);
				String msg = String.format("Producer received error during publishing (Spring message %s) at %s",
						springMessageId, timestamp);
				LOGGER.warn(msg, cause);
				MessagingException messagingException = key.send(msg, cause);

				if (key.getConfirmCorrelation() != null) {
					key.getConfirmCorrelation().failed(messagingException);
				}
			} else if (correlationKey instanceof ErrorQueueRepublishCorrelationKey key) {
				try {
					key.handleError();
				} catch (SolaceAcknowledgmentException e) { // unlikely to happen
					LOGGER.warn("Cannot republish message {} to error queue {}. It was/will be redelivered on the original queue",
							key.getSourceMessageId(), key.getErrorQueueName(), e);
					throw e;
				}
			} else {
				LOGGER.warn("Producer received error for correlation key: {} at {}", correlationKey, timestamp, cause);
			}
		}
	}
}
