package com.solace.spring.cloud.stream.binder.outbound;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.properties.SmfMessageWriterProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.util.BatchProxyCorrelationKey;
import com.solace.spring.cloud.stream.binder.util.ClosedChannelBindingException;
import com.solace.spring.cloud.stream.binder.util.CorrelationData;
import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.spring.cloud.stream.binder.util.ErrorChannelSendingCorrelationKey;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager.CloudStreamEventHandler;
import com.solace.spring.cloud.stream.binder.util.StaticMessageHeaderMapAccessor;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.StaleSessionException;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class JCSMPOutboundMessageHandler implements MessageHandler, Lifecycle {
	private final String id = UUID.randomUUID().toString();
	private final DestinationType configDestinationType;
	private final Destination configDestination;
	private final JCSMPSession jcsmpSession;
	private final MessageChannel errorChannel;
	private final JCSMPSessionProducerManager producerManager;
	private final ExtendedProducerProperties<SolaceProducerProperties> properties;
	private final SmfMessageWriterProperties smfMessageWriterProperties;
	private final JCSMPStreamingPublishCorrelatingEventHandler producerEventHandler = new CloudStreamEventHandler();
	@Nullable private final SolaceMeterAccessor solaceMeterAccessor;
	private XMLMessageProducer producer;
	@Nullable private TransactedSession transactedSession;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private boolean isRunning = false;
	private ErrorMessageStrategy errorMessageStrategy;

	// DATAGO-134580: when the broker sends an unsolicited CloseFlow (message-spool shutdown,
	// DR failover, "Service Unavailable" on GD), JCSMP marks the producer terminally closed.
	// The session itself stays connected, so the next producer.send(...) throws
	// StaleSessionException synchronously. We set this flag from the catch block and lazily
	// recreate the producer at the top of the next handleMessage(...) call.
	private volatile boolean producerNeedsRecreation = false;
	private final Object recreateLock = new Object();

	private static final Logger LOGGER = LoggerFactory.getLogger(JCSMPOutboundMessageHandler.class);

	public JCSMPOutboundMessageHandler(ProducerDestination destination,
									   JCSMPSession jcsmpSession,
									   MessageChannel errorChannel,
									   JCSMPSessionProducerManager producerManager,
									   ExtendedProducerProperties<SolaceProducerProperties> properties,
									   @Nullable SolaceMeterAccessor solaceMeterAccessor) {
		this.configDestinationType = properties.getExtension().getDestinationType();
		this.configDestination = configDestinationType == DestinationType.TOPIC ?
				JCSMPFactory.onlyInstance().createTopic(destination.getName()) :
				JCSMPFactory.onlyInstance().createQueue(destination.getName());
		this.jcsmpSession = jcsmpSession;
		this.errorChannel = errorChannel;
		this.producerManager = producerManager;
		this.properties = properties;
		this.smfMessageWriterProperties = new SmfMessageWriterProperties(properties.getExtension());
		this.solaceMeterAccessor = solaceMeterAccessor;
	}

	@Override
	public void handleMessage(@NonNull Message<?> message) throws MessagingException {
		ErrorChannelSendingCorrelationKey correlationKey = new ErrorChannelSendingCorrelationKey(message,
				errorChannel, errorMessageStrategy);

		if (! isRunning()) {
			String msg0 = String.format("Cannot send message using handler %s", id);
			String msg1 = String.format("Message handler %s is not running", id);
			throw handleMessagingException(correlationKey, msg0, new ClosedChannelBindingException(msg1));
		}

		// DATAGO-134580: if a previous publish marked the producer stale (broker fanned out an
		// unsolicited CloseFlow), rebuild it before attempting this publish. A failure here is
		// routed through the normal error channel via handleMessagingException, with the
		// recreation flag left armed so the next message retries.
		recreateProducerIfNeeded(correlationKey);

		try {
			CorrelationData correlationData = message.getHeaders()
					.get(SolaceBinderHeaders.CONFIRM_CORRELATION, CorrelationData.class);
			if (correlationData != null) {
				correlationData.setMessage(message);
				correlationKey.setConfirmCorrelation(correlationData);
			}
		} catch (IllegalArgumentException e) {
			throw handleMessagingException(correlationKey,
					String.format("Unable to parse header %s", SolaceBinderHeaders.CONFIRM_CORRELATION), e);
		}

		List<XMLMessage> smfMessages;
		List<Destination> dynamicDestinations;
		if (message.getHeaders().containsKey(BinderHeaders.BATCH_HEADERS)) {
			LOGGER.debug("Detected header {}, handling as batched message (Message<List<?>>) <message handler ID: {}>",
					BinderHeaders.BATCH_HEADERS, id);
			smfMessages = xmlMessageMapper.mapBatchedToSmf(message, smfMessageWriterProperties);

			BatchProxyCorrelationKey batchProxyCorrelationKey = transactedSession == null ?
					new BatchProxyCorrelationKey(correlationKey, smfMessages.size()) : null;
			smfMessages.forEach(smfMessage -> smfMessage.setCorrelationKey(
					Objects.requireNonNullElse(batchProxyCorrelationKey, correlationKey)));

			if (transactedSession == null) {
				smfMessages.get(smfMessages.size() - 1).setAckImmediately(true);
			}

			// after successfully running xmlMessageMapper.mapBatchMessage(),
			// BinderHeaders.BATCH_HEADERS is verified to be well-formed

			@SuppressWarnings("unchecked")
			List<Map<String, Object>> batchedHeaders = (List<Map<String, Object>>) message.getHeaders()
					.getOrDefault(BinderHeaders.BATCH_HEADERS,
							Collections.nCopies(smfMessages.size(), Collections.emptyMap()));

			dynamicDestinations = batchedHeaders.stream()
					.map(h -> getDynamicDestination(h, correlationKey))
					.toList();
		} else {
			XMLMessage smfMessage = xmlMessageMapper.mapToSmf(message, smfMessageWriterProperties);
			smfMessage.setCorrelationKey(correlationKey);
			smfMessages = List.of(smfMessage);
			dynamicDestinations = Collections.singletonList(getDynamicDestination(message.getHeaders(), correlationKey));
		}

		try {
			for (int i = 0; i < smfMessages.size(); i++) {
				XMLMessage smfMessage = smfMessages.get(i);
				Destination targetDestination = Objects.requireNonNullElse(dynamicDestinations.get(i), configDestination);

				LOGGER.debug("Publishing message {} of {} to destination [ {}:{} ] <message handler ID: {}>",
						i + 1, smfMessages.size(), targetDestination instanceof Topic ? "TOPIC" : "QUEUE",
						targetDestination, id);

				producer.send(smfMessage, targetDestination);
			}

			if (transactedSession != null) {
				LOGGER.debug("Committing transaction <message handler ID: {}>", id);
				transactedSession.commit();

				// Need to resolve the correlation key manually.
				// Transacted producers do not call the event handler callbacks.
				// See JCSMPStreamingPublishCorrelatingEventHandler javadocs for more info.
				producerEventHandler.responseReceivedEx(correlationKey);
			}
		} catch (JCSMPException e) {
			if (transactedSession != null) {
				try {
					if (!(e instanceof RollbackException)) {
						LOGGER.debug("Rolling back transaction <message handler ID: {}>", id);
						transactedSession.rollback();
					}
				} catch (JCSMPException ex) {
					LOGGER.debug("Failed to rollback transaction", ex);
					e.addSuppressed(ex);
				} finally {
					// Need to resolve the correlation key manually.
					// Transacted producers do not call the event handler callbacks.
					// See JCSMPStreamingPublishCorrelatingEventHandler javadocs for more info.
					producerEventHandler.handleErrorEx(correlationKey, e, System.currentTimeMillis());
				}
			}

			// DATAGO-134580: when the broker tears down the publisher flow (unsolicited
			// CloseFlow on message-spool shutdown, DR failover, etc.), JCSMP can surface
			// the failure on send(...) in three related forms - all of which mean the
			// per-binding producer reference is dead and must be rebuilt before the next
			// publish:
			//   - StaleSessionException ........ the typical form once JCSMP has propagated
			//                                    its internal stale-marker to the send caller
			//   - JCSMPTransportException ...... the raw transport-level form, observed when
			//                                    JCSMP delivers the CloseFlow event before
			//                                    the stale-marker reaches send() (see
			//                                    JCSMPProducerCloseFlowRecoveryIT - the
			//                                    bug-witness assertion accepts either form
			//                                    via the JCSMPException supertype because
			//                                    both have been observed against the broker)
			//   - ClosedFacilityException ...... the already-closed-locally signal, e.g.
			//                                    a redundant send after the producer has
			//                                    already been marked closed
			// Recreating the producer on a non-CloseFlow JCSMPTransportException is harmless:
			// the new producer inherits the still-good JCSMPSession, and if the transport
			// fault is at session level the recreate itself will fail and surface through
			// the error channel via handleMessagingException - same path as a normal failure.
			// The volatile write must happen before the throw below so a concurrent thread
			// reading the flag sees the update.
			if (e instanceof StaleSessionException
					|| e instanceof JCSMPTransportException
					|| e instanceof ClosedFacilityException) {
				if (!producerNeedsRecreation) {
					LOGGER.warn("Detected stale JCSMP producer for binding {} (cause: {}); will " +
									"recreate on next message <message handler ID: {}>",
							properties.getBindingName(), e.getClass().getSimpleName(), id);
				}
				producerNeedsRecreation = true;
			}

			throw handleMessagingException(correlationKey, "Unable to send message(s) to destination", e);
		} finally {
			if (solaceMeterAccessor != null) {
				for (XMLMessage smfMessage : smfMessages) {
					solaceMeterAccessor.recordMessage(properties.getBindingName(), smfMessage);
				}
			}
		}
	}

	private Destination getDynamicDestination(Map<String, Object> headers, ErrorChannelSendingCorrelationKey correlationKey) {
		try {
			String dynamicDestName;
			String targetDestinationHeader = StaticMessageHeaderMapAccessor.get(headers,
					BinderHeaders.TARGET_DESTINATION, String.class);
			if (StringUtils.hasText(targetDestinationHeader)) {
				dynamicDestName = targetDestinationHeader.trim();
			} else {
				return null;
			}

			String targetDestinationTypeHeader = StaticMessageHeaderMapAccessor.get(headers,
					SolaceBinderHeaders.TARGET_DESTINATION_TYPE, String.class);
			if (StringUtils.hasText(targetDestinationTypeHeader)) {
				targetDestinationTypeHeader = targetDestinationTypeHeader.trim().toUpperCase();
				if (targetDestinationTypeHeader.equals(DestinationType.TOPIC.name())) {
					return JCSMPFactory.onlyInstance().createTopic(dynamicDestName);
				} else if (targetDestinationTypeHeader.equals(DestinationType.QUEUE.name())) {
					return JCSMPFactory.onlyInstance().createQueue(dynamicDestName);
				} else {
					throw new IllegalArgumentException(String.format("Incorrect value specified for header '%s'. Expected [ %s|%s ] but actual value is [ %s ]",
							SolaceBinderHeaders.TARGET_DESTINATION_TYPE, DestinationType.TOPIC.name(), DestinationType.QUEUE.name(), targetDestinationTypeHeader));
				}
			}

			//No dynamic destinationType present so use configured destinationType
			return configDestinationType == DestinationType.TOPIC ?
					JCSMPFactory.onlyInstance().createTopic(dynamicDestName) :
					JCSMPFactory.onlyInstance().createQueue(dynamicDestName);
		} catch (Exception e) {
			throw handleMessagingException(correlationKey, "Unable to parse headers", e);
		}
	}

	@Override
	public void start() {
		LOGGER.info("Creating producer to {} {} <message handler ID: {}>", configDestinationType, configDestination.getName(), id);
		if (isRunning()) {
			LOGGER.warn("Nothing to do, message handler {} is already running", id);
			return;
		}
		// Clear any stale recreation flag left over from a prior lifecycle.
		producerNeedsRecreation = false;

		try {
			Map<String, String> headerNameMapping = properties.getExtension().getHeaderNameMapping();
			if (headerNameMapping != null && !headerNameMapping.isEmpty()) {
				Set<String> uniqueTargetHeaderNames = new HashSet<>(headerNameMapping.values());
				if (uniqueTargetHeaderNames.size() < headerNameMapping.size()) {
					IllegalArgumentException exception = new IllegalArgumentException(String.format(
							"Two or more headers map to the same header name in headerNameMapping %s <outbound adapter %s>",
							properties.getExtension().getHeaderNameMapping(), id));
					LOGGER.warn(exception.getMessage());
					throw exception;
				}
			}

			producerManager.get(id);
			createProducerInternal();
		} catch (Exception e) {
			String msg = String.format("Unable to get a message producer for session %s", jcsmpSession.getSessionName());
			LOGGER.warn(msg, e);
			closeResources();
			throw new RuntimeException(msg, e);
		}

		isRunning = true;
	}

	/**
	 * Builds the per-binding {@link XMLMessageProducer} (and the underlying
	 * {@link TransactedSession} when the binding is transacted) using the same flow
	 * properties and event handler as the initial {@link #start()} call. Extracted from
	 * {@code start()} so both the initial-create path and the post-CloseFlow recreate path
	 * share one implementation - the alternative would be duplicating the
	 * transacted/non-transacted branch in two places.
	 */
	private void createProducerInternal() throws JCSMPException {
		if (properties.getExtension().isTransacted()) {
			LOGGER.info("Creating transacted session  <message handler ID: {}>", id);
			transactedSession = jcsmpSession.createTransactedSession();
			producer = transactedSession.createProducer(SolaceProvisioningUtil.getProducerFlowProperties(jcsmpSession),
					producerEventHandler);
		} else {
			producer = jcsmpSession.createProducer(SolaceProvisioningUtil.getProducerFlowProperties(jcsmpSession),
					producerEventHandler);
		}
	}

	/**
	 * DATAGO-134580: if a prior {@code producer.send(...)} surfaced a
	 * {@link StaleSessionException}, {@link JCSMPTransportException}, or
	 * {@link ClosedFacilityException}, the broker has torn down our publisher flow
	 * (typically via unsolicited CloseFlow) and the local producer reference points at a
	 * terminally closed instance. Close the dead producer (and the dead transacted session,
	 * if any) and build fresh ones via {@link #createProducerInternal()} so the next
	 * publish can succeed.
	 *
	 * <p>Double-checked-locked so concurrent dispatcher threads do not all recreate. The
	 * lock guards close + create only; once the flag is cleared, subsequent
	 * {@link #handleMessage(Message)} calls fall through to the publish path without
	 * acquiring the lock.
	 *
	 * <p>If recreation itself fails (e.g. the broker is still mid-restart from the spool
	 * shutdown), the flag stays {@code true}, the current in-flight message is surfaced
	 * via {@link #handleMessagingException} the same way the original failing send was,
	 * and the next inbound message retries recreation. No internal retry loop, no new
	 * threads.
	 */
	private void recreateProducerIfNeeded(ErrorChannelSendingCorrelationKey correlationKey) throws MessagingException {
		if (!producerNeedsRecreation) return;
		synchronized (recreateLock) {
			if (!producerNeedsRecreation) return;
			LOGGER.warn("Recreating JCSMP producer for binding {} after stale-flow detection <message handler ID: {}>",
					properties.getBindingName(), id);
			try {
				if (producer != null) producer.close();
			} catch (Exception closeError) {
				LOGGER.debug("Failed to close stale producer during recreation <message handler ID: {}>", id, closeError);
			}
			try {
				if (transactedSession != null) transactedSession.close();
			} catch (Exception closeError) {
				LOGGER.debug("Failed to close stale transacted session during recreation <message handler ID: {}>", id, closeError);
			}
			try {
				createProducerInternal();
				producerNeedsRecreation = false;
			} catch (JCSMPException createError) {
				throw handleMessagingException(correlationKey,
						"Failed to recreate JCSMP producer after stale-flow detection", createError);
			}
		}
	}

	@Override
	public void stop() {
		if (!isRunning()) return;
		closeResources();
		isRunning = false;
	}

	private void closeResources() {
		LOGGER.info("Stopping producer to {} {} <message handler ID: {}>", configDestinationType, configDestination.getName(), id);
		// Clear any pending recreation request so a stop/start cycle starts from a clean state.
		producerNeedsRecreation = false;
		if (producer != null) {
			LOGGER.info("Closing producer <message handler ID: {}>", id);
			producer.close();
		}
		if (transactedSession != null) {
			LOGGER.info("Closing transacted session <message handler ID: {}>", id);
			transactedSession.close();
		}
		producerManager.release(id);
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	public void setErrorMessageStrategy(ErrorMessageStrategy errorMessageStrategy) {
		this.errorMessageStrategy = errorMessageStrategy;
	}

	private MessagingException handleMessagingException(ErrorChannelSendingCorrelationKey key, String msg, Exception e)
			throws MessagingException {
		LOGGER.warn(msg, e);
		return key.send(msg, e);
	}

	public String getBindingName() {
		return properties.getBindingName();
	}

	public SmfMessageWriterProperties getSmfMessageWriterProperties() {
		return smfMessageWriterProperties;
	}
}
