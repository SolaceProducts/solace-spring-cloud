package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.inbound.acknowledge.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.inbound.acknowledge.SolaceAckUtil;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solace.spring.cloud.stream.binder.util.UnboundFlowReceiverContainerException;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.StaleSessionException;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.RequeueCurrentMessageException;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

abstract class InboundXMLMessageListener implements Runnable {
	final FlowReceiverContainer flowReceiverContainer;
	final ConsumerDestination consumerDestination;
	private final ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties;
	final ThreadLocal<AttributeAccessor> attributesHolder;
	private final BatchCollector batchCollector;
	private final XMLMessageMapper xmlMessageMapper;
	private final Consumer<Message<?>> messageConsumer;
	private final JCSMPAcknowledgementCallbackFactory ackCallbackFactory;
	@Nullable private final SolaceMeterAccessor solaceMeterAccessor;
	private final boolean needHolder;
	private final boolean needAttributes;
	private final AtomicBoolean stopFlag = new AtomicBoolean(false);
	private final Supplier<Boolean> remoteStopFlag;

	private static final Log logger = LogFactory.getLog(InboundXMLMessageListener.class);

	InboundXMLMessageListener(FlowReceiverContainer flowReceiverContainer,
							  ConsumerDestination consumerDestination,
							  ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
							  @Nullable BatchCollector batchCollector,
							  Consumer<Message<?>> messageConsumer,
							  JCSMPAcknowledgementCallbackFactory ackCallbackFactory,
							  @Nullable SolaceMeterAccessor solaceMeterAccessor,
							  @Nullable AtomicBoolean remoteStopFlag,
							  ThreadLocal<AttributeAccessor> attributesHolder,
							  boolean needHolder,
							  boolean needAttributes) {
		this.flowReceiverContainer = flowReceiverContainer;
		this.consumerDestination = consumerDestination;
		this.consumerProperties = consumerProperties;
		this.batchCollector = batchCollector;
		this.messageConsumer = messageConsumer;
		this.ackCallbackFactory = ackCallbackFactory;
		this.solaceMeterAccessor = solaceMeterAccessor;
		this.remoteStopFlag = () -> remoteStopFlag != null && remoteStopFlag.get();
		this.attributesHolder = attributesHolder;
		this.needHolder = needHolder;
		this.needAttributes = needAttributes;
		this.xmlMessageMapper = flowReceiverContainer.getXMLMessageMapper();
	}

	abstract void handleMessage(Supplier<Message<?>> messageSupplier, Consumer<Message<?>> sendToConsumerHandler,
								AcknowledgmentCallback acknowledgmentCallback, boolean isBatched)
			throws SolaceAcknowledgmentException;

	@Override
	public void run() {
		try {
			while (keepPolling()) {
				try {
					receive();
				} catch (RuntimeException | UnboundFlowReceiverContainerException e) {
					logger.warn(String.format("Exception received while consuming messages from destination %s",
							consumerDestination.getName()), e);
				}
			}
		} catch (StaleSessionException e) {
			logger.error("Session has lost connection", e);
		} catch (Throwable t) {
			logger.error(String.format("Received unexpected error while consuming from destination %s",
					consumerDestination.getName()), t);
			throw t;
		} finally {
			logger.info(String.format("Closing flow receiver to destination %s", consumerDestination.getName()));
			flowReceiverContainer.unbind();
		}
	}

	private boolean keepPolling() {
		return !stopFlag.get() && !remoteStopFlag.get();
	}

	private void receive() throws UnboundFlowReceiverContainerException, StaleSessionException {
		if (batchCollector != null) {
			batchCollector.resetBatchCollectionStartTimestampIfEmpty();
		}

		MessageContainer messageContainer;

		try {
			messageContainer = flowReceiverContainer.receive(25);
		} catch (StaleSessionException e) {
			throw e;
		} catch (JCSMPException e) {
			String msg = String.format("Received error while trying to read message from endpoint %s",
					flowReceiverContainer.getEndpointName());
			if ((e instanceof JCSMPTransportException || e instanceof ClosedFacilityException) && !keepPolling()) {
				logger.debug(msg, e);
			} else {
				logger.warn(msg, e);
			}
			return;
		}

		if (solaceMeterAccessor != null && messageContainer != null) {
			solaceMeterAccessor.recordMessage(consumerProperties.getBindingName(), messageContainer.getMessage());
		}

		try {
			if (batchCollector != null) {
				batchCollector.addToBatch(messageContainer);
				processBatchIfAvailable();
			} else if (messageContainer != null) {
				processMessage(messageContainer);
			}
		} finally {
			if (needHolder || needAttributes) {
				attributesHolder.remove();
			}
		}
	}

	private void processMessage(MessageContainer messageContainer) {
		BytesXMLMessage bytesXMLMessage = messageContainer.getMessage();
		AcknowledgmentCallback acknowledgmentCallback = ackCallbackFactory.createCallback(messageContainer);

		try {
			handleMessage(() -> createOneMessage(bytesXMLMessage, acknowledgmentCallback),
					m -> sendOneToConsumer(m, bytesXMLMessage),
					acknowledgmentCallback,
					false);
		} catch (SolaceAcknowledgmentException e) {
			throw e;
		} catch (Exception e) {
			try {
				if (ExceptionUtils.indexOfType(e, RequeueCurrentMessageException.class) > -1) {
					logger.warn(String.format(
							"Exception thrown while processing XMLMessage %s. Message will be requeued.",
							bytesXMLMessage.getMessageId()), e);
					AckUtils.requeue(acknowledgmentCallback);
				} else {
					logger.warn(String.format(
							"Exception thrown while processing XMLMessage %s. Message will be requeued.",
							bytesXMLMessage.getMessageId()), e);
					if (!SolaceAckUtil.republishToErrorQueue(acknowledgmentCallback)) {
						AckUtils.requeue(acknowledgmentCallback);
					}
				}
			} catch (SolaceAcknowledgmentException e1) {
				e1.addSuppressed(e);
				logger.warn(String.format("Exception thrown while re-queuing XMLMessage %s.",
						bytesXMLMessage.getMessageId()), e1);
				throw e1;
			}
		}
	}

	private void processBatchIfAvailable() {
		Optional<List<MessageContainer>> batchedMessages = batchCollector.collectBatchIfAvailable();
		if (batchedMessages.isEmpty()) {
			return;
		}

		AcknowledgmentCallback acknowledgmentCallback = consumerProperties.getExtension().isTransacted() ?
				ackCallbackFactory.createTransactedBatchCallback(batchedMessages.get(),
						flowReceiverContainer.getTransactedSession()) :
				ackCallbackFactory.createBatchCallback(batchedMessages.get());

		try {
			List<BytesXMLMessage> xmlMessages = batchedMessages.get()
					.stream()
					.map(MessageContainer::getMessage)
					.collect(Collectors.toList());
			handleMessage(() -> createBatchMessage(xmlMessages,
							// transactions are sync-only.
							// No support for user-controlled client acks via AcknowledgmentCallback
							consumerProperties.getExtension().isTransacted() ? null : acknowledgmentCallback),
					m -> sendBatchToConsumer(m, xmlMessages, acknowledgmentCallback),
					acknowledgmentCallback,
					true);
		} catch (Exception e) {
				try {
					if (ExceptionUtils.indexOfType(e, RequeueCurrentMessageException.class) > -1) {
						if (logger.isWarnEnabled()) {
							logger.warn("Exception thrown while processing batch. Batch's message will be requeued.", e);
						}
						AckUtils.requeue(acknowledgmentCallback);
					} else {
						if (logger.isWarnEnabled()) {
							logger.warn("Exception thrown while processing batch. Batch's messages will be requeued.", e);
						}
						if (!SolaceAckUtil.republishToErrorQueue(acknowledgmentCallback)) {
							AckUtils.requeue(acknowledgmentCallback);
						}
					}
				} catch (SolaceAcknowledgmentException e1) {
					e1.addSuppressed(e);
					logger.warn("Exception thrown while re-queuing batch.", e1);
					throw e1;
				}
		} finally {
			batchCollector.confirmDelivery();
		}
	}

	Message<?> createOneMessage(BytesXMLMessage bytesXMLMessage, AcknowledgmentCallback acknowledgmentCallback) {
		setAttributesIfNecessary(bytesXMLMessage, acknowledgmentCallback);
		return xmlMessageMapper.map(bytesXMLMessage, acknowledgmentCallback, consumerProperties.getExtension());
	}

	Message<?> createBatchMessage(List<BytesXMLMessage> bytesXMLMessages,
								  AcknowledgmentCallback acknowledgmentCallback) {
		setBatchAttributesIfNecessary(bytesXMLMessages, null, acknowledgmentCallback);
		return xmlMessageMapper.mapBatchMessage(bytesXMLMessages, acknowledgmentCallback, consumerProperties.getExtension());
	}

	void sendOneToConsumer(final Message<?> message, final BytesXMLMessage bytesXMLMessage)
			throws RuntimeException {
		setAttributesIfNecessary(bytesXMLMessage, message);
		sendToConsumer(message);
	}

	private void sendBatchToConsumer(
			final Message<?> message,
			final List<BytesXMLMessage> bytesXMLMessages,
			final AcknowledgmentCallback acknowledgmentCallback)
			throws RuntimeException {
		setBatchAttributesIfNecessary(bytesXMLMessages, message, acknowledgmentCallback);
		sendToConsumer(message);
	}

	private void sendToConsumer(final Message<?> message) throws RuntimeException {
		AtomicInteger deliveryAttempt = StaticMessageHeaderAccessor.getDeliveryAttempt(message);
		if (deliveryAttempt != null) {
			deliveryAttempt.incrementAndGet();
		}
		messageConsumer.accept(message);
	}

	void setAttributesIfNecessary(XMLMessage xmlMessage, AcknowledgmentCallback acknowledgmentCallback) {
		setAttributesIfNecessary(xmlMessage, null, acknowledgmentCallback);
	}

	void setAttributesIfNecessary(XMLMessage xmlMessage, Message<?> message) {
		setAttributesIfNecessary(xmlMessage, message, null);
	}

	void setBatchAttributesIfNecessary(List<? extends XMLMessage> xmlMessages,
									   @Nullable Message<?> batchMessage,
									   AcknowledgmentCallback acknowledgmentCallback) {
		if (batchMessage != null && StaticMessageHeaderAccessor.getAcknowledgmentCallback(batchMessage) != null) {
			setAttributesIfNecessary(xmlMessages, batchMessage, null);
		} else {
			setAttributesIfNecessary(xmlMessages, batchMessage, acknowledgmentCallback);
		}
	}

	private void setAttributesIfNecessary(Object rawXmlMessage, Message<?> message,
										  AcknowledgmentCallback acknowledgmentCallback) {
		if (needHolder) {
			attributesHolder.set(ErrorMessageUtils.getAttributeAccessor(null, null));
		}

		if (needAttributes) {
			AttributeAccessor attributes = attributesHolder.get();
			if (attributes != null) {
				attributes.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, message);
				attributes.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_RAW_MESSAGE, rawXmlMessage);
				attributes.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_ACKNOWLEDGMENT_CALLBACK,
						acknowledgmentCallback);
			}
		}
	}

	public AtomicBoolean getStopFlag() {
		return stopFlag;
	}
}
