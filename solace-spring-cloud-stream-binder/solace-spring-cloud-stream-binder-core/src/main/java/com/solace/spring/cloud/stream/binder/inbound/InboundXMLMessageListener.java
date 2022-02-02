package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.inbound.acknowledge.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solace.spring.cloud.stream.binder.util.SolaceBatchAcknowledgementException;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solace.spring.cloud.stream.binder.util.SolaceStaleMessageException;
import com.solace.spring.cloud.stream.binder.util.UnboundFlowReceiverContainerException;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.RequeueCurrentMessageException;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

abstract class InboundXMLMessageListener implements Runnable {
	final FlowReceiverContainer flowReceiverContainer;
	final ConsumerDestination consumerDestination;
	private final SolaceConsumerProperties consumerProperties;
	private final boolean batchingEnabled;
	final ThreadLocal<AttributeAccessor> attributesHolder;
	private final List<MessageContainer> batchedMessages;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private final Consumer<Message<?>> messageConsumer;
	private final JCSMPAcknowledgementCallbackFactory ackCallbackFactory;
	private long timeSentLastBatch = System.currentTimeMillis();
	private final boolean needHolder;
	private final boolean needAttributes;
	private final AtomicBoolean stopFlag = new AtomicBoolean(false);
	private final Supplier<Boolean> remoteStopFlag;

	private static final Log logger = LogFactory.getLog(InboundXMLMessageListener.class);

	InboundXMLMessageListener(FlowReceiverContainer flowReceiverContainer,
							  ConsumerDestination consumerDestination,
							  SolaceConsumerProperties consumerProperties,
							  boolean batchingEnabled,
							  Consumer<Message<?>> messageConsumer,
							  JCSMPAcknowledgementCallbackFactory ackCallbackFactory,
							  @Nullable AtomicBoolean remoteStopFlag,
							  ThreadLocal<AttributeAccessor> attributesHolder,
							  boolean needHolder,
							  boolean needAttributes) {
		this.flowReceiverContainer = flowReceiverContainer;
		this.consumerDestination = consumerDestination;
		this.consumerProperties = consumerProperties;
		this.batchingEnabled = batchingEnabled;
		this.messageConsumer = messageConsumer;
		this.ackCallbackFactory = ackCallbackFactory;
		this.remoteStopFlag = () -> remoteStopFlag != null && remoteStopFlag.get();
		this.attributesHolder = attributesHolder;
		this.needHolder = needHolder;
		this.needAttributes = needAttributes;
		this.batchedMessages = batchingEnabled ? new ArrayList<>(consumerProperties.getBatchMaxSize()) :
				Collections.emptyList();
	}

	abstract void handleMessage(Supplier<Message<?>> messageSupplier, Consumer<Message<?>> sendToConsumerHandler,
								AcknowledgmentCallback acknowledgmentCallback, boolean isBatched) throws SolaceAcknowledgmentException;

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
		} finally {
			logger.info(String.format("Closing flow receiver to destination %s", consumerDestination.getName()));
			flowReceiverContainer.unbind();
		}
	}

	private boolean keepPolling() {
		return !stopFlag.get() && !remoteStopFlag.get();
	}

	private void receive() throws UnboundFlowReceiverContainerException {
		MessageContainer messageContainer;

		try {
			if (batchingEnabled && consumerProperties.getBatchTimeout() > 0) {
				messageContainer = flowReceiverContainer.receive(consumerProperties.getBatchTimeout());
			} else {
				messageContainer = flowReceiverContainer.receive();
			}
		} catch (JCSMPException e) {
			String msg = String.format("Received error while trying to read message from endpoint %s",
					flowReceiverContainer.getQueueName());
			if ((e instanceof JCSMPTransportException || e instanceof ClosedFacilityException) && !keepPolling()) {
				logger.debug(msg, e);
			} else {
				logger.warn(msg, e);
			}
			return;
		}

		try {
			if (batchingEnabled) {
				if (messageContainer != null) {
					batchedMessages.add(messageContainer);
				}
				processBatchIfNecessary();
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
			swallowStaleException(e, bytesXMLMessage);
		} catch (Exception e) {
			try {
				if (ExceptionUtils.indexOfType(e, RequeueCurrentMessageException.class) > -1) {
					logger.warn(String.format(
							"Exception thrown while processing XMLMessage %s. Message will be requeued.",
							bytesXMLMessage.getMessageId()), e);
					AckUtils.requeue(acknowledgmentCallback);
				} else {
					logger.warn(String.format(
							"Exception thrown while processing XMLMessage %s. Message will be rejected.",
							bytesXMLMessage.getMessageId()), e);
					AckUtils.reject(acknowledgmentCallback);
				}
			} catch (SolaceAcknowledgmentException e1) {
				e1.addSuppressed(e);
				swallowStaleException(e1, bytesXMLMessage);
			}
		}
	}

	private void processBatchIfNecessary() {
		if (batchedMessages.size() < consumerProperties.getBatchMaxSize()) {
			long batchTimeDiff = System.currentTimeMillis() - timeSentLastBatch;
			if (batchedMessages.size() == 0 || consumerProperties.getBatchTimeout() == 0 ||
					batchTimeDiff < consumerProperties.getBatchTimeout()) {
				if (logger.isTraceEnabled()) {
					logger.trace(String.format("Collecting batch... Size: %s, Time since last batch: %s ms",
							batchedMessages.size(), batchTimeDiff));
				}
				return;
			} else if (logger.isTraceEnabled()) {
				logger.trace(String.format("Batch timeout reached, processing batch of %s messages...",
						batchedMessages.size()));
			}
		} else if (logger.isTraceEnabled()) {
			logger.trace(String.format("Max batch size reached, processing batch of %s messages...",
					batchedMessages.size()));
		}

		AcknowledgmentCallback acknowledgmentCallback = ackCallbackFactory.createBatchCallback(batchedMessages);
		try {
			List<BytesXMLMessage> xmlMessages = batchedMessages.stream()
					.map(MessageContainer::getMessage)
					.collect(Collectors.toList());
			handleMessage(() -> createBatchMessage(xmlMessages, acknowledgmentCallback),
					m -> sendBatchToConsumer(m, xmlMessages),
					acknowledgmentCallback,
					true);
		} catch (Exception e) {
			if (e instanceof SolaceBatchAcknowledgementException && ((SolaceBatchAcknowledgementException) e)
					.isAllStaleExceptions()) {
				logger.info("Cannot acknowledge batch, all messages are stale", e);
			} else {
				try {
					if (ExceptionUtils.indexOfType(e, RequeueCurrentMessageException.class) > -1) {
						if (logger.isWarnEnabled()) {
							logger.warn("Exception thrown while processing batch. Batch's message will be requeued.",
									e);
						}
						AckUtils.requeue(acknowledgmentCallback);
					} else {
						if (logger.isWarnEnabled()) {
							logger.warn("Exception thrown while processing batch. Batch's messages will be rejected.",
									e);
						}
						AckUtils.reject(acknowledgmentCallback);
					}
				} catch (SolaceAcknowledgmentException e1) {
					e1.addSuppressed(e);
					if (e1 instanceof SolaceBatchAcknowledgementException && ((SolaceBatchAcknowledgementException) e1)
							.isAllStaleExceptions()) {
						logger.info("Cannot acknowledge batch, all messages are stale", e1);
					} else {
						throw e;
					}
				}
			}
		} finally {
			timeSentLastBatch = System.currentTimeMillis();
			batchedMessages.clear();
		}
	}

	Message<?> createOneMessage(BytesXMLMessage bytesXMLMessage, AcknowledgmentCallback acknowledgmentCallback) {
		setAttributesIfNecessary(bytesXMLMessage, acknowledgmentCallback);
		return xmlMessageMapper.map(bytesXMLMessage, acknowledgmentCallback);
	}

	Message<?> createBatchMessage(List<BytesXMLMessage> bytesXMLMessages,
								  AcknowledgmentCallback acknowledgmentCallback) {
		setAttributesIfNecessary(bytesXMLMessages, acknowledgmentCallback);
		return xmlMessageMapper.mapBatchMessage(bytesXMLMessages, acknowledgmentCallback);
	}

	void sendOneToConsumer(final Message<?> message, final BytesXMLMessage bytesXMLMessage)
			throws RuntimeException {
		setAttributesIfNecessary(bytesXMLMessage, message);
		sendToConsumer(message);
	}

	void sendBatchToConsumer(final Message<?> message, final List<BytesXMLMessage> bytesXMLMessages)
			throws RuntimeException {
		setAttributesIfNecessary(bytesXMLMessages, message);
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

	void setAttributesIfNecessary(List<? extends XMLMessage> xmlMessages,
								  AcknowledgmentCallback acknowledgmentCallback) {
		setAttributesIfNecessary(xmlMessages, null, acknowledgmentCallback);
	}

	void setAttributesIfNecessary(List<? extends XMLMessage> xmlMessages, Message<?> batchMessage) {
		setAttributesIfNecessary(xmlMessages, batchMessage, null);
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

	private void swallowStaleException(SolaceAcknowledgmentException e, BytesXMLMessage bytesXMLMessage) {
		if (ExceptionUtils.indexOfType(e, SolaceStaleMessageException.class) > -1) {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Cannot acknowledge stale XMLMessage %s", bytesXMLMessage.getMessageId()),
						e);
			}
		} else {
			throw e;
		}
	}

	public AtomicBoolean getStopFlag() {
		return stopFlag;
	}
}
