package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

abstract class InboundXMLMessageListener implements Runnable {
	final FlowReceiverContainer flowReceiverContainer;
	final ConsumerDestination consumerDestination;
	final ThreadLocal<AttributeAccessor> attributesHolder;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
	private final Consumer<Message<?>> messageConsumer;
	private final JCSMPAcknowledgementCallbackFactory ackCallbackFactory;
	private final boolean needHolder;
	private final boolean needAttributes;
	private final AtomicBoolean stopFlag = new AtomicBoolean(false);
	private final Supplier<Boolean> remoteStopFlag;

	private static final Log logger = LogFactory.getLog(InboundXMLMessageListener.class);

	InboundXMLMessageListener(FlowReceiverContainer flowReceiverContainer,
							  ConsumerDestination consumerDestination,
							  Consumer<Message<?>> messageConsumer,
							  JCSMPAcknowledgementCallbackFactory ackCallbackFactory,
							  @Nullable AtomicBoolean remoteStopFlag,
							  ThreadLocal<AttributeAccessor> attributesHolder,
							  boolean needHolder,
							  boolean needAttributes) {
		this.flowReceiverContainer = flowReceiverContainer;
		this.consumerDestination = consumerDestination;
		this.messageConsumer = messageConsumer;
		this.ackCallbackFactory = ackCallbackFactory;
		this.remoteStopFlag = () -> remoteStopFlag != null && remoteStopFlag.get();
		this.attributesHolder = attributesHolder;
		this.needHolder = needHolder;
		this.needAttributes = needAttributes;
	}

	abstract void handleMessage(BytesXMLMessage bytesXMLMessage, AcknowledgmentCallback acknowledgmentCallback) throws SolaceAcknowledgmentException;

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
			messageContainer = flowReceiverContainer.receive();
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

		if (messageContainer == null) {
			return;
		}

		BytesXMLMessage bytesXMLMessage = messageContainer.getMessage();
		AcknowledgmentCallback acknowledgmentCallback = ackCallbackFactory.createCallback(messageContainer);

		try {
			handleMessage(bytesXMLMessage, acknowledgmentCallback);
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
				swallowStaleException(e1, bytesXMLMessage);
			}
		} finally {
			if (needHolder) {
				attributesHolder.remove();
			}
		}
	}

	Message<?> createMessage(BytesXMLMessage bytesXMLMessage, AcknowledgmentCallback acknowledgmentCallback) {
		setAttributesIfNecessary(bytesXMLMessage, acknowledgmentCallback);
		return xmlMessageMapper.map(bytesXMLMessage, acknowledgmentCallback);
	}

	void sendToConsumer(final Message<?> message, final BytesXMLMessage bytesXMLMessage) throws RuntimeException {
		setAttributesIfNecessary(bytesXMLMessage, message);
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

	private void setAttributesIfNecessary(XMLMessage xmlMessage, Message<?> message,
								  AcknowledgmentCallback acknowledgmentCallback) {
		if (needHolder) {
			attributesHolder.set(ErrorMessageUtils.getAttributeAccessor(null, null));
		}

		if (needAttributes) {
			AttributeAccessor attributes = attributesHolder.get();
			if (attributes != null) {
				attributes.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, message);
				attributes.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_RAW_MESSAGE, xmlMessage);
				attributes.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_ACKNOWLEDGMENT_CALLBACK,
						acknowledgmentCallback);
			}
		}
	}

	private void swallowStaleException(SolaceAcknowledgmentException e, BytesXMLMessage bytesXMLMessage) {
		if (ExceptionUtils.indexOfType(e, SolaceStaleMessageException.class) > -1) {
			logger.info(String.format("Cannot acknowledge stale XMLMessage %s", bytesXMLMessage.getMessageId()), e);
		} else {
			throw e;
		}
	}

	public AtomicBoolean getStopFlag() {
		return stopFlag;
	}
}
