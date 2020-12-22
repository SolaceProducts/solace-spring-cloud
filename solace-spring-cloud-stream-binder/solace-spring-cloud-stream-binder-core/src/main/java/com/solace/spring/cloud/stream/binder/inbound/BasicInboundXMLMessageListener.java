package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.JCSMPAcknowledgementCallbackFactory;
import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class BasicInboundXMLMessageListener extends InboundXMLMessageListener {
	private final BiFunction<Message<?>, RuntimeException, Boolean> errorHandlerFunction;

	private static final Log logger = LogFactory.getLog(BasicInboundXMLMessageListener.class);

	BasicInboundXMLMessageListener(FlowReceiverContainer flowReceiverContainer,
							  ConsumerDestination consumerDestination,
							  Consumer<Message<?>> messageConsumer,
							  JCSMPAcknowledgementCallbackFactory ackCallbackFactory,
							  BiFunction<Message<?>, RuntimeException, Boolean> errorHandlerFunction,
							  @Nullable AtomicBoolean remoteStopFlag,
							  ThreadLocal<AttributeAccessor> attributesHolder,
							  boolean needHolderAndAttributes) {
		super(flowReceiverContainer, consumerDestination, messageConsumer, ackCallbackFactory, remoteStopFlag,
				attributesHolder, needHolderAndAttributes, needHolderAndAttributes);
		this.errorHandlerFunction = errorHandlerFunction;
	}

	void handleMessage(BytesXMLMessage bytesXMLMessage, AcknowledgmentCallback acknowledgmentCallback) {
		Message<?> message;
		try {
			message = createMessage(bytesXMLMessage, acknowledgmentCallback);
		} catch (RuntimeException e) {
			boolean processedByErrorHandler = errorHandlerFunction != null && errorHandlerFunction.apply(null, e);
			if (processedByErrorHandler) {
				AckUtils.autoAck(acknowledgmentCallback);
			} else {
				logger.warn(String.format("Failed to map XMLMessage %s to a Spring Message and no error channel " +
						"was configured. Message will be rejected.", bytesXMLMessage.getMessageId()), e);
				AckUtils.reject(acknowledgmentCallback);
			}
			return;
		}

		sendToConsumer(message, bytesXMLMessage);
		AckUtils.autoAck(acknowledgmentCallback);
	}
}
