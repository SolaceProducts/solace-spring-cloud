package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class RetryableInboundXMLMessageListener extends InboundXMLMessageListener {
	private final RetryTemplate retryTemplate;
	private final RecoveryCallback<?> recoveryCallback;

	private static final Log logger = LogFactory.getLog(RetryableInboundXMLMessageListener.class);

	RetryableInboundXMLMessageListener(FlowReceiverContainer flowReceiverContainer,
									   ConsumerDestination consumerDestination,
									   Consumer<Message<?>> messageConsumer,
									   JCSMPAcknowledgementCallbackFactory ackCallbackFactory,
									   RetryTemplate retryTemplate,
									   RecoveryCallback<?> recoveryCallback,
									   @Nullable AtomicBoolean remoteStopFlag,
									   ThreadLocal<AttributeAccessor> attributesHolder) {
		super(flowReceiverContainer, consumerDestination, messageConsumer, ackCallbackFactory, remoteStopFlag,
				attributesHolder, false, true);
		this.retryTemplate = retryTemplate;
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	void handleMessage(BytesXMLMessage bytesXMLMessage, AcknowledgmentCallback acknowledgmentCallback) throws SolaceAcknowledgmentException {
		Message<?> message = retryTemplate.execute((context) -> {
			attributesHolder.set(context);
			return createMessage(bytesXMLMessage, acknowledgmentCallback);
		}, (context) -> {
			recoveryCallback.recover(context);
			AckUtils.autoAck(acknowledgmentCallback);
			return null;
		});

		if (message == null) {
			return;
		}

		retryTemplate.execute((context) -> {
			attributesHolder.set(context);
			sendToConsumer(message, bytesXMLMessage);
			AckUtils.autoAck(acknowledgmentCallback);
			return null;
		}, (context) -> {
			Object toReturn = recoveryCallback.recover(context);
			AckUtils.autoAck(acknowledgmentCallback);
			return toReturn;
		});
	}
}
