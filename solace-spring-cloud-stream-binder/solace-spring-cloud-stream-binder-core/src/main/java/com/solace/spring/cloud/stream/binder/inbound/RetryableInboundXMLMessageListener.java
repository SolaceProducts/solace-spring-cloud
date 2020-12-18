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
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;

class RetryableInboundXMLMessageListener extends InboundXMLMessageListener implements RetryListener {
	private final RetryTemplate retryTemplate;
	private final RecoveryCallback<?> recoveryCallback;

	private static final Log logger = LogFactory.getLog(RetryableInboundXMLMessageListener.class);

	RetryableInboundXMLMessageListener(FlowReceiverContainer flowReceiverContainer,
									   ConsumerDestination consumerDestination,
									   Consumer<Message<?>> messageConsumer,
									   JCSMPAcknowledgementCallbackFactory ackCallbackFactory,
									   BiFunction<Message<?>, RuntimeException, Boolean> errorHandlerFunction,
									   RetryTemplate retryTemplate,
									   RecoveryCallback<?> recoveryCallback,
									   @Nullable AtomicBoolean remoteStopFlag,
									   ThreadLocal<AttributeAccessor> attributesHolder) {
		super(flowReceiverContainer, consumerDestination, messageConsumer, ackCallbackFactory, errorHandlerFunction,
				remoteStopFlag, attributesHolder, false, true);
		this.retryTemplate = retryTemplate;
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	void handleMessage(BytesXMLMessage bytesXMLMessage, AcknowledgmentCallback acknowledgmentCallback) {
		retryTemplate.execute((context) -> {
			sendToConsumer(createMessage(bytesXMLMessage, acknowledgmentCallback), bytesXMLMessage);
			AckUtils.autoAck(acknowledgmentCallback);
			return null;
		}, (context) -> {
			Object toReturn = recoveryCallback.recover(context);
			AckUtils.autoAck(acknowledgmentCallback);
			return toReturn;
		});
	}

	@Override
	public <T, E extends Throwable> boolean open(RetryContext retryContext, RetryCallback<T, E> retryCallback) {
		if (recoveryCallback != null) {
			attributesHolder.set(retryContext);
		}
		return true;
	}

	@Override
	public <T, E extends Throwable> void close(RetryContext retryContext, RetryCallback<T, E> retryCallback,
											   Throwable throwable) {
		attributesHolder.remove();
	}

	@Override
	public <T, E extends Throwable> void onError(RetryContext retryContext, RetryCallback<T, E> retryCallback,
												 Throwable throwable) {
		logger.warn(String.format("Failed to consume a message from destination %s - attempt %s",
				consumerDestination.getName(),
				retryContext.getRetryCount()));
	}
}
