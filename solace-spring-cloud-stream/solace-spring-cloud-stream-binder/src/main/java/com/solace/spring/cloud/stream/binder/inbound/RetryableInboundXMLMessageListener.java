package com.solace.spring.cloud.stream.binder.inbound;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.messaging.Message;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;

import java.util.function.Consumer;
import java.util.function.Function;

class RetryableInboundXMLMessageListener extends InboundXMLMessageListener implements RetryListener {
	private final RetryTemplate retryTemplate;
	private final RecoveryCallback<?> recoveryCallback;

	private static final Log logger = LogFactory.getLog(RetryableInboundXMLMessageListener.class);

	RetryableInboundXMLMessageListener(ConsumerDestination consumerDestination,
									   Consumer<Message<?>> messageConsumer,
									   Function<RuntimeException,Boolean> errorHandlerFunction,
									   RetryTemplate retryTemplate,
									   RecoveryCallback<?> recoveryCallback,
									   ThreadLocal<AttributeAccessor> attributesHolder) {
		super(consumerDestination, messageConsumer, errorHandlerFunction, attributesHolder, false, true);
		this.retryTemplate = retryTemplate;
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	void handleMessage(final Message<?> message, final BytesXMLMessage bytesXMLMessage) {
		retryTemplate.execute((context) -> {
			sendToConsumer(message, bytesXMLMessage);
			AckUtils.autoAck(StaticMessageHeaderAccessor.getAcknowledgmentCallback(message));
			return null;
		}, (context) -> {
			setAttributesIfNecessary(bytesXMLMessage, null);
			Object toReturn = recoveryCallback.recover(context);
			AckUtils.autoNack(StaticMessageHeaderAccessor.getAcknowledgmentCallback(message));
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
