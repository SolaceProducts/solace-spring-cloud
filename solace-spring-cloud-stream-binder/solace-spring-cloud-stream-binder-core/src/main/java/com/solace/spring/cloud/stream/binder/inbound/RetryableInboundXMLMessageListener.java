package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.inbound.acknowledge.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.core.RecoveryCallback;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryTemplate;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

class RetryableInboundXMLMessageListener extends InboundXMLMessageListener {
	private final RetryTemplate retryTemplate;
	private final RecoveryCallback<?> recoveryCallback;

	RetryableInboundXMLMessageListener(FlowReceiverContainer flowReceiverContainer,
									   ConsumerDestination consumerDestination,
									   ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
									   @Nullable BatchCollector batchCollector,
									   Consumer<Message<?>> messageConsumer,
									   JCSMPAcknowledgementCallbackFactory ackCallbackFactory,
									   RetryTemplate retryTemplate,
									   RecoveryCallback<?> recoveryCallback,
									   @Nullable SolaceMeterAccessor solaceMeterAccessor,
									   @Nullable AtomicBoolean remoteStopFlag,
									   ThreadLocal<AttributeAccessor> attributesHolder) {
		super(flowReceiverContainer,
				consumerDestination,
				consumerProperties,
				batchCollector,
				messageConsumer,
				ackCallbackFactory,
				solaceMeterAccessor,
				remoteStopFlag,
				attributesHolder,
				false,
				true);
		this.retryTemplate = retryTemplate;
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	void handleMessage(Supplier<Message<?>> messageSupplier, Consumer<Message<?>> sendToConsumerHandler,
								 AcknowledgmentCallback acknowledgmentCallback, boolean isBatched)
			throws SolaceAcknowledgmentException {
		try {
			Message<?> message;
			try {
				attributesHolder.set(ErrorMessageUtils.getAttributeAccessor(null, null));
				message = retryTemplate.execute(() -> messageSupplier.get());
			}
			catch (RetryException ex) {
				if (recoveryCallback != null) {
					recoveryCallback.recover(attributesHolder.get(), ex.getCause());
				}
				AckUtils.autoAck(acknowledgmentCallback);
				return;
			}

			if (message == null) {
				return;
			}

			try {
				attributesHolder.set(ErrorMessageUtils.getAttributeAccessor(message, null));
				retryTemplate.execute(() -> {
					sendToConsumerHandler.accept(message);
					AckUtils.autoAck(acknowledgmentCallback);
					return null;
				});
			}
			catch (RetryException ex) {
				if (recoveryCallback != null) {
					recoveryCallback.recover(attributesHolder.get(), ex.getCause());
				}
				AckUtils.autoAck(acknowledgmentCallback);
			}
		} finally {
			attributesHolder.remove();
		}
	}
}
