package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.inbound.acknowledge.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.meter.SolaceMessageMeterBinder;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
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
									   @Nullable SolaceMessageMeterBinder solaceMessageMeterBinder,
									   @Nullable AtomicBoolean remoteStopFlag,
									   ThreadLocal<AttributeAccessor> attributesHolder) {
		super(flowReceiverContainer,
				consumerDestination,
				consumerProperties,
				batchCollector,
				messageConsumer,
				ackCallbackFactory,
				solaceMessageMeterBinder,
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
		Message<?> message = retryTemplate.execute((context) -> {
			attributesHolder.set(context);
			return messageSupplier.get();
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
			sendToConsumerHandler.accept(message);
			AckUtils.autoAck(acknowledgmentCallback);
			return null;
		}, (context) -> {
			Object toReturn = recoveryCallback.recover(context);
			AckUtils.autoAck(acknowledgmentCallback);
			return toReturn;
		});
	}
}
