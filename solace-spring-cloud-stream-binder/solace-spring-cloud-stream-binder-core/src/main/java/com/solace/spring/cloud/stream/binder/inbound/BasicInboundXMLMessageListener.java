package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.inbound.acknowledge.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
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
import java.util.function.Supplier;

public class BasicInboundXMLMessageListener extends InboundXMLMessageListener {
	private final BiFunction<Message<?>, RuntimeException, Boolean> errorHandlerFunction;

	private static final Log logger = LogFactory.getLog(BasicInboundXMLMessageListener.class);

	BasicInboundXMLMessageListener(FlowReceiverContainer flowReceiverContainer,
								   ConsumerDestination consumerDestination,
								   SolaceConsumerProperties consumerProperties,
								   boolean batchingEnabled,
								   Consumer<Message<?>> messageConsumer,
								   JCSMPAcknowledgementCallbackFactory ackCallbackFactory,
								   BiFunction<Message<?>, RuntimeException, Boolean> errorHandlerFunction,
								   @Nullable AtomicBoolean remoteStopFlag,
								   ThreadLocal<AttributeAccessor> attributesHolder,
								   boolean needHolderAndAttributes) {
		super(flowReceiverContainer, consumerDestination, consumerProperties, batchingEnabled, messageConsumer,
				ackCallbackFactory, remoteStopFlag, attributesHolder, needHolderAndAttributes, needHolderAndAttributes);
		this.errorHandlerFunction = errorHandlerFunction;
	}

	@Override
	void handleMessage(Supplier<Message<?>> messageSupplier, Consumer<Message<?>> sendToConsumerHandler,
							 AcknowledgmentCallback acknowledgmentCallback, boolean isBatched)
			throws SolaceAcknowledgmentException {
		Message<?> message;
		try {
			message = messageSupplier.get();
		} catch (RuntimeException e) {
			boolean processedByErrorHandler = errorHandlerFunction != null && errorHandlerFunction.apply(null, e);
			if (processedByErrorHandler) {
				AckUtils.autoAck(acknowledgmentCallback);
			} else {
				logger.warn(String.format("Failed to map %s to a Spring Message and no error channel " +
						"was configured. Message will be rejected.", isBatched ? "a batch of XMLMessages" :
						"an XMLMessage"), e);
				AckUtils.reject(acknowledgmentCallback);
			}
			return;
		}

		sendToConsumerHandler.accept(message);
		AckUtils.autoAck(acknowledgmentCallback);
	}
}
