package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.inbound.JCSMPInboundChannelAdapter;
import com.solace.spring.cloud.stream.binder.inbound.JCSMPMessageSource;
import com.solace.spring.cloud.stream.binder.outbound.JCSMPOutboundMessageHandler;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.SolaceErrorMessageHandler;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageConversionException;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solace.spring.cloud.stream.binder.util.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SolaceMessageChannelBinder
		extends AbstractMessageChannelBinder<
						ExtendedConsumerProperties<SolaceConsumerProperties>,
						ExtendedProducerProperties<SolaceProducerProperties>,
						SolaceQueueProvisioner>
		implements ExtendedPropertiesBinder<MessageChannel, SolaceConsumerProperties, SolaceProducerProperties>,
				DisposableBean {

	private final JCSMPSession jcsmpSession;
	private final JCSMPSessionProducerManager sessionProducerManager;
	private final AtomicBoolean consumersRemoteStopFlag = new AtomicBoolean(false);
	private final String errorHandlerProducerKey = UUID.randomUUID().toString();
	private SolaceExtendedBindingProperties extendedBindingProperties = new SolaceExtendedBindingProperties();
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final SolaceMessageHeaderErrorMessageStrategy errorMessageStrategy = new SolaceMessageHeaderErrorMessageStrategy();

	public SolaceMessageChannelBinder(JCSMPSession jcsmpSession, SolaceQueueProvisioner solaceQueueProvisioner) {
		super(new String[0], solaceQueueProvisioner);
		this.jcsmpSession = jcsmpSession;
		this.sessionProducerManager = new JCSMPSessionProducerManager(jcsmpSession);
	}

	@Override
	public void destroy() {
		logger.info(String.format("Closing JCSMP session %s", jcsmpSession.getSessionName()));
		sessionProducerManager.release(errorHandlerProducerKey);
		consumersRemoteStopFlag.set(true);
		jcsmpSession.closeSession();
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
														  ExtendedProducerProperties<SolaceProducerProperties> producerProperties,
														  MessageChannel errorChannel) {
		JCSMPOutboundMessageHandler handler = new JCSMPOutboundMessageHandler(
				destination, jcsmpSession, errorChannel, sessionProducerManager);

		if (errorChannel != null) {
			handler.setErrorMessageStrategy(new DefaultErrorMessageStrategy());
		}

		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
													 ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		JCSMPInboundChannelAdapter adapter = new JCSMPInboundChannelAdapter(destination, jcsmpSession,
				properties.getConcurrency(), getConsumerEndpointProperties(properties),
				getConsumerPostStart(properties), consumersRemoteStopFlag);

		ErrorInfrastructure errorInfra = registerErrorInfrastructure(destination, group, properties);
		if (properties.getMaxAttempts() > 1) {
			adapter.setRetryTemplate(buildRetryTemplate(properties));
			adapter.setRecoveryCallback(errorInfra.getRecoverer());
		} else {
			adapter.setErrorChannel(errorInfra.getErrorChannel());
		}

		adapter.setErrorMessageStrategy(errorMessageStrategy);
		return adapter;
	}

	@Override
	protected PolledConsumerResources createPolledConsumerResources(String name, String group,
																	ConsumerDestination destination,
																	ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties) {
		if (consumerProperties.getConcurrency() > 1) {
			logger.warn("Polled consumers do not support concurrency > 1, it will be ignored...");
		}

		EndpointProperties endpointProperties = getConsumerEndpointProperties(consumerProperties);
		Consumer<Queue> postStart = getConsumerPostStart(consumerProperties);
		JCSMPMessageSource messageSource = new JCSMPMessageSource(destination, jcsmpSession, consumerProperties, endpointProperties, postStart);
		ErrorInfrastructure errorInfra = registerErrorInfrastructure(destination, group, consumerProperties, true);
		return new PolledConsumerResources(messageSource, errorInfra);
	}

	@Override
	protected void postProcessPollableSource(DefaultPollableMessageSource bindingTarget) {
		bindingTarget.setAttributesProvider((accessor, message) -> {
			Object rawMessage = message.getHeaders().get(SolaceMessageHeaderErrorMessageStrategy.SOLACE_RAW_MESSAGE);
			if (rawMessage != null) {
				accessor.setAttribute(SolaceMessageHeaderErrorMessageStrategy.SOLACE_RAW_MESSAGE, rawMessage);
			}
		});
	}

	@Override
	protected MessageHandler getErrorMessageHandler(ConsumerDestination destination, String group,
													ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties) {
			return new SolaceErrorMessageHandler(destination, consumerProperties, errorHandlerProducerKey, sessionProducerManager);
	}

	@Override
	protected MessageHandler getPolledConsumerErrorMessageHandler(ConsumerDestination destination, String group,
																  ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties) {
		final MessageHandler handler = getErrorMessageHandler(destination, group, consumerProperties);
		if (handler != null) {
			return handler;
		} else {
			return super.getPolledConsumerErrorMessageHandler(destination, group, consumerProperties);
		}
	}

	@Override
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return errorMessageStrategy;
	}

	@Override
	protected String errorsBaseName(ConsumerDestination destination, String group,
									ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties) {
		return destination.getName() + ".errors"; // topic.group is already included in the queue/destination's name
	}

	@Override
	public SolaceConsumerProperties getExtendedConsumerProperties(String channelName) {
		return extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public SolaceProducerProperties getExtendedProducerProperties(String channelName) {
		return extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	public String getDefaultsPrefix() {
		return this.extendedBindingProperties.getDefaultsPrefix();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
	}

	public void setExtendedBindingProperties(SolaceExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	/**
		WORKAROUND (SOL-4272) ----------------------------------------------------------
		Temporary endpoints are only provisioned when the consumer is created.
		Ideally, these should be done within the provisioningProvider itself.
	*/
	private EndpointProperties getConsumerEndpointProperties(ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		return SolaceProvisioningUtil.getEndpointProperties(properties.getExtension());
	}

	/**
		WORKAROUND (SOL-4272) ----------------------------------------------------------
		Temporary endpoints are only provisioned when the consumer is created.
		Ideally, these should be done within the provisioningProvider itself.
	*/
	private Consumer<Queue> getConsumerPostStart(ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		return (queue) -> {
			for (String topic : provisioningProvider.getTrackedTopicsForQueue(queue.getName())) {
				provisioningProvider.addSubscriptionToQueue(queue, topic, properties.getExtension());
			}
		};
	}

	public Message<?> sendRequest(String topic, Message<?> message) throws MessagingException {
		return sendRequest(topic, message, 30_000);
	}

	public Message<?> sendRequest(String topic, Message<?> requestMsg, int timeout) throws MessagingException {
		createEmptyConsumerIfNonExists();

		BytesXMLMessage responseBytesXMLMessage;
		Topic targetTopic = JCSMPFactory.onlyInstance().createTopic(topic);
		try {
			XMLMessage xmlMessage = xmlMessageMapper.map(requestMsg, new SolaceConsumerProperties());
			xmlMessage.setDeliveryMode(DeliveryMode.DIRECT);

			final Requestor requestor = jcsmpSession.createRequestor();
			responseBytesXMLMessage = requestor.request(xmlMessage, timeout, targetTopic);

		} catch (JCSMPRequestTimeoutException e) {
			throw new MessagingException(
					requestMsg,
					String.format("Timeout of request reply message to topic %s", targetTopic.getName()),
					e);
		} catch (JCSMPException e) {
			throw new MessagingException(
					requestMsg,
					String.format("Unable to send request message to topic %s", targetTopic.getName()),
					e);
		}

		try {
			return xmlMessageMapper.map(responseBytesXMLMessage);
		} catch (SolaceMessageConversionException e) {
			throw new MessagingException(requestMsg, "Response can not be mapped", e);
		}
	}

	private void createEmptyConsumerIfNonExists() {
		if (jcsmpSession instanceof JCSMPBasicSession && ((JCSMPBasicSession)jcsmpSession).getConsumer() != null) {
			return;
		}

		try {
			final XMLMessageConsumer consumer = jcsmpSession.getMessageConsumer((XMLMessageListener) null);
			consumer.start();
		} catch (JCSMPException e) {
			throw new MessagingException("Unable to start anonymous consumer", e);
		}
	}
}
