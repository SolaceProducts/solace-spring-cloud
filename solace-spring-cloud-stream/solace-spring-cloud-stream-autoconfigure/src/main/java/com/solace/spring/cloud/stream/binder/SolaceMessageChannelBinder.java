package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.inbound.JCSMPInboundChannelAdapter;
import com.solace.spring.cloud.stream.binder.inbound.JCSMPMessageSource;
import com.solace.spring.cloud.stream.binder.outbound.JCSMPOutboundMessageHandler;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.SolaceErrorMessageHandler;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solace.spring.cloud.stream.binder.util.SolaceProvisioningUtil;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.util.UUID;
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
	private final String errorHandlerProducerKey = UUID.randomUUID().toString();
	private SolaceExtendedBindingProperties extendedBindingProperties = new SolaceExtendedBindingProperties();

	private static final SolaceMessageHeaderErrorMessageStrategy errorMessageStrategy = new SolaceMessageHeaderErrorMessageStrategy();

	public SolaceMessageChannelBinder(JCSMPSession jcsmpSession, SolaceQueueProvisioner solaceQueueProvisioner) {
		super(new String[0], solaceQueueProvisioner);
		this.jcsmpSession = jcsmpSession;
		this.sessionProducerManager = new JCSMPSessionProducerManager(jcsmpSession);
	}

	@Override
	public void destroy() {
		sessionProducerManager.release(errorHandlerProducerKey);
		jcsmpSession.closeSession();
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
														  ExtendedProducerProperties<SolaceProducerProperties> producerProperties,
														  MessageChannel errorChannel) {
		JCSMPOutboundMessageHandler handler = new JCSMPOutboundMessageHandler(
				destination, jcsmpSession, errorChannel, producerProperties, sessionProducerManager);

		if (errorChannel != null) {
			handler.setErrorMessageStrategy(new DefaultErrorMessageStrategy());
		}

		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
													 ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		JCSMPInboundChannelAdapter adapter = new JCSMPInboundChannelAdapter(destination, jcsmpSession,
				getConsumerEndpointProperties(properties), getConsumerPostStart());

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
		EndpointProperties endpointProperties = getConsumerEndpointProperties(consumerProperties);
		Consumer<Queue> postStart = getConsumerPostStart();
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
	private Consumer<Queue> getConsumerPostStart() {
		return (queue) -> {
			for (String topic : provisioningProvider.getTrackedTopicsForQueue(queue.getName())) {
				provisioningProvider.addSubscriptionToQueue(queue, topic);
			}
		};
	}
}
