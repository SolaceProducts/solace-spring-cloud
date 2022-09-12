package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.inbound.BatchCollector;
import com.solace.spring.cloud.stream.binder.inbound.JCSMPInboundChannelAdapter;
import com.solace.spring.cloud.stream.binder.inbound.JCSMPMessageSource;
import com.solace.spring.cloud.stream.binder.meter.SolaceMessageMeterBinder;
import com.solace.spring.cloud.stream.binder.outbound.JCSMPOutboundMessageHandler;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceConsumerDestination;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.RetryableTaskService;
import com.solace.spring.cloud.stream.binder.util.SolaceErrorMessageHandler;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessage;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.util.List;
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
	private final Context jcsmpContext;
	private final JCSMPSessionProducerManager sessionProducerManager;
	private final AtomicBoolean consumersRemoteStopFlag = new AtomicBoolean(false);
	private final String errorHandlerProducerKey = UUID.randomUUID().toString();
	private SolaceMessageMeterBinder solaceMessageMeterBinder;
	private SolaceExtendedBindingProperties extendedBindingProperties = new SolaceExtendedBindingProperties();

	private final RetryableTaskService taskService = new RetryableTaskService();

	private static final SolaceMessageHeaderErrorMessageStrategy errorMessageStrategy = new SolaceMessageHeaderErrorMessageStrategy();

	public SolaceMessageChannelBinder(JCSMPSession jcsmpSession, SolaceQueueProvisioner solaceQueueProvisioner) {
		this(jcsmpSession, null, solaceQueueProvisioner);
	}
	public SolaceMessageChannelBinder(JCSMPSession jcsmpSession, Context jcsmpContext, SolaceQueueProvisioner solaceQueueProvisioner) {
		super(new String[0], solaceQueueProvisioner);
		this.jcsmpSession = jcsmpSession;
		this.jcsmpContext = jcsmpContext;
		this.sessionProducerManager = new JCSMPSessionProducerManager(jcsmpSession);
	}

	@Override
	public void destroy() {
		logger.info(String.format("Closing JCSMP session %s", jcsmpSession.getSessionName()));
		if (taskService != null) taskService.close();
		sessionProducerManager.release(errorHandlerProducerKey);
		consumersRemoteStopFlag.set(true);
		jcsmpSession.closeSession();
		if (jcsmpContext != null) {
			jcsmpContext.destroy();
		}
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
														  ExtendedProducerProperties<SolaceProducerProperties> producerProperties,
														  MessageChannel errorChannel) {
		JCSMPOutboundMessageHandler handler = new JCSMPOutboundMessageHandler(
				destination, jcsmpSession, errorChannel, sessionProducerManager, producerProperties.getExtension());

		if (errorChannel != null) {
			handler.setErrorMessageStrategy(errorMessageStrategy);
		}

		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
													 ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		SolaceConsumerDestination solaceDestination = (SolaceConsumerDestination) destination;

		JCSMPInboundChannelAdapter adapter = new JCSMPInboundChannelAdapter(
				solaceDestination,
				jcsmpSession,
				taskService,
				properties,
				getConsumerEndpointProperties(properties),
				solaceMessageMeterBinder);

		adapter.setRemoteStopFlag(consumersRemoteStopFlag);
		adapter.setPostStart(getConsumerPostStart(solaceDestination, properties));

		if (properties.getExtension().isAutoBindErrorQueue()) {
			adapter.setErrorQueueInfrastructure(new ErrorQueueInfrastructure(
					sessionProducerManager,
					errorHandlerProducerKey,
					solaceDestination.getErrorQueueName(),
					properties.getExtension(),
					taskService));
		}

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

		SolaceConsumerDestination solaceDestination = (SolaceConsumerDestination) destination;

		EndpointProperties endpointProperties = getConsumerEndpointProperties(consumerProperties);
		JCSMPMessageSource messageSource = new JCSMPMessageSource(solaceDestination,
				jcsmpSession,
				consumerProperties.isBatchMode() ? new BatchCollector(consumerProperties.getExtension()) : null,
				taskService,
				consumerProperties,
				endpointProperties,
				solaceMessageMeterBinder);

		messageSource.setRemoteStopFlag(consumersRemoteStopFlag::get);
		messageSource.setPostStart(getConsumerPostStart(solaceDestination, consumerProperties));

		if (consumerProperties.getExtension().isAutoBindErrorQueue()) {
			messageSource.setErrorQueueInfrastructure(new ErrorQueueInfrastructure(sessionProducerManager,
					errorHandlerProducerKey,
					solaceDestination.getErrorQueueName(),
					consumerProperties.getExtension(),
					taskService));
		}

		ErrorInfrastructure errorInfra = registerErrorInfrastructure(destination, group, consumerProperties, true);
		return new PolledConsumerResources(messageSource, errorInfra);
	}

	@Override
	protected void postProcessPollableSource(DefaultPollableMessageSource bindingTarget) {
		bindingTarget.setAttributesProvider((accessor, message) -> {
			Object sourceData = StaticMessageHeaderAccessor.getSourceData(message);
			if (sourceData == null || sourceData instanceof XMLMessage || sourceData instanceof List) {
				accessor.setAttribute(SolaceMessageHeaderErrorMessageStrategy.ATTR_SOLACE_RAW_MESSAGE, sourceData);
			}
		});
	}

	@Override
	protected MessageHandler getErrorMessageHandler(ConsumerDestination destination, String group,
													ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties) {
			return new SolaceErrorMessageHandler();
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
		SolaceConsumerDestination solaceDestination = (SolaceConsumerDestination) destination;
		StringBuilder errorsBaseName = new StringBuilder(solaceDestination.getBindingDestinationName()).append('.');
		if (solaceDestination.isTemporary()) {
			errorsBaseName.append("anon").append('.');
		}
		return errorsBaseName.append(solaceDestination.getPhysicalGroupName()).append(".errors").toString();
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

	public void setSolaceMessageMeterBinder(SolaceMessageMeterBinder solaceMessageMeterBinder) {
		this.solaceMessageMeterBinder = solaceMessageMeterBinder;
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
	private Consumer<Queue> getConsumerPostStart(SolaceConsumerDestination destination,
												 ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		return (queue) -> {
			provisioningProvider.addSubscriptionToQueue(queue, destination.getBindingDestinationName(), properties.getExtension(), true);

			//Process additional subscriptions
			for (String subscription : destination.getAdditionalSubscriptions()) {
				provisioningProvider.addSubscriptionToQueue(queue, subscription, properties.getExtension(), false);
			}
		};
	}
}
