package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.inbound.BatchCollector;
import com.solace.spring.cloud.stream.binder.inbound.JCSMPInboundChannelAdapter;
import com.solace.spring.cloud.stream.binder.inbound.JCSMPMessageSource;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.outbound.JCSMPOutboundMessageHandler;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceConsumerDestination;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceEndpointProvisioner;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.util.DefaultSolaceSessionManager;
import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.SolaceErrorMessageHandler;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solace.spring.cloud.stream.binder.util.SolaceSessionManager;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
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
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SolaceMessageChannelBinder
		extends AbstractMessageChannelBinder<
						ExtendedConsumerProperties<SolaceConsumerProperties>,
						ExtendedProducerProperties<SolaceProducerProperties>,
		SolaceEndpointProvisioner>
		implements ExtendedPropertiesBinder<MessageChannel, SolaceConsumerProperties, SolaceProducerProperties>,
				DisposableBean {

	private final SolaceSessionManager solaceSessionManager;
	private final JCSMPSessionProducerManager sessionProducerManager;
	private final AtomicBoolean consumersRemoteStopFlag = new AtomicBoolean(false);
	private final String errorHandlerProducerKey = UUID.randomUUID().toString();
	@Nullable private SolaceMeterAccessor solaceMeterAccessor;
	private SolaceExtendedBindingProperties extendedBindingProperties = new SolaceExtendedBindingProperties();
	private static final SolaceMessageHeaderErrorMessageStrategy errorMessageStrategy = new SolaceMessageHeaderErrorMessageStrategy();
	@Nullable private SolaceBinderHealthAccessor solaceBinderHealthAccessor;

	public SolaceMessageChannelBinder(SolaceSessionManager solaceSessionManager, SolaceEndpointProvisioner solaceEndpointProvisioner) {
		super(new String[0], solaceEndpointProvisioner);
		this.solaceSessionManager = solaceSessionManager;
		this.sessionProducerManager = new JCSMPSessionProducerManager(solaceSessionManager);
	}

	@Override
	public String getBinderIdentity() {
		return "solace-" + super.getBinderIdentity();
	}

	@Override
	public void destroy() {
		sessionProducerManager.release(errorHandlerProducerKey);
		consumersRemoteStopFlag.set(true);
		solaceSessionManager.close();
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
														  ExtendedProducerProperties<SolaceProducerProperties> producerProperties,
														  MessageChannel errorChannel) throws Exception {
		JCSMPOutboundMessageHandler handler = new JCSMPOutboundMessageHandler(
				destination,
				solaceSessionManager.getSession(),
				errorChannel,
				sessionProducerManager,
				producerProperties,
				solaceMeterAccessor);

		if (errorChannel != null) {
			handler.setErrorMessageStrategy(errorMessageStrategy);
		}

		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
													 ExtendedConsumerProperties<SolaceConsumerProperties> properties) throws Exception {
		if (!properties.isBatchMode() && properties.getExtension().isTransacted()) {
			throw new IllegalArgumentException("Non-batched, transacted consumers are not supported");
		}

		if (properties.getExtension().isTransacted() && properties.getExtension().isAutoBindErrorQueue()) {
			throw new IllegalArgumentException("transacted consumers do not support error queues");
		}

		SolaceConsumerDestination solaceDestination = (SolaceConsumerDestination) destination;

		JCSMPInboundChannelAdapter adapter = new JCSMPInboundChannelAdapter(
				solaceDestination,
				solaceSessionManager.getSession(),
				properties,
				getConsumerEndpointProperties(properties),
				solaceMeterAccessor);

		if (solaceBinderHealthAccessor != null) {
			adapter.setSolaceBinderHealthAccessor(solaceBinderHealthAccessor);
		}

		adapter.setRemoteStopFlag(consumersRemoteStopFlag);
		adapter.setPostStart(getConsumerPostStart(solaceDestination, properties));

		if (properties.getExtension().isAutoBindErrorQueue()) {
			adapter.setErrorQueueInfrastructure(new ErrorQueueInfrastructure(
					sessionProducerManager,
					errorHandlerProducerKey,
					solaceDestination.getErrorQueueName(),
					properties.getExtension()));
		}

		ErrorInfrastructure errorInfra = registerErrorInfrastructure(destination, group, properties);
		if (properties.getMaxAttempts() > 1) {
			adapter.setRetryTemplate(buildSolaceRetryTemplate(properties));
			adapter.setRecoveryCallback(wrapRecoveryCallback(errorInfra.getRecoverer()));
		} else {
			adapter.setErrorChannel(errorInfra.getErrorChannel());
		}

		adapter.setErrorMessageStrategy(errorMessageStrategy);
		adapter.setBeanFactory(getBeanFactory());
		return adapter;
	}

	@Override
	protected PolledConsumerResources createPolledConsumerResources(String name, String group,
																	ConsumerDestination destination,
																	ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties) {
		if (!consumerProperties.isBatchMode() && consumerProperties.getExtension().isTransacted()) {
			throw new IllegalArgumentException("Non-batched, transacted consumers are not supported");
		}

		if (consumerProperties.getExtension().isTransacted() && consumerProperties.getExtension().isAutoBindErrorQueue()) {
			throw new IllegalArgumentException("transacted consumers do not support error queues");
		}

		if (consumerProperties.getConcurrency() > 1) {
			logger.warn("Polled consumers do not support concurrency > 1, it will be ignored...");
		}

		try {
			SolaceConsumerDestination solaceDestination = (SolaceConsumerDestination) destination;

			EndpointProperties endpointProperties = getConsumerEndpointProperties(consumerProperties);
			JCSMPMessageSource messageSource = new JCSMPMessageSource(solaceDestination,
					solaceSessionManager.getSession(),
					consumerProperties.isBatchMode() ? new BatchCollector(consumerProperties.getExtension())
							: null,
					consumerProperties,
					endpointProperties,
					solaceMeterAccessor);

			if (solaceBinderHealthAccessor != null) {
				messageSource.setSolaceBinderHealthAccessor(solaceBinderHealthAccessor);
			}

			messageSource.setRemoteStopFlag(consumersRemoteStopFlag::get);
			messageSource.setPostStart(getConsumerPostStart(solaceDestination, consumerProperties));

			if (consumerProperties.getExtension().isAutoBindErrorQueue()) {
				messageSource.setErrorQueueInfrastructure(
						new ErrorQueueInfrastructure(sessionProducerManager,
								errorHandlerProducerKey,
								solaceDestination.getErrorQueueName(),
								consumerProperties.getExtension()));
			}

			ErrorInfrastructure errorInfra = registerErrorInfrastructure(destination, group,
					consumerProperties, true);
			messageSource.setBeanFactory(getBeanFactory());
			return new PolledConsumerResources(messageSource, errorInfra);
		} catch (Exception e) {
			throw new RuntimeException("Failed to create polled consumer.", e);
		}
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

	public void setSolaceMeterAccessor(@Nullable SolaceMeterAccessor solaceMeterAccessor) {
		this.solaceMeterAccessor = solaceMeterAccessor;
	}

	public void setSolaceBinderHealthAccessor(@Nullable SolaceBinderHealthAccessor solaceBinderHealthAccessor) {
		this.solaceBinderHealthAccessor = solaceBinderHealthAccessor;
	}

	/**
	 * Build a RetryTemplate for message retry handling.
	 * This is a custom implementation to work around the type incompatibility between
	 * Spring Cloud Stream's RetryTemplate (org.springframework.core.retry.RetryTemplate)
	 * and Spring Retry's RetryTemplate (org.springframework.retry.support.RetryTemplate).
	 */
	private RetryTemplate buildSolaceRetryTemplate(ExtendedConsumerProperties<?> properties) {
		RetryTemplate retryTemplate = new RetryTemplate();

		// Configure retry policy
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(properties.getMaxAttempts());
		retryTemplate.setRetryPolicy(retryPolicy);

		// Configure backoff policy
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(properties.getBackOffInitialInterval());
		backOffPolicy.setMultiplier(properties.getBackOffMultiplier());
		backOffPolicy.setMaxInterval(properties.getBackOffMaxInterval());
		retryTemplate.setBackOffPolicy(backOffPolicy);

		return retryTemplate;
	}

	/**
	 * Wraps ErrorMessageSendingRecoverer into a RecoveryCallback to bridge the type incompatibility
	 * between Spring Integration and Spring Retry in Spring Boot 4.x.
	 */
	private RecoveryCallback<Object> wrapRecoveryCallback(org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer recoverer) {
		return context -> {
			Throwable lastThrowable = context.getLastThrowable();
			if (lastThrowable != null) {
				// RetryContext implements AttributeAccessor, so we can pass it directly
				recoverer.recover(context, lastThrowable);
			}
			return null;
		};
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
	private Consumer<Endpoint> getConsumerPostStart(SolaceConsumerDestination destination,
													ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		return (endpoint) -> {
			if (endpoint instanceof Queue queue) {
				provisioningProvider.addSubscriptionToQueue(queue, destination.getBindingDestinationName(), properties.getExtension(), true);

				//Process additional subscriptions
				for (String subscription : destination.getAdditionalSubscriptions()) {
					provisioningProvider.addSubscriptionToQueue(queue, subscription, properties.getExtension(), false);
				}
			}
		};
	}
}
