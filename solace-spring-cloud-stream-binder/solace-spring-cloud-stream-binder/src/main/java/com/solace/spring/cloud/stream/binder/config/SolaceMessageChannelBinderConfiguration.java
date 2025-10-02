package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.outbound.JCSMPOutboundMessageHandler;
import com.solace.spring.cloud.stream.binder.properties.SolaceBinderConfigurationProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceEndpointProvisioner;
import com.solace.spring.cloud.stream.binder.util.SessionInitializationMode;
import com.solace.spring.cloud.stream.binder.util.SolaceSessionManager;
import com.solacesystems.jcsmp.JCSMPException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.security.oauth2.client.servlet.OAuth2ClientAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;


@Configuration
@Import({SolaceSessionConfig.class, SolaceHealthIndicatorsConfiguration.class, OAuth2ClientAutoConfiguration.class})
@EnableConfigurationProperties({SolaceBinderConfigurationProperties.class, SolaceExtendedBindingProperties.class})
public class SolaceMessageChannelBinderConfiguration {
	private final SolaceExtendedBindingProperties solaceExtendedBindingProperties;
	private final SolaceBinderConfigurationProperties solaceBinderConfigurationProperties;
	private final SolaceSessionManager solaceSessionManager;

	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceMessageChannelBinderConfiguration.class);

	public SolaceMessageChannelBinderConfiguration(SolaceSessionManager solaceSessionManager,
			SolaceExtendedBindingProperties solaceExtendedBindingProperties,
			SolaceBinderConfigurationProperties solaceBinderConfigurationProperties) {
		this.solaceSessionManager = solaceSessionManager;
		this.solaceExtendedBindingProperties = solaceExtendedBindingProperties;
		this.solaceBinderConfigurationProperties = solaceBinderConfigurationProperties;
	}

	@PostConstruct
	public void init() throws JCSMPException {
		if (SessionInitializationMode.EAGER.equals(solaceBinderConfigurationProperties.getSessionInitializationMode())) {
			LOGGER.debug("Eagerly initializing Solace session.");
			solaceSessionManager.getSession();
		} else {
			LOGGER.debug("Deferring Solace session initialization.");
		}
	}

	@Bean
	SolaceMessageChannelBinder solaceMessageChannelBinder(
			SolaceSessionManager solaceSessionManager,
			SolaceEndpointProvisioner solaceEndpointProvisioner,
			@Nullable ProducerMessageHandlerCustomizer<JCSMPOutboundMessageHandler> producerCustomizer,
			@Nullable SolaceBinderHealthAccessor solaceBinderHealthAccessor,
			@Nullable SolaceMeterAccessor solaceMeterAccessor) {
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(solaceSessionManager, solaceEndpointProvisioner);
		binder.setExtendedBindingProperties(solaceExtendedBindingProperties);
		binder.setProducerMessageHandlerCustomizer(producerCustomizer);
		binder.setSolaceMeterAccessor(solaceMeterAccessor);
		binder.setSolaceBinderHealthAccessor(solaceBinderHealthAccessor);
		return binder;
	}

	@Bean
	SolaceEndpointProvisioner provisioningProvider(SolaceSessionManager solaceSessionManager) {
		return new SolaceEndpointProvisioner(solaceSessionManager);
	}

}
